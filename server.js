import express from "express";
import * as cheerio from "cheerio";
import puppeteer from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import axios from "axios";
import he from "he";
import fs from "node:fs/promises";
import path from "node:path";
import { isIP } from "node:net";
import { performance } from "node:perf_hooks";

const axiosMaxRedirects = Number.parseInt(process.env.SCRAPER_AXIOS_MAX_REDIRECTS || "", 10);
if (Number.isFinite(axiosMaxRedirects) && axiosMaxRedirects >= 0) {
  axios.defaults.maxRedirects = axiosMaxRedirects;
}

const app = express();
app.set("etag", false);

puppeteer.use(StealthPlugin());

const NAVIGATION_TIMEOUT = Math.max(
  5000,
  Number.parseInt(process.env.SCRAPER_NAVIGATION_TIMEOUT_MS || "40000", 10) || 40000
);
const STAGE1_HARD_TIMEOUT_MS = 40000;
const STAGE3_HARD_TIMEOUT_MS = 30000;

const HUMAN_DELAY_RANGE = [400, 800];

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
];
const VIEWPORT_WIDTHS = [1280, 1366, 1440, 1536, 1680, 1920];
const VIEWPORT_HEIGHTS = [720, 768, 900, 960, 1080];

// FIX: Réduit de 15 à 6 images max
const MAX_IMAGE_RESULTS = Math.max(
  1,
  Number.parseInt(process.env.SCRAPER_MAX_IMAGE_RESULTS || "6", 10) || 6
);
const BEST_IMAGE_LIMIT = Math.min(6, MAX_IMAGE_RESULTS);

const PRODUCT_IMAGE_KEYWORDS = [
  "product", "media", "gallery", "item", "detail", "zoom", "images", "photo", "pdp",
  "swatch", "packshot", "pim", "published",
];
const PLACEHOLDER_KEYWORDS = [
  "placeholder", "transparent", "pixel", "spacer", "blank", "loading", "spinner", "logo", "icon",
];

const CURRENCY_SYMBOLS = [
  "$", "€", "£", "¥", "₹", "₩", "₽", "₫", "₦", "₪", "฿", "₴", "₱", "₲", "₵", "₡", "R$",
];

const CURRENCY_CODES = [
  "USD", "EUR", "GBP", "JPY", "CNY", "CAD", "AUD", "NZD", "CHF", "HKD", "SEK", "NOK", "DKK",
  "PLN", "RON", "HUF", "CZK", "MXN", "ARS", "BRL", "TRY", "ZAR", "AED", "SAR", "INR", "KRW",
  "RUB", "SGD", "TWD", "MYR", "THB", "PHP", "IDR",
];

const CURRENCY_SYMBOL_TO_CODE = new Map([
  ["$", "USD"], ["US$", "USD"], ["USD$", "USD"], ["€", "EUR"], ["£", "GBP"], ["¥", "JPY"],
  ["C$", "CAD"], ["CA$", "CAD"], ["A$", "AUD"], ["AU$", "AUD"], ["NZ$", "NZD"],
  ["HK$", "HKD"], ["₩", "KRW"], ["₽", "RUB"], ["₹", "INR"], ["₺", "TRY"], ["R$", "BRL"],
]);

const CURRENCY_SYMBOL_PATTERN = CURRENCY_SYMBOLS.map((symbol) =>
  symbol.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
).join("|");
const CURRENCY_CODE_PATTERN = CURRENCY_CODES.join("|");
const PRICE_REGEXES = [
  new RegExp(`(?:${CURRENCY_SYMBOL_PATTERN})\\s*\\d[\\d.,]*`, "i"),
  new RegExp(`\\d[\\d.,]*\\s*(?:${CURRENCY_SYMBOL_PATTERN})`, "i"),
  new RegExp(`(?:${CURRENCY_CODE_PATTERN})\\s*\\d[\\d.,]*`, "i"),
  new RegExp(`\\d[\\d.,]*\\s*(?:${CURRENCY_CODE_PATTERN})`, "i"),
];

const PRICE_CONTEXT_NEGATIVE_KEYWORDS = [
  "assurance", "garantie", "warranty", "insurance", "protection", "coverage",
  "subscription", "abonnement", "support", "service", "plan",
];

const DEFAULT_USD_TO_EUR_RATE = 0.92;

// ─── IMAGE PRIORITY SOURCES ───────────────────────────────────────────────────
const SOURCE_PRIORITY = {
  jsonld_product: 8000,   // FIX: JSON-LD produit devient la source la plus fiable
  og_image: 5000,         // FIX: og:image réduit car souvent image marketing/bundle
  twitter_image: 4000,
  itemprop_image: 4000,
  dom_strong: 2000,
  dom_weak: 1000,
  fallback: 0,
};

const STRONG_PRODUCT_URL_PATTERNS = [
  /\/pim\//i,
  /\/published\//i,
  /\/pdp\//i,
  /\/packshot/i,
  /\/product[-_]?image/i,
  /[-_]main[-_.]/i,
  /[-_]primary[-_.]/i,
  /[-_]front[-_.]/i,
  /[-_]default[-_.]/i,
  /\/gallery\//i,
  /\/zoom\//i,
  /media[-_]\d+[-_]\d+\./i,
  /\/ecomm\//i,
  /[-_]ecomm[-_]/i,
];

const MARKETING_URL_PATTERNS = [
  // ── UI / assets non-produit ──
  /\/banner/i,
  /\/marketing/i,
  /\/campaign/i,
  /\/editorial/i,
  /\/homepage/i,
  /\/landing/i,
  /\/lookbook/i,
  /\/promo/i,
  /\/newsletter/i,
  /\/carousel/i,
  /\/slider/i,
  /library-sites/i,
  /\/logo/i,
  /\/icon/i,
  /\/sprite/i,
  /\/avatar/i,
  /media_principal/i,
  /media_thumbnail/i,
  /[-_]thumbnail[-_]?/i,
  /\/tile_/i,
  /[-_]tile[-_]/i,
  /shade_finder/i,

  // ── Images éditoriales / lookbook ──
  // look: pénalité légère uniquement, pas de blocage dur
  // car "look" peut être dans le nom du kit produit
  /lifestyle/i,
  // bundle: pas bloqué ici car un kit/bundle EST parfois le produit principal
  // géré par pénalité légère dans computeImagePriorityScore

  // ── Photos avec modèle (pas le produit seul) ──
  /[-_]model[-_]/i,
  /model_shoot/i,
  /[-_]hm[-_]/i,
  /[-_]rq[-_]/i,
  /[-_]rm[-_]/i,
  /[-_]hf[-_]/i,

  // ── Codes géographiques = variantes éditoriales régionales ──
  /middle.?east/i,
  /north.?africa/i,
  /[-_]apac[-_]/i,
  /[-_]emea[-_]/i,
  /[-_]latam[-_]/i,
  /[-_]mena[-_]/i,

  // ── Contenu tutorial / avant-après ──
  /before.?after/i,
  /after.?before/i,
  /how[-_]?to/i,
  /tutorial/i,
  /routine/i,
  /grwm/i,

  // ── Gros plans non-produit ──
  /texture/i,
  /close.?up/i,
  /[-_]on[-_]skin/i,
  /application/i,
  /swatch/i,

  // ── Codes campagne saisonnière ──
  /[-_]fall\d{2}(?!.*(?:product|ecomm|item))/i,
  /[-_]spring\d{2}(?!.*(?:product|ecomm|item))/i,
  /[-_]summer\d{2}(?!.*(?:product|ecomm|item))/i,
  /[-_]winter\d{2}(?!.*(?:product|ecomm|item))/i,
  /[-_]aw\d{2}[-_]/i,
  /[-_]ss\d{2}[-_]/i,
  /[-_]holiday[-_]/i,
  /[-_]xmas[-_]/i,
  /[-_]festive[-_]/i,

  // ── Dates dans le nom de fichier = photos de campagne datées ──
  /[-_]20\d{2}(?:0[1-9]|1[0-2])[-_]/i,
];

// Minimum size threshold
const MIN_IMAGE_DIMENSION = 300;

// Patterns qui indiquent une image produit pure (fond blanc/neutre, produit seul)
const PURE_PRODUCT_URL_PATTERNS = [
  /\/products?\//i,
  /\/packshot/i,
  /\/pim\//i,
  /fit=fill/i,
  /[-_]main[-_.]/i,
  /[-_]primary[-_.]/i,
  /[-_]front[-_.]/i,
  /[-_]default[-_.]/i,
  /[-_]\d{1,5}ml[-_.]/i,
  /[-_]\d{1,4}g[-_.]/i,
  /[-_]\d{1,4}oz[-_.]/i,
  /ecomm.*product|product.*ecomm/i,
];

function parseNumericPrice(value) {
  if (!value) return null;
  const digits = `${value}`.match(/[\d]/g);
  if (!digits || !digits.length) return null;
  let normalized = `${value}`.replace(/[^\d.,]/g, "");
  if (!normalized) return null;
  const lastComma = normalized.lastIndexOf(",");
  const lastDot = normalized.lastIndexOf(".");
  if (lastComma > lastDot) {
    normalized = normalized.replace(/\./g, "").replace(/,/g, ".");
  } else if (lastDot > lastComma) {
    normalized = normalized.replace(/,/g, "");
  } else {
    normalized = normalized.replace(/[.,]/g, "");
  }
  const parsed = Number.parseFloat(normalized);
  if (!Number.isFinite(parsed)) return null;
  return parsed;
}

function scorePriceCandidate(rawValue, { order = 0, currencyHints = [], contextPenalty = 0 } = {}) {
  const value = `${rawValue || ""}`.trim();
  if (!value) return null;
  const upperValue = value.toUpperCase();
  const numericValue = parseNumericPrice(value);
  const digitsCount = (value.match(/\d/g) || []).length;
  const decimalMatch = value.match(/[.,](\d{1,})/);
  const decimalDigits = decimalMatch ? decimalMatch[1].length : 0;
  const hasCurrencySymbol = CURRENCY_SYMBOLS.some((symbol) => value.includes(symbol));
  const hasCurrencyCode = CURRENCY_CODES.some((code) => upperValue.includes(code));
  const matchesHint = currencyHints.some((hint) => upperValue.includes(hint.toUpperCase()));

  let score = 0;
  if (hasCurrencySymbol) score += 6;
  if (hasCurrencyCode) score += 5;
  if (matchesHint) score += 4;
  if (decimalDigits === 2) score += 2;
  else if (decimalDigits === 1) score += 1;
  if (digitsCount >= 4) score += 3;
  else if (digitsCount >= 3) score += 2;
  if (numericValue !== null) {
    if (numericValue >= 1000) score += 3;
    else if (numericValue >= 100) score += 2;
    else if (numericValue >= 20) score += 1;
    else if (numericValue < 5) score -= 2;
    else if (numericValue < 10) score -= 1;
  }
  if (Number.isFinite(contextPenalty) && contextPenalty !== 0) score += contextPenalty;
  const tieBreaker = order * 0.01;
  return { value, score: score - tieBreaker, order };
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms || 0)));
}

function randomItem(list) {
  return list[Math.floor(Math.random() * list.length)];
}

function randomBetween(min, max) {
  return min + Math.random() * (max - min);
}

const { TimeoutError } = puppeteer.errors ?? {};

const BROWSER_LAUNCH_ARGS = [
  "--no-sandbox",
  "--disable-setuid-sandbox",
  "--disable-dev-shm-usage",
  "--disable-gpu",
  "--no-zygote",
  "--ignore-certificate-errors",
  "--ignore-certificate-errors-spki-list",
  "--disable-features=site-per-process",
  "--disable-software-rasterizer",
];

let sharedBrowserPromise = null;

function createBrowserLaunchOptions(userAgent) {
  const args = [...BROWSER_LAUNCH_ARGS];
  if (userAgent) args.push(`--user-agent=${userAgent}`);
  return { headless: "new", args };
}

function shouldDecodeLikelyBase64(value) {
  if (typeof value !== "string") return false;
  const normalized = value.replace(/\s+/g, "");
  if (normalized.length < 40 || normalized.length % 4 !== 0) return false;
  if (!/^[A-Za-z0-9+/=]+$/.test(normalized)) return false;
  return true;
}

function decodeLikelyBase64(value) {
  if (!shouldDecodeLikelyBase64(value)) return null;
  try {
    const decoded = Buffer.from(value.replace(/\s+/g, ""), "base64").toString("utf8");
    if (decoded && /<html|<!doctype html|<body|<head/i.test(decoded)) return decoded;
  } catch {
    return null;
  }
  return null;
}

function pickBrightDataHtmlCandidate(payload) {
  const paths = [
    ["solution", "response", "body"],
    ["solution", "content"],
    ["response", "body"],
    ["body"],
    ["result"],
    ["content"],
    ["html"],
  ];
  for (const pathParts of paths) {
    let current = payload;
    for (const key of pathParts) {
      if (!current || typeof current !== "object") { current = null; break; }
      current = current[key];
    }
    if (typeof current === "string" && current.trim()) return current;
    if (Buffer.isBuffer(current)) {
      const asText = current.toString("utf8");
      if (asText.trim()) return asText;
    }
  }
  return null;
}

async function acquireSharedBrowser() {
  if (sharedBrowserPromise) {
    try {
      const existing = await sharedBrowserPromise;
      const hasProcess = Boolean(existing?.process?.()?.pid);
      if (existing?.isConnected?.() && hasProcess) return existing;
      await existing?.close?.().catch(() => {});
    } catch {
      sharedBrowserPromise = null;
    }
  }
  sharedBrowserPromise = puppeteer
    .launch(createBrowserLaunchOptions())
    .then((browser) => {
      browser?.once?.("disconnected", () => { sharedBrowserPromise = null; });
      return browser;
    })
    .catch((err) => { sharedBrowserPromise = null; throw err; });
  return sharedBrowserPromise;
}

async function enableRequestOptimizations(page) {
  try {
    await page.setRequestInterception(true);
  } catch {
    return () => {};
  }
  const handler = (request) => {
    try {
      const type = request.resourceType();
      if (["stylesheet", "font", "media", "image"].includes(type)) {
        request.abort();
        return;
      }
    } catch {
      // ignore
    }
    request.continue().catch(() => {});
  };
  page.on("request", handler);
  return () => {
    page.off("request", handler);
    page.setRequestInterception(false).catch(() => {});
  };
}

async function configurePage(page, url, preferredUserAgent) {
  const userAgent = preferredUserAgent || pickUserAgent();
  const viewport = pickViewport();
  await page.setUserAgent(userAgent);
  await page.setViewport(viewport);
  await page.setJavaScriptEnabled(true);
  if (typeof page.setDefaultNavigationTimeout === "function") {
    page.setDefaultNavigationTimeout(NAVIGATION_TIMEOUT);
  }
  if (typeof page.setDefaultTimeout === "function") {
    page.setDefaultTimeout(Math.max(NAVIGATION_TIMEOUT, 30000));
  }
  await enableRequestOptimizations(page);
  return { userAgent, viewport };
}

async function navigatePage(page, url) {
  const strategies = [
    { waitUntil: "domcontentloaded", label: "domcontentloaded" },
    { waitUntil: "load", label: "load" },
  ];
  let lastError = null;
  for (const strategy of strategies) {
    const attemptStart = performance.now();
    try {
      await page.goto(url, { waitUntil: strategy.waitUntil, timeout: NAVIGATION_TIMEOUT });
      await page.waitForSelector("body", { timeout: Math.min(10000, NAVIGATION_TIMEOUT) }).catch(() => {});
      const durationSeconds = roundDuration((performance.now() - attemptStart) / 1000);
      return { waitUntil: strategy.label, durationSeconds, navigationTimedOut: false };
    } catch (err) {
      const durationSeconds = roundDuration((performance.now() - attemptStart) / 1000);
      lastError = err;
      const isTimeout = TimeoutError && err instanceof TimeoutError;
      err.navigationWaitUntil = strategy.label;
      err.navigationDurationSeconds = durationSeconds;
      err.navigationTimedOut = isTimeout;
      if (!isTimeout) throw err;
    }
  }
  if (lastError) throw lastError;
  throw new Error("Navigation failed");
}

function pickUserAgent() {
  return randomItem(USER_AGENTS);
}

function pickViewport() {
  const width = randomItem(VIEWPORT_WIDTHS) + Math.floor(Math.random() * 40);
  const height = randomItem(VIEWPORT_HEIGHTS) + Math.floor(Math.random() * 60);
  const deviceScaleFactor = Math.random() < 0.2 ? 2 : 1;
  return { width, height, deviceScaleFactor };
}

function normalizeUrl(value, baseUrl) {
  if (!value) return null;
  const trimmed = `${value}`.trim();
  if (!trimmed) return null;
  if (trimmed.startsWith("//")) return `https:${trimmed}`;
  if (/^https?:/i.test(trimmed)) return trimmed;
  if (trimmed.startsWith("data:")) return null;
  try {
    return new URL(trimmed, baseUrl).toString();
  } catch {
    return null;
  }
}

function isValidImageUrl(url) {
  if (!url) return false;
  const lower = url.toLowerCase();

  if (/\.gif($|\?|&)/i.test(lower)) return false;
  if (/\.svg($|\?|&)/i.test(lower)) return false;
  if (/library-sites/i.test(lower)) return false;
  if (/fid\.(gif|png|jpg|webp)|loyalty|mysephora.*fid|blackfid|goldfid|bronzefid/i.test(lower)) return false;
  if (/img\.youtube\.com|i\.ytimg\.com|vumbnail\.com|vimeo\.com\/video/i.test(lower)) return false;
  if (/media_thumbnail|[-_]thumbnail[-_\d]/i.test(lower)) return false;

  if (!/\.(jpe?g|png|webp|avif)(?:$|\?|&)/i.test(lower) && !/\/(image|photo|picture|img)\//i.test(lower)) {
    if (!/image|photo|picture|img|media|gallery/i.test(lower)) return false;
  }
  if (PLACEHOLDER_KEYWORDS.some((kw) => lower.includes(kw))) return false;
  return true;
}

// FIX: Calcul du score d'image amélioré
function computeImagePriorityScore(url, sourcePriority = 0) {
  if (!url) return -Infinity;

  let score = sourcePriority;

  // Pénalité lourde pour images marketing/non-produit
  for (const pattern of MARKETING_URL_PATTERNS) {
    if (pattern.test(url)) {
      score -= 5000;
      break;
    }
  }

  // Bonus fort pour patterns d'images produit pures
  for (const pattern of PURE_PRODUCT_URL_PATTERNS) {
    if (pattern.test(url)) {
      score += 4000;
      break;
    }
  }

  // Bonus pour patterns URL produit forts
  for (const pattern of STRONG_PRODUCT_URL_PATTERNS) {
    if (pattern.test(url)) {
      score += 3000;
      break;
    }
  }

  // Scoring par dimensions
  const dims = extractDimensionsFromUrl(url);
  if (dims.width && dims.height) {
    const area = dims.width * dims.height;
    score += Math.min(area / 100, 2000);
    const ratio = dims.width / dims.height;
    // FIX: Pénaliser les images très larges (bannières) mais pas les images carrées (produit)
    if (ratio > 2.5) score -= 4000;
    else if (ratio > 1.8) score -= 2000;
    else if (ratio > 1.5) score -= 500;
    // Bonus pour images carrées ou quasi-carrées (typique produit cosmétique)
    else if (ratio >= 0.8 && ratio <= 1.2) score += 1000;
    if (dims.width < MIN_IMAGE_DIMENSION || dims.height < MIN_IMAGE_DIMENSION) return -Infinity;
  } else if (dims.width) {
    score += Math.min(dims.width * 2, 1000);
    if (dims.width < MIN_IMAGE_DIMENSION) return -Infinity;
  }

  // Format bonuses
  if (/\.webp($|\?)/i.test(url)) score += 200;
  if (/\.jpe?g($|\?)/i.test(url)) score += 150;
  if (/\.png($|\?)/i.test(url)) score += 100;
  if (/\.svg($|\?)/i.test(url)) score -= 500;

  // Quality hints in URL
  if (/_large|_xl|_2x|@2x|1200|1600|_zoom/i.test(url)) score += 300;

  // Pénalité légère pour bundle/look (pas bloquant car peut être le produit lui-même)
  // mais on préfère un packshot pur si disponible
  if (/[-_]bundle[-_]|_bundle|bundle_/i.test(url)) score -= 800;
  if (/look[-_]|[-_]look[-_]|[-_]look\./i.test(url)) score -= 600;

  // Pénalité légère pour images avec code modèle CT (_hm_, _rq_ etc.)
  // déjà dans MARKETING mais double-vérification par score pour les cas limites
  if (/[-_](?:hm|rq|rm|hf|em|cm)[-_]/i.test(url)) score -= 3000;

  // Bonus fort pour images packshot identifiées par alt text (via filename)
  if (/packshot|pack[-_]shot/i.test(url)) score += 2000;

  return score;
}

function parseDimension(value) {
  if (value === undefined || value === null) return null;
  const stringValue = `${value}`.replace(/,/g, ".");
  const match = stringValue.match(/\d+(?:\.\d+)?/);
  if (!match) return null;
  const number = Number.parseFloat(match[0]);
  if (!Number.isFinite(number) || number <= 0) return null;
  return Math.round(number);
}

function extractDimensionsFromUrl(url) {
  let width = null;
  let height = null;
  try {
    const parsed = new URL(url);
    const widthKeys = ["w", "width", "wid", "rw", "mw", "scalewidth"];
    const heightKeys = ["h", "height", "hei", "rh", "mh", "scaleheight"];
    for (const key of widthKeys) {
      const candidate = parseDimension(parsed.searchParams.get(key));
      if (candidate) { width = candidate; break; }
    }
    for (const key of heightKeys) {
      const candidate = parseDimension(parsed.searchParams.get(key));
      if (candidate) { height = candidate; break; }
    }
  } catch {
    // ignore
  }
  if (!width || !height) {
    const match = url.match(/([0-9]{2,4})x([0-9]{2,4})/);
    if (match) {
      if (!width) width = parseDimension(match[1]);
      if (!height) height = parseDimension(match[2]);
    }
  }
  return { width, height };
}

function extractSrcsetCandidates(value) {
  if (!value) return [];
  return value.split(",").map((part) => part.trim()).filter(Boolean).map((part) => {
    const [urlPart, descriptor] = part.split(/\s+/, 2);
    const result = { url: urlPart, width: null, density: null };
    if (descriptor) {
      if (descriptor.endsWith("w")) {
        const widthValue = parseDimension(descriptor.slice(0, -1));
        if (widthValue) result.width = widthValue;
      } else if (descriptor.endsWith("x")) {
        const densityValue = Number.parseFloat(descriptor.slice(0, -1));
        if (Number.isFinite(densityValue) && densityValue > 0) result.density = densityValue;
      }
    }
    return result;
  }).filter((candidate) => candidate.url);
}

function createImageDedupKey(url) {
  if (!url) return "";
  try {
    const parsed = new URL(url);
    const skipKeys = new Set([
      "w", "width", "wid", "rw", "mw",
      "h", "height", "hei", "rh", "mh",
      "sw", "sh", "sm", "frz-v", "frzv",
      "scalewidth", "scaleheight", "scalemode",
      "imwidth", "imheight", "size", "scale", "scaling",
      "fit", "crop", "dpr", "ar", "trim",
      "quality", "q", "compression",
      "format", "auto", "ext", "fm",
      "ts", "timestamp", "cache", "v", "ver", "version", "cb", "t",
      "c", "f", "g",
      "bgcolor", "bg", "pad",
    ]);

    const normalizedSearch = new URLSearchParams();
    const sortedKeys = Array.from(parsed.searchParams.keys()).sort();
    for (const key of sortedKeys) {
      if (skipKeys.has(key.toLowerCase())) continue;
      const values = parsed.searchParams.getAll(key);
      for (const value of values) normalizedSearch.append(key.toLowerCase(), value);
    }
    const normalizedQuery = normalizedSearch.toString();
    const fullKey = `${parsed.hostname}${parsed.pathname}${normalizedQuery ? `?${normalizedQuery}` : ""}`.toLowerCase();
    const filename = parsed.pathname.split("/").pop() || "";
    const isSignificantFilename = filename.length > 15 && /\.(jpe?g|png|webp|avif)/i.test(filename);
    if (isSignificantFilename) {
      // FIX: strip dimensions from filename before dedup
      const filenameNoDims = filename.replace(/_\d{2,4}x\d{2,4}/gi, "").replace(/\d{2,4}x\d{2,4}_/gi, "").replace(/-\d{2,4}x\d{2,4}/gi, "").replace(/\d{2,4}x\d{2,4}-/gi, "");
      return `${parsed.hostname}__file__${filenameNoDims}${normalizedQuery ? `?${normalizedQuery}` : ""}`.toLowerCase();
    }
    return fullKey;
  } catch {
    return `${url}`.trim().toLowerCase();
  }
}

function dedupeImagesByScore(entries) {
  const bestByKey = new Map();
  for (const entry of entries) {
    if (!entry.url) continue;
    const key = createImageDedupKey(entry.url);
    const existing = bestByKey.get(key);
    if (!existing || entry.score > existing.score) {
      bestByKey.set(key, entry);
    }
  }
  return Array.from(bestByKey.values())
    .sort((a, b) => b.score - a.score)
    .map((e) => e.url);
}

function computePriceContextPenalty(text) {
  if (!text) return 0;
  const lower = `${text}`.toLowerCase();
  return PRICE_CONTEXT_NEGATIVE_KEYWORDS.some((keyword) => lower.includes(keyword)) ? -8 : 0;
}

function findPriceInTexts(texts, currencyHints = []) {
  if (!Array.isArray(texts) || !texts.length) return null;
  const normalizedCurrencyHints = Array.from(new Set(
    currencyHints.map((hint) => `${hint || ""}`.trim()).filter(Boolean)
  ));
  const candidates = [];
  texts.forEach((text, index) => {
    const normalized = `${text || ""}`.replace(/\s+/g, " ").trim();
    if (!normalized) return;
    const contextPenalty = computePriceContextPenalty(normalized);
    for (const regex of PRICE_REGEXES) {
      const match = normalized.match(regex);
      if (match && match[0]) {
        const candidate = scorePriceCandidate(match[0], { order: index, currencyHints: normalizedCurrencyHints, contextPenalty });
        if (candidate) { candidates.push(candidate); break; }
      }
    }
  });
  if (!candidates.length && normalizedCurrencyHints.length) {
    const numberRegex = /\d[\d.,]*/;
    texts.forEach((text, index) => {
      const normalized = `${text || ""}`.replace(/\s+/g, " ").trim();
      if (!normalized) return;
      const contextPenalty = computePriceContextPenalty(normalized);
      const numberMatch = normalized.match(numberRegex);
      if (!numberMatch) return;
      const numberValue = numberMatch[0];
      for (const hint of normalizedCurrencyHints) {
        const combined = combinePriceWithCurrency(numberValue, hint);
        const candidate = scorePriceCandidate(combined, { order: texts.length + index, currencyHints: normalizedCurrencyHints, contextPenalty });
        if (candidate) candidates.push(candidate);
      }
    });
  }
  if (!candidates.length) return null;
  const deduped = new Map();
  for (const candidate of candidates) {
    const key = candidate.value;
    const existing = deduped.get(key);
    if (!existing || existing.score < candidate.score) deduped.set(key, candidate);
  }
  const best = Array.from(deduped.values()).sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    return a.order - b.order;
  })[0];
  return best ? best.value : null;
}

function combinePriceWithCurrency(priceValue, currencyValue) {
  const priceText = `${priceValue || ""}`.replace(/\s+/g, " ").trim();
  if (!priceText) return null;
  const currencyText = `${currencyValue || ""}`.replace(/\s+/g, " ").trim();
  if (!currencyText) return priceText;
  const upperPrice = priceText.toUpperCase();
  const upperCurrency = currencyText.toUpperCase();
  if (upperPrice.includes(upperCurrency)) return priceText;
  if (CURRENCY_SYMBOLS.some((symbol) => priceText.includes(symbol))) return priceText;
  if (/^[A-Z]{2,3}$/.test(upperCurrency)) return `${priceText} ${upperCurrency}`.trim();
  return `${currencyText} ${priceText}`.trim();
}

function getUsdToEurRate() {
  const envValue = Number.parseFloat(process.env.SCRAPER_USD_TO_EUR_RATE || "");
  if (Number.isFinite(envValue) && envValue > 0) return envValue;
  return DEFAULT_USD_TO_EUR_RATE;
}

function detectCurrencyFromText(value, currencyHints = []) {
  const text = `${value || ""}`;
  const normalizedHints = (currencyHints || []).map((hint) => `${hint || ""}`.trim()).filter(Boolean);
  for (const hint of normalizedHints) {
    const upper = hint.toUpperCase();
    if (CURRENCY_CODES.includes(upper)) return upper;
    const direct = CURRENCY_SYMBOL_TO_CODE.get(hint) || CURRENCY_SYMBOL_TO_CODE.get(upper);
    if (direct) return direct;
  }
  const upperText = text.toUpperCase();
  for (const code of CURRENCY_CODES) { if (upperText.includes(code)) return code; }
  for (const [symbol, code] of CURRENCY_SYMBOL_TO_CODE.entries()) {
    if (!symbol || !text.includes(symbol)) continue;
    if (symbol === "$") {
      if (/US\$|USD/.test(upperText)) return "USD";
      const hintedCode = normalizedHints.map((hint) => `${hint}`.toUpperCase()).find((hint) => CURRENCY_CODES.includes(hint));
      if (hintedCode) return hintedCode;
      return "USD";
    }
    return code;
  }
  return null;
}

function formatPriceNumber(value) {
  if (!Number.isFinite(value)) return null;
  const rounded = Math.round(value * 100) / 100;
  const fixed = rounded.toFixed(2);
  return fixed.replace(/\.00$/, "").replace(/(\.\d)0$/, "$1");
}

function normalizePriceOutput(value, currencyHints = []) {
  if (!value) return null;
  let amount = parseNumericPrice(value);
  if (amount === null) {
    const digitsOnly = `${value}`.replace(/[^0-9.,-]/g, "");
    amount = parseNumericPrice(digitsOnly);
    if (amount === null) {
      const sanitized = digitsOnly.replace(/,/g, "");
      return sanitized || null;
    }
  }
  const detectedCurrency = detectCurrencyFromText(value, currencyHints);
  if (detectedCurrency === "USD") amount *= getUsdToEurRate();
  return formatPriceNumber(amount);
}

// ─── MAIN EXTRACTION FUNCTION ─────────────────────────────────────────────────
function extractFromHtmlContent(html, url) {
  if (!html) return { title: null, description: null, price: null, images: [] };
  const $ = cheerio.load(html);

  // ── Title ──
  const metaTitle =
    $("meta[property='og:title']").attr("content") ||
    $("meta[name='twitter:title']").attr("content") ||
    $("meta[name='title']").attr("content") || null;
  const domTitle = $("h1").first().text().trim() || $("title").first().text().trim() || null;
  const title = metaTitle || domTitle || null;

  // ── Description ──
  const description =
    $("meta[property='og:description']").attr("content") ||
    $("meta[name='description']").attr("content") ||
    $("meta[name='twitter:description']").attr("content") ||
    $("p").toArray().map((el) => $(el).text().trim()).find((text) => text.length > 60) || null;

  // ── Price ──
  const priceValues = [];
  const currencyValues = new Set();

  function pushPriceValue(value) {
    if (!value) return;
    const normalized = `${value}`.replace(/\s+/g, " ").trim();
    if (normalized) priceValues.push(normalized);
  }
  function pushCurrencyValue(value) {
    if (!value) return;
    const normalized = `${value}`.replace(/\s+/g, " ").trim();
    if (normalized) currencyValues.add(normalized);
  }

  const priceMetaSelectors = [
    "meta[property='product:price:amount']", "meta[name='product:price:amount']",
    "meta[itemprop='price']", "meta[property='og:price:amount']", "meta[name='og:price:amount']",
  ];
  for (const selector of priceMetaSelectors) {
    $(selector).toArray().forEach((element) => pushPriceValue($(element).attr("content")));
  }

  const currencyMetaSelectors = [
    "meta[itemprop='priceCurrency']", "meta[property='product:price:currency']",
    "meta[name='product:price:currency']", "meta[property='og:price:currency']", "meta[name='og:price:currency']",
  ];
  for (const selector of currencyMetaSelectors) {
    $(selector).toArray().forEach((element) => pushCurrencyValue($(element).attr("content")));
  }

  const priceElementSelectors = [
    "[class*='price']", "[id*='price']", "span[itemprop='price']",
    "meta[itemprop='price']", "[data-price]", "[data-price-amount]",
  ];
  $(priceElementSelectors.join(",")).toArray().forEach((element) => {
    const el = $(element);
    const content = el.attr("content") || el.attr("data-price") || el.attr("data-price-amount") || el.text();
    pushPriceValue(content);
    const currency = el.attr("data-currency") || el.attr("data-price-currency") || el.attr("data-currency-code") || null;
    pushCurrencyValue(currency);
  });

  // ── Images ──────────────────────────────────────────────────────────────────
  const imageCandidates = [];

  function addCandidate(rawUrl, sourcePriority = SOURCE_PRIORITY.fallback) {
    if (!rawUrl) return;
    const normalized = normalizeUrl(rawUrl, url);
    if (!normalized) return;
    if (!isValidImageUrl(normalized)) return;
    const score = computeImagePriorityScore(normalized, sourcePriority);
    imageCandidates.push({ url: normalized, score });
  }

  // PRIORITY 1: JSON-LD structured data (FIX: maintenant la source la plus fiable)
  $("script[type='application/ld+json']").toArray().forEach((element) => {
    const text = $(element).text();
    if (!text) return;
    try {
      const json = JSON.parse(text);
      const nodes = Array.isArray(json) ? json : [json];
      const toArray = (value) => (Array.isArray(value) ? value : value ? [value] : []);
      const hasType = (node, typeName) => {
        const types = toArray(node?.["@type"]).map((item) => `${item}`.toLowerCase());
        return types.includes(`${typeName}`.toLowerCase());
      };

      const processImageValue = (imageValue, priority) => {
        if (!imageValue) return;
        if (typeof imageValue === "string") {
          addCandidate(imageValue, priority);
          return;
        }
        if (typeof imageValue === "object") {
          const imageUrl = imageValue.url || imageValue.contentUrl || imageValue.image || imageValue.thumbnailUrl || imageValue['@id'];
          if (imageUrl) addCandidate(imageUrl, priority);
        }
      };

      nodes.forEach((node) => {
        if (!node || typeof node !== "object") return;

        if (node.price) pushPriceValue(node.price);
        if (node.priceCurrency) pushCurrencyValue(node.priceCurrency);

        toArray(node["@graph"]).forEach((graphNode) => {
          if (!graphNode || typeof graphNode !== "object") return;
          if (hasType(graphNode, "Product")) {
            toArray(graphNode.image).forEach((img) => processImageValue(img, SOURCE_PRIORITY.jsonld_product));
          }
        });

        if (hasType(node, "Product")) {
          toArray(node.image).forEach((img) => processImageValue(img, SOURCE_PRIORITY.jsonld_product));
        } else {
          const imageField = node.image || node.images || node.photo || node.thumbnailUrl;
          toArray(imageField).forEach((img) => processImageValue(img, SOURCE_PRIORITY.itemprop_image));
        }

        [...toArray(node.offers), ...toArray(node.aggregateOffer)].forEach((offer) => {
          if (!offer || typeof offer !== "object") return;
          if (offer.price) pushPriceValue(offer.price);
          if (offer.priceCurrency) pushCurrencyValue(offer.priceCurrency);
          toArray(offer.priceSpecification).forEach((spec) => {
            if (!spec || typeof spec !== "object") return;
            if (spec.price) pushPriceValue(spec.price);
            if (spec.priceCurrency) pushCurrencyValue(spec.priceCurrency);
          });
        });
      });
    } catch {
      // ignore invalid JSON-LD
    }
  });

  // PRIORITY 2: og:image (FIX: priorité réduite, soumis au scoring marketing)
  const ogImage = $("meta[property='og:image']").attr("content") ||
    $("meta[property='og:image:url']").attr("content");
  if (ogImage) addCandidate(ogImage, SOURCE_PRIORITY.og_image);

  // PRIORITY 3: twitter:image
  const twitterImage = $("meta[name='twitter:image']").attr("content") ||
    $("meta[name='twitter:image:src']").attr("content");
  if (twitterImage) addCandidate(twitterImage, SOURCE_PRIORITY.twitter_image);

  // PRIORITY 4: link[rel='image_src']
  const linkImage = $("link[rel='image_src']").attr("href");
  if (linkImage) addCandidate(linkImage, SOURCE_PRIORITY.twitter_image);

  // PRIORITY 5: itemprop="image"
  $("[itemprop='image']").toArray().forEach((element) => {
    const src = $(element).attr("content") || $(element).attr("src");
    if (src) addCandidate(src, SOURCE_PRIORITY.itemprop_image);
  });

  // PRIORITY 6: DOM images
  $("img").toArray().forEach((element) => {
    const el = $(element);
    const src = el.attr("src") || el.attr("data-src") || el.attr("data-lazy-src") || el.attr("data-original");
    if (src) {
      const normalized = normalizeUrl(src, url);
      if (normalized && isValidImageUrl(normalized)) {
        const isStrong = STRONG_PRODUCT_URL_PATTERNS.some((p) => p.test(normalized));
        const priority = isStrong ? SOURCE_PRIORITY.dom_strong : SOURCE_PRIORITY.dom_weak;
        addCandidate(src, priority);
      }
    }

    const srcsetValues = [el.attr("srcset"), el.attr("data-srcset"), el.attr("data-sources")].filter(Boolean);
    for (const srcset of srcsetValues) {
      extractSrcsetCandidates(srcset).forEach((candidate) => {
        const isStrong = STRONG_PRODUCT_URL_PATTERNS.some((p) => p.test(candidate.url || ""));
        addCandidate(candidate.url, isStrong ? SOURCE_PRIORITY.dom_strong : SOURCE_PRIORITY.dom_weak);
      });
    }
  });

  // source elements (picture)
  $("source").toArray().forEach((element) => {
    const el = $(element);
    const srcsetValues = [el.attr("srcset"), el.attr("data-srcset"), el.attr("data-src")].filter(Boolean);
    for (const srcset of srcsetValues) {
      extractSrcsetCandidates(srcset).forEach((candidate) => {
        addCandidate(candidate.url, SOURCE_PRIORITY.dom_weak);
      });
    }
  });

  // PRIORITY 7: Scripts JSON embarqués
  $("script").toArray().forEach((element) => {
    const scriptContent = $(element).html() || "";
    if (!scriptContent || scriptContent.length < 50) return;
    const type = $(element).attr("type") || "";
    if (type === "application/ld+json") return;

    const imageUrlPattern = /["']((https?:)?\/\/[^"'\s,]+\.(?:jpe?g|png|webp|avif)[^"'\s,]*?)["']/gi;
    let match;
    while ((match = imageUrlPattern.exec(scriptContent)) !== null) {
      const rawUrl = match[1];
      if (!rawUrl) continue;
      const normalized = normalizeUrl(rawUrl, url);
      if (!normalized) continue;
      if (!isValidImageUrl(normalized)) continue;

      const isStrong = STRONG_PRODUCT_URL_PATTERNS.some((p) => p.test(normalized));
      const isMarketing = MARKETING_URL_PATTERNS.some((p) => p.test(normalized));
      if (isMarketing) continue;

      const scriptId = $(element).attr("id") || "";
      const isNextData = scriptId === "__NEXT_DATA__" || scriptContent.includes("__NEXT_DATA__");
      const isInitialState = scriptContent.includes("__INITIAL_STATE__") || scriptContent.includes("pageData");
      const isShopify = scriptContent.includes("Shopify.") || scriptContent.includes("ShopifyAnalytics");

      let priority;
      if (isNextData || isInitialState || isShopify) {
        priority = isStrong ? SOURCE_PRIORITY.dom_strong : SOURCE_PRIORITY.itemprop_image;
      } else {
        priority = isStrong ? SOURCE_PRIORITY.dom_strong : SOURCE_PRIORITY.dom_weak;
      }

      addCandidate(normalized, priority);
    }
  });

  // Sort, deduplicate and limit
  let finalImages = dedupeImagesByScore(imageCandidates).slice(0, MAX_IMAGE_RESULTS);

  // ── Final price ──
  const currencyHintList = Array.from(currencyValues);
  const price = findPriceInTexts(priceValues, currencyHintList) ||
    (priceValues.length && currencyHintList.length
      ? combinePriceWithCurrency(priceValues.map((v) => v.match(/\d[\d.,]*/)?.[0] || null).find(Boolean), currencyHintList[0])
      : null);

  const normalizedPrice = normalizePriceOutput(price, currencyHintList);

  return {
    title,
    description,
    price: normalizedPrice && `${normalizedPrice}`.trim() ? normalizedPrice : null,
    images: finalImages,
  };
}

function isValidResult(result) {
  return Boolean(result && result.title && Array.isArray(result.images) && result.images.length > 0);
}

function roundDuration(seconds) {
  return Number(seconds.toFixed(3));
}

function isDisallowedHostname(hostname) {
  const normalizedHost = `${hostname || ""}`.trim().toLowerCase().replace(/^\[|\]$/g, "");
  if (!normalizedHost || normalizedHost === "localhost") return true;
  const ipType = isIP(normalizedHost);
  if (ipType === 4) {
    const octets = normalizedHost.split(".").map((part) => Number.parseInt(part, 10));
    if (octets.length !== 4 || octets.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) return true;
    if (normalizedHost === "127.0.0.1" || normalizedHost === "0.0.0.0") return true;
    const [first, second] = octets;
    if (first === 10) return true;
    if (first === 172 && second >= 16 && second <= 31) return true;
    if (first === 192 && second === 168) return true;
    if (first === 169 && second === 254) return true;
  }
  if (ipType === 6) {
    if (normalizedHost === "::1") return true;
    const firstHextet = (normalizedHost.split(":").find(Boolean) || "0").toLowerCase();
    if (/^f[c-d][0-9a-f]{0,2}$/i.test(firstHextet)) return true;
  }
  return false;
}

function isAllowedScrapeUrl(rawUrl) {
  if (typeof rawUrl !== "string") return false;
  const input = rawUrl.trim();
  if (!input || input.length > 2048) return false;
  try {
    const parsed = new URL(input);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return false;
    if (isDisallowedHostname(parsed.hostname)) return false;
    return true;
  } catch {
    return false;
  }
}

async function runStageWithHardTimeout(stageName, timeoutMs, stageFn) {
  const timeoutResult = new Promise((resolve) => {
    setTimeout(() => {
      resolve({ ok: false, stage: stageName, error: `${stageName} hard timeout after ${timeoutMs}ms` });
    }, timeoutMs);
  });
  return Promise.race([stageFn(), timeoutResult]);
}

function decodeHtmlEntities(value) {
  if (typeof value !== "string") return value;
  return he.decode(value);
}

function buildSuccessPayload(data, meta) {
  const imageObjects = (data.images || []).map((url) => ({ url }));
  return {
    ok: true,
    title: decodeHtmlEntities(data.title) || null,
    description: decodeHtmlEntities(data.description) || null,
    price: data.price || null,
    images: imageObjects,
    meta,
  };
}

async function runStage1(url) {
  if (process.env.DISABLE_STAGE1 === "true") {
    return { ok: false, stage: "stage1", error: "Stage1 disabled" };
  }
  const stageStart = performance.now();
  let browser;
  let page;
  let pageSetup = null;
  let lastError = null;
  let lastErrorMessage = null;
  let usingSharedBrowser = false;
  try {
    try {
      browser = await acquireSharedBrowser();
      usingSharedBrowser = true;
    } catch {
      browser = await puppeteer.launch(createBrowserLaunchOptions());
      usingSharedBrowser = false;
    }
    if (!browser) throw new Error("Browser launch failed");
    page = await browser.newPage();
    pageSetup = await configurePage(page, url);
    const { userAgent } = pageSetup;
    const navigationStart = performance.now();
    let navigationMeta = null;
    let navigationError = null;
    try {
      navigationMeta = await navigatePage(page, url);
    } catch (err) {
      navigationError = err;
      lastError = err;
      lastErrorMessage = err?.message || "Navigation error";
    }
    const navigationDurationSeconds = navigationMeta?.durationSeconds ?? navigationError?.navigationDurationSeconds ?? roundDuration((performance.now() - navigationStart) / 1000);
    const navigationWaitUntil = navigationMeta?.waitUntil ?? navigationError?.navigationWaitUntil ?? null;
    const navigationTimedOut = (navigationMeta && navigationMeta.navigationTimedOut) || Boolean(navigationError?.navigationTimedOut);
    await delay(randomBetween(...HUMAN_DELAY_RANGE));
    const html = await page.content();
    const extracted = extractFromHtmlContent(html, url);
    if (isValidResult(extracted)) {
      const durationSeconds = roundDuration((performance.now() - stageStart) / 1000);
      return buildSuccessPayload(extracted, {
        stage: "stage1", blocked: false, fallbackUsed: false, durationSeconds,
        network: { durationSeconds }, userAgent, navigationWaitUntil, navigationTimedOut,
      });
    }
    if (!lastErrorMessage) lastErrorMessage = "Stage1 produced no valid result";
    if (navigationError) throw navigationError;
  } catch (err) {
    lastError = err;
    lastErrorMessage = err?.message || lastErrorMessage || "Stage1 failed";
  } finally {
    if (page) await page.close().catch(() => {});
    if (browser && !usingSharedBrowser) await browser.close().catch(() => {});
    if (usingSharedBrowser && browser && !browser.isConnected?.()) sharedBrowserPromise = null;
  }
  return { ok: false, stage: "stage1", error: lastErrorMessage || lastError?.message || "Stage1 failed" };
}

async function runStage3(url) {
  const apiKey = process.env.BRIGHTDATA_API_KEY;
  if (!apiKey) return { ok: false, stage: "stage3", error: "BRIGHTDATA_API_KEY missing" };
  const stageStart = performance.now();
  const payload = {
    zone: process.env.BRIGHTDATA_ZONE || "web_unlocker1",
    url,
    format: "raw",
  };
  const headers = {
    Authorization: `Bearer ${process.env.BRIGHTDATA_API_KEY}`,
    "Content-Type": "application/json",
  };
  let attempts = 1;
  try {
    const response = await axios.post("https://api.brightdata.com/request", payload, {
      headers, timeout: NAVIGATION_TIMEOUT, responseType: "arraybuffer",
    });
    const responseBuffer = Buffer.isBuffer(response.data)
      ? response.data
      : response.data instanceof ArrayBuffer
        ? Buffer.from(response.data)
        : Buffer.from(response.data || "", "utf8");
    const utf8Payload = responseBuffer.toString("utf8");
    const htmlCandidates = [];
    if (utf8Payload.trim()) htmlCandidates.push(utf8Payload);
    let parsedPayload = null;
    try { parsedPayload = JSON.parse(utf8Payload); } catch { parsedPayload = null; }
    if (parsedPayload) {
      const wrappedCandidate = pickBrightDataHtmlCandidate(parsedPayload);
      if (wrappedCandidate) htmlCandidates.push(wrappedCandidate);
    }
    const decodedCandidates = htmlCandidates.map((candidate) => decodeLikelyBase64(candidate)).filter((candidate) => typeof candidate === "string" && candidate.trim().length > 0);
    htmlCandidates.push(...decodedCandidates);
    const htmlContent = htmlCandidates.find((candidate) => {
      if (typeof candidate !== "string") return false;
      const trimmed = candidate.trim();
      if (!trimmed) return false;
      return /<html|<!doctype html|<body|<head/i.test(trimmed) || !trimmed.startsWith("{");
    });
    if (!htmlContent) return { ok: false, stage: "stage3", attempts, error: "Empty response body from BrightData" };
    const extracted = extractFromHtmlContent(htmlContent, url);
    if (!isValidResult(extracted)) return { ok: false, stage: "stage3", attempts, error: "Invalid BrightData extraction" };
    const durationSeconds = roundDuration((performance.now() - stageStart) / 1000);
    return buildSuccessPayload(extracted, {
      stage: "brightdata", fallbackUsed: true, blocked: false,
      costEstimate: 0.0015, durationSeconds, network: { durationSeconds }, attempts,
    });
  } catch (err) {
    const statusText = err?.response?.status ? ` (status ${err.response.status})` : "";
    const message = err?.message ? `${err.message}${statusText}` : `BrightData request failed${statusText}`;
    return { ok: false, stage: "stage3", attempts, error: message };
  }
}

async function scrapeWithStages(url) {
  if (!url) throw new Error("URL is required");
  const requestStart = performance.now();
  const steps = { stage1: "skipped", stage3: "skipped" };

  const resolveStageStatus = (result, attempted, allowMissingAsSkipped) => {
    if (!attempted) return "skipped";
    if (result?.ok) return "success";
    if (allowMissingAsSkipped) {
      const errorText = `${result?.error || ""}`.toLowerCase();
      if (errorText.includes("missing") || errorText.includes("skip")) return "skipped";
    }
    return "failed";
  };

  const buildStageLog = (stageName, result, attempted) => {
    const ok = Boolean(result?.ok);
    const error = result?.error || null;
    const errorText = `${error || ""}`.toLowerCase();
    const missingError = errorText.includes("missing");
    const status = !attempted ? "skipped" : ok ? "success" : missingError ? "skipped" : "failed";
    const blocked = Boolean(result?.meta?.blocked || result?.status === "blocked");
    const durationSeconds = typeof result?.meta?.durationSeconds === "number" ? result.meta.durationSeconds : null;
    let meta = result?.meta ? { ...result.meta } : null;
    if (stageName === "stage3") {
      const brightDataUsed = Boolean(attempted && !missingError);
      meta = { ...(meta || {}), brightDataUsed };
    }
    return { attempted, status, ok, error, blocked, durationSeconds, meta };
  };

  const stage1Result = await runStageWithHardTimeout("stage1", STAGE1_HARD_TIMEOUT_MS, () => runStage1(url));
  steps.stage1 = resolveStageStatus(stage1Result, true, false);
  let finalResult = null;
  let finalStage = stage1Result?.meta?.stage || "stage1";

  if (stage1Result?.ok) {
    finalResult = stage1Result;
  }

  let stage3Result = null;
  let stage3Attempted = false;
  if (!finalResult) {
    stage3Attempted = true;
    stage3Result = await runStageWithHardTimeout("stage3", STAGE3_HARD_TIMEOUT_MS, () => runStage3(url));
    if (stage3Result?.ok) {
      finalResult = stage3Result;
      finalStage = stage3Result?.meta?.stage || "brightdata";
    }
  }
  steps.stage3 = resolveStageStatus(stage3Result, stage3Attempted, true);

  if (!finalResult) {
    finalResult = { ok: false, status: "blocked" };
    finalStage = "failed";
  }

  const durationSeconds = roundDuration((performance.now() - requestStart) / 1000);
  const blocked = Boolean(
    finalResult?.meta?.blocked || finalResult?.status === "blocked" ||
    stage1Result?.status === "blocked" || stage3Result?.status === "blocked"
  );

  const logEntry = {
    event: "SCRAPE",
    url,
    stage: finalStage,
    ok: Boolean(finalResult?.ok),
    blocked,
    duration: durationSeconds,
    imagesCount: Array.isArray(finalResult?.images) ? finalResult.images.length : 0,
    title: finalResult?.title || null,
    price: finalResult?.price || null,
    timestamp: new Date().toISOString(),
    steps,
    stages: {
      stage1: buildStageLog("stage1", stage1Result, true),
      stage3: buildStageLog("stage3", stage3Result, stage3Attempted),
    },
  };

  if (!finalResult.ok) {
    logEntry.errors = {
      stage1: stage1Result?.error || null,
      stage3: stage3Result?.error || null,
    };
  }

  console.log(JSON.stringify(logEntry));
  return finalResult;
}

app.get("/", (_req, res) => {
  res.json({ ok: true, status: "feednly-scraper", uptime: process.uptime() });
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    uptime: process.uptime(),
    brightDataConfigured: Boolean(process.env.BRIGHTDATA_API_KEY),
  });
});

app.get("/scrape", async (req, res) => {
  const { url } = req.query;
  if (!url) {
    res.status(400).json({ ok: false, error: "Missing url query parameter" });
    return;
  }
  if (!isAllowedScrapeUrl(`${url}`)) {
    res.status(400).json({ ok: false, error: "Invalid or disallowed URL" });
    return;
  }
  try {
    const result = await scrapeWithStages(`${url}`);
    res.json(result);
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || "Scrape failed" });
  }
});

const portValue = process.env.PORT;
let PORT = Number.parseInt(`${portValue ?? ""}`.trim(), 10);
if (!Number.isFinite(PORT) || PORT <= 0) PORT = 8080;

app.listen(PORT, "0.0.0.0");
