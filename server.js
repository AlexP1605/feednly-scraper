import express from "express";
import * as cheerio from "cheerio";
import puppeteer from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import axios from "axios";
import fs from "node:fs/promises";
import path from "node:path";
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
  Number.parseInt(process.env.SCRAPER_NAVIGATION_TIMEOUT_MS || "45000", 10) || 45000
);
const HUMAN_DELAY_RANGE = [1200, 2400];
const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
];
const VIEWPORT_WIDTHS = [1280, 1366, 1440, 1536, 1680, 1920];
const VIEWPORT_HEIGHTS = [720, 768, 900, 960, 1080];
const MAX_IMAGE_RESULTS = Math.max(
  1,
  Number.parseInt(process.env.SCRAPER_MAX_IMAGE_RESULTS || "15", 10) || 15
);
const BEST_IMAGE_LIMIT = Math.min(10, MAX_IMAGE_RESULTS);

const PRODUCT_IMAGE_KEYWORDS = [
  "product",
  "media",
  "gallery",
  "item",
  "detail",
  "zoom",
  "images",
  "photo",
  "pdp",
];
const PLACEHOLDER_KEYWORDS = [
  "placeholder",
  "transparent",
  "pixel",
  "spacer",
  "blank",
  "loading",
  "spinner",
  "logo",
  "icon",
];

const CURRENCY_SYMBOLS = [
  "$",
  "€",
  "£",
  "¥",
  "₹",
  "₩",
  "₽",
  "₫",
  "₦",
  "₪",
  "฿",
  "₴",
  "₱",
  "₲",
  "₵",
  "₡",
  "R$",
];

const CURRENCY_CODES = [
  "USD",
  "EUR",
  "GBP",
  "JPY",
  "CNY",
  "CAD",
  "AUD",
  "NZD",
  "CHF",
  "HKD",
  "SEK",
  "NOK",
  "DKK",
  "PLN",
  "RON",
  "HUF",
  "CZK",
  "MXN",
  "ARS",
  "BRL",
  "TRY",
  "ZAR",
  "AED",
  "SAR",
  "INR",
  "KRW",
  "RUB",
  "SGD",
  "TWD",
  "MYR",
  "THB",
  "PHP",
  "IDR",
];

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

function parseNumericPrice(value) {
  if (!value) return null;
  const digits = `${value}`.match(/[\d]/g);
  if (!digits || !digits.length) {
    return null;
  }
  let normalized = `${value}`.replace(/[^\d.,]/g, "");
  if (!normalized) {
    return null;
  }
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
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return parsed;
}

function scorePriceCandidate(rawValue, { order = 0, currencyHints = [] } = {}) {
  const value = `${rawValue || ""}`.trim();
  if (!value) {
    return null;
  }
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

  // Prefer earlier discoveries when scores are equal.
  const tieBreaker = order * 0.01;

  return {
    value,
    score: score - tieBreaker,
    order,
  };
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

function shuffleList(values) {
  const list = [...(values || [])];
  for (let i = list.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [list[i], list[j]] = [list[j], list[i]];
  }
  return list;
}

const { TimeoutError } = puppeteer.errors ?? {};

const BROWSER_LAUNCH_ARGS = ["--no-sandbox", "--disable-setuid-sandbox"];
let sharedBrowserPromise = null;
const cookieCache = new Map();

function createBrowserLaunchOptions(proxyUrl) {
  const args = [...BROWSER_LAUNCH_ARGS];
  if (proxyUrl) {
    args.push(`--proxy-server=${proxyUrl}`);
  }
  return { headless: "new", args };
}

async function acquireSharedBrowser() {
  if (sharedBrowserPromise) {
    try {
      const existing = await sharedBrowserPromise;
      const hasProcess = Boolean(existing?.process?.()?.pid);
      if (existing?.isConnected?.() && hasProcess) {
        return existing;
      }
      await existing?.close?.().catch(() => {});
    } catch {
      sharedBrowserPromise = null;
    }
  }

  sharedBrowserPromise = puppeteer
    .launch(createBrowserLaunchOptions())
    .then((browser) => {
      browser?.once?.("disconnected", () => {
        sharedBrowserPromise = null;
      });
      return browser;
    })
    .catch((err) => {
      sharedBrowserPromise = null;
      throw err;
    });

  return sharedBrowserPromise;
}

async function enableRequestOptimizations(page, targetUrl) {
  let targetHostname = null;
  try {
    targetHostname = new URL(targetUrl).hostname;
  } catch {
    targetHostname = null;
  }

  try {
    await page.setRequestInterception(true);
  } catch {
    return () => {};
  }

  const handler = (request) => {
    try {
      const type = request.resourceType();
      if (type === "stylesheet" || type === "font" || type === "media") {
        request.abort();
        return;
      }
      if (type === "image") {
        if (targetHostname) {
          try {
            const requestHostname = new URL(request.url()).hostname;
            if (requestHostname && requestHostname !== targetHostname) {
              request.abort();
              return;
            }
          } catch {
            request.abort();
            return;
          }
        }
      }
    } catch {
      // Ignore interception errors and fall back to continuing the request.
    }
    request.continue().catch(() => {});
  };

  page.on("request", handler);
  return () => {
    page.off("request", handler);
    page.setRequestInterception(false).catch(() => {});
  };
}

async function configurePage(page, url) {
  const userAgent = pickUserAgent();
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
  const disableInterception = await enableRequestOptimizations(page, url);
  return { userAgent, viewport, disableInterception };
}

async function navigatePage(page, url) {
  const strategies = [
    { waitUntil: "domcontentloaded", label: "domcontentloaded" },
    { waitUntil: "load", label: "load" },
    { waitUntil: "networkidle2", label: "networkidle2" },
  ];
  let lastError = null;
  for (const strategy of strategies) {
    const attemptStart = performance.now();
    try {
      await page.goto(url, { waitUntil: strategy.waitUntil, timeout: NAVIGATION_TIMEOUT });
      await page
        .waitForSelector("body", { timeout: Math.min(10000, NAVIGATION_TIMEOUT) })
        .catch(() => {});
      const durationSeconds = roundDuration((performance.now() - attemptStart) / 1000);
      return { waitUntil: strategy.label, durationSeconds, navigationTimedOut: false };
    } catch (err) {
      const durationSeconds = roundDuration((performance.now() - attemptStart) / 1000);
      lastError = err;
      const isTimeout = TimeoutError && err instanceof TimeoutError;
      err.navigationWaitUntil = strategy.label;
      err.navigationDurationSeconds = durationSeconds;
      err.navigationTimedOut = isTimeout;
      if (!isTimeout) {
        throw err;
      }
    }
  }
  if (lastError) {
    throw lastError;
  }
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
  if (trimmed.startsWith("//")) {
    return `https:${trimmed}`;
  }
  if (/^https?:/i.test(trimmed)) {
    return trimmed;
  }
  if (trimmed.startsWith("data:")) return null;
  try {
    return new URL(trimmed, baseUrl).toString();
  } catch {
    return null;
  }
}

function isLikelyProductImage(url, { requireProductKeyword = true } = {}) {
  if (!url) return false;
  const lower = url.toLowerCase();
  if (!/\.(jpe?g|png|webp|gif)(?:$|\?)/.test(lower)) return false;
  if (PLACEHOLDER_KEYWORDS.some((keyword) => lower.includes(keyword))) return false;
  if (!requireProductKeyword) return true;
  return PRODUCT_IMAGE_KEYWORDS.some((keyword) => lower.includes(keyword));
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
    const widthKeys = ["w", "width", "wid", "rw", "mw"];
    const heightKeys = ["h", "height", "hei", "rh", "mh"];
    for (const key of widthKeys) {
      const candidate = parseDimension(parsed.searchParams.get(key));
      if (candidate) {
        width = candidate;
        break;
      }
    }
    for (const key of heightKeys) {
      const candidate = parseDimension(parsed.searchParams.get(key));
      if (candidate) {
        height = candidate;
        break;
      }
    }
  } catch {
    // ignore URL parsing issues
  }
  if (!width || !height) {
    const match = url.match(/([0-9]{2,4})x([0-9]{2,4})/);
    if (match) {
      if (!width) {
        width = parseDimension(match[1]);
      }
      if (!height) {
        height = parseDimension(match[2]);
      }
    }
  }
  return { width, height };
}

function extractSrcsetCandidates(value) {
  if (!value) return [];
  return value
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => {
      const [urlPart, descriptor] = part.split(/\s+/, 2);
      const result = { url: urlPart, width: null, density: null };
      if (descriptor) {
        if (descriptor.endsWith("w")) {
          const widthValue = parseDimension(descriptor.slice(0, -1));
          if (widthValue) {
            result.width = widthValue;
          }
        } else if (descriptor.endsWith("x")) {
          const densityValue = Number.parseFloat(descriptor.slice(0, -1));
          if (Number.isFinite(densityValue) && densityValue > 0) {
            result.density = densityValue;
          }
        }
      }
      return result;
    })
    .filter((candidate) => candidate.url);
}

function normalizeImageCandidate(candidate, url, options) {
  const normalized = normalizeUrl(candidate, url);
  if (normalized && isLikelyProductImage(normalized, options)) {
    return normalized;
  }
  return null;
}

function addImageCandidate(collection, candidate, url, options) {
  const normalized = normalizeImageCandidate(candidate, url, options);
  if (normalized) {
    collection.push(normalized);
  }
  return normalized;
}

function computeImageScore(url, meta = {}) {
  const dimensions = extractDimensionsFromUrl(url);
  let width = meta.width || dimensions.width || null;
  let height = meta.height || dimensions.height || null;
  if (!width && meta.aspectRatio && height) {
    width = Math.round(height * meta.aspectRatio);
  }
  if (!height && meta.aspectRatio && width) {
    height = Math.round(width / meta.aspectRatio);
  }
  let score = 0;
  if (width && height) {
    score = width * height;
  } else if (width) {
    score = width * 800;
  } else if (height) {
    score = height * 800;
  } else {
    score = 1000 + Math.min(url.length, 500);
    if (/\b(?:large|xl|zoom|big|hero|main|product|detail)\b/i.test(url)) {
      score += 5000;
    }
  }
  if (meta.density && meta.density > 0 && Number.isFinite(meta.density)) {
    score *= meta.density;
  }
  if (meta.order !== undefined && meta.order !== null) {
    score -= meta.order;
  }
  return score;
}

const IMAGE_KEYWORD_BONUSES = ["meta", "og", "product", "main", "hero", "cover"];
const IMAGE_QUALITY_HINTS = ["_large", "-large", "_xlarge", "-xlarge", "_2x", "-2x", "@2x", "1200", "1600"];
const IMAGE_NEGATIVE_KEYWORDS = ["logo", "icon", "nav", "sprite", "small", "thumbnail", "thumb", "avatar"];

function scoreImage(url) {
  if (!url) return 0;
  const normalized = `${url}`.trim();
  if (!normalized) return 0;
  const lower = normalized.toLowerCase();
  let score = 0;

  const extensionMatch = lower.match(/\.([a-z0-9]+)(?:[?#]|$)/);
  if (extensionMatch) {
    const ext = extensionMatch[1];
    if (ext === "svg") {
      score -= 250;
    } else if (["jpg", "jpeg", "png", "webp", "avif", "gif", "bmp", "tif", "tiff", "jfif", "pjpeg", "pjp"].includes(ext)) {
      score += 40;
    } else {
      score += 20;
    }
  } else {
    score += 15;
  }

  for (const keyword of IMAGE_KEYWORD_BONUSES) {
    if (lower.includes(keyword)) {
      score += 25;
    }
  }

  for (const hint of IMAGE_QUALITY_HINTS) {
    if (lower.includes(hint)) {
      score += 18;
    }
  }

  for (const negative of IMAGE_NEGATIVE_KEYWORDS) {
    if (lower.includes(negative)) {
      score -= 35;
    }
  }

  return score;
}

function findPriceInTexts(texts, currencyHints = []) {
  if (!Array.isArray(texts) || !texts.length) return null;
  const normalizedCurrencyHints = Array.from(
    new Set(
      currencyHints
        .map((hint) => `${hint || ""}`.trim())
        .filter(Boolean)
    )
  );

  const candidates = [];

  texts.forEach((text, index) => {
    const normalized = `${text || ""}`.replace(/\s+/g, " ").trim();
    if (!normalized) return;
    for (const regex of PRICE_REGEXES) {
      const match = normalized.match(regex);
      if (match && match[0]) {
        const candidate = scorePriceCandidate(match[0], {
          order: index,
          currencyHints: normalizedCurrencyHints,
        });
        if (candidate) {
          candidates.push(candidate);
        }
        break;
      }
    }
  });

  if (!candidates.length && normalizedCurrencyHints.length) {
    const numberRegex = /\d[\d.,]*/;
    texts.forEach((text, index) => {
      const normalized = `${text || ""}`.replace(/\s+/g, " ").trim();
      if (!normalized) return;
      const numberMatch = normalized.match(numberRegex);
      if (!numberMatch) return;
      const numberValue = numberMatch[0];
      for (const hint of normalizedCurrencyHints) {
        const combined = combinePriceWithCurrency(numberValue, hint);
        const candidate = scorePriceCandidate(combined, {
          order: texts.length + index,
          currencyHints: normalizedCurrencyHints,
        });
        if (candidate) {
          candidates.push(candidate);
        }
      }
    });
  }

  if (!candidates.length) {
    return null;
  }

  const deduped = new Map();
  for (const candidate of candidates) {
    const key = candidate.value;
    const existing = deduped.get(key);
    if (!existing || existing.score < candidate.score) {
      deduped.set(key, candidate);
    }
  }

  const best = Array.from(deduped.values()).sort((a, b) => {
    if (b.score !== a.score) {
      return b.score - a.score;
    }
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
  if (upperPrice.includes(upperCurrency)) {
    return priceText;
  }
  if (CURRENCY_SYMBOLS.some((symbol) => priceText.includes(symbol))) {
    return priceText;
  }
  if (/^[A-Z]{2,3}$/.test(upperCurrency)) {
    return `${priceText} ${upperCurrency}`.trim();
  }
  return `${currencyText} ${priceText}`.trim();
}

function createImageDedupKey(url) {
  if (!url) return "";
  try {
    const parsed = new URL(url);
    const normalizedSearch = new URLSearchParams();
    const skipKeys = new Set([
      "w",
      "width",
      "wid",
      "rw",
      "mw",
      "h",
      "height",
      "hei",
      "rh",
      "mh",
      "ts",
      "timestamp",
      "cache",
      "quality",
      "q",
    ]);
    const sortedKeys = Array.from(parsed.searchParams.keys()).sort();
    for (const key of sortedKeys) {
      if (skipKeys.has(key.toLowerCase())) {
        continue;
      }
      const values = parsed.searchParams.getAll(key);
      for (const value of values) {
        normalizedSearch.append(key.toLowerCase(), value);
      }
    }
    const normalizedQuery = normalizedSearch.toString();
    return `${parsed.origin}${parsed.pathname}${normalizedQuery ? `?${normalizedQuery}` : ""}`.toLowerCase();
  } catch {
    return `${url}`.trim().toLowerCase();
  }
}

function dedupeImages(values) {
  const order = [];
  const seen = new Set();
  const bestByKey = new Map();
  for (const value of values || []) {
    if (!value) continue;
    const trimmed = `${value}`.trim();
    if (!trimmed) continue;
    const key = createImageDedupKey(trimmed);
    const score = computeImageScore(trimmed);
    if (!seen.has(key)) {
      seen.add(key);
      order.push(key);
      bestByKey.set(key, { url: trimmed, score });
      continue;
    }
    const current = bestByKey.get(key);
    if (!current || score > current.score) {
      bestByKey.set(key, { url: trimmed, score });
    }
  }
  return order
    .map((key) => bestByKey.get(key)?.url)
    .filter((url) => typeof url === "string" && url.trim().length > 0);
}

function extractFromHtmlContent(html, url) {
  if (!html) {
    return { title: null, description: null, price: null, images: [] };
  }
  const $ = cheerio.load(html);

  const metaTitle =
    $("meta[property='og:title']").attr("content") ||
    $("meta[name='twitter:title']").attr("content") ||
    $("meta[name='title']").attr("content") ||
    null;
  const domTitle = $("h1").first().text().trim() || $("title").first().text().trim() || null;
  const title = metaTitle || domTitle || null;

  const description =
    $("meta[property='og:description']").attr("content") ||
    $("meta[name='description']").attr("content") ||
    $("meta[name='twitter:description']").attr("content") ||
    $("p").toArray().map((el) => $(el).text().trim()).find((text) => text.length > 60) ||
    null;

  const priceValues = [];
  const currencyValues = new Set();

  function pushPriceValue(value) {
    if (!value) return;
    const normalized = `${value}`.replace(/\s+/g, " ").trim();
    if (normalized) {
      priceValues.push(normalized);
    }
  }

  function pushCurrencyValue(value) {
    if (!value) return;
    const normalized = `${value}`.replace(/\s+/g, " ").trim();
    if (normalized) {
      currencyValues.add(normalized);
    }
  }

  const priceMetaSelectors = [
    "meta[property='product:price:amount']",
    "meta[name='product:price:amount']",
    "meta[itemprop='price']",
    "meta[property='og:price:amount']",
    "meta[name='og:price:amount']",
  ];
  for (const selector of priceMetaSelectors) {
    $(selector)
      .toArray()
      .forEach((element) => {
        const value = $(element).attr("content");
        pushPriceValue(value);
      });
  }

  const currencyMetaSelectors = [
    "meta[itemprop='priceCurrency']",
    "meta[property='product:price:currency']",
    "meta[name='product:price:currency']",
    "meta[property='og:price:currency']",
    "meta[name='og:price:currency']",
  ];
  for (const selector of currencyMetaSelectors) {
    $(selector)
      .toArray()
      .forEach((element) => {
        const value = $(element).attr("content");
        pushCurrencyValue(value);
      });
  }

  const priceElementSelectors = [
    "[class*='price']",
    "[id*='price']",
    "span[itemprop='price']",
    "meta[itemprop='price']",
    "[data-price]",
    "[data-price-amount]",
  ];
  $(priceElementSelectors.join(","))
    .toArray()
    .forEach((element) => {
      const el = $(element);
      const content =
        el.attr("content") ||
        el.attr("data-price") ||
        el.attr("data-price-amount") ||
        el.text();
      pushPriceValue(content);
      const currency =
        el.attr("data-currency") ||
        el.attr("data-price-currency") ||
        el.attr("data-currency-code") ||
        null;
      pushCurrencyValue(currency);
    });

  let price = findPriceInTexts(priceValues, Array.from(currencyValues));

  const images = [];
  const fallbackCandidateMap = new Map();
  let fallbackOrder = 0;

  function registerFallbackCandidate(candidate, meta = {}) {
    const normalized = normalizeImageCandidate(candidate, url, { requireProductKeyword: false });
    if (!normalized) return;
    const order = meta.order ?? fallbackOrder;
    const score = computeImageScore(normalized, { ...meta, order });
    const existing = fallbackCandidateMap.get(normalized);
    if (!existing || existing.score < score) {
      fallbackCandidateMap.set(normalized, { url: normalized, score, order });
    }
    fallbackOrder += 1;
  }

  const imageSelectors = [
    "meta[property='og:image']",
    "meta[property='og:image:url']",
    "meta[name='twitter:image']",
    "meta[name='twitter:image:src']",
    "link[rel='image_src']",
  ];
  for (const selector of imageSelectors) {
    $(selector)
      .toArray()
      .forEach((element) => {
        const candidate = $(element).attr("content") || $(element).attr("href");
        addImageCandidate(images, candidate, url, { requireProductKeyword: false });
        if (candidate) {
          registerFallbackCandidate(candidate, { order: fallbackOrder });
        }
      });
  }

  $("img")
    .toArray()
    .forEach((element) => {
      const el = $(element);
      const src = el.attr("src") || el.attr("data-src");
      const widthAttr =
        el.attr("width") ||
        el.attr("data-width") ||
        el.attr("data-original-width") ||
        el.attr("data-large-width") ||
        el.attr("data-zoom-width") ||
        el.attr("data-image-width");
      const heightAttr =
        el.attr("height") ||
        el.attr("data-height") ||
        el.attr("data-original-height") ||
        el.attr("data-large-height") ||
        el.attr("data-zoom-height") ||
        el.attr("data-image-height");
      const width = parseDimension(widthAttr);
      const height = parseDimension(heightAttr);
      const aspectRatio = width && height ? width / height : null;
      addImageCandidate(images, src, url, { requireProductKeyword: true });
      if (src) {
        registerFallbackCandidate(src, { width, height, aspectRatio });
      }
      const srcsetValues = [el.attr("srcset"), el.attr("data-srcset"), el.attr("data-sources")].filter(Boolean);
      for (const srcset of srcsetValues) {
        extractSrcsetCandidates(srcset).forEach((candidate) => {
          addImageCandidate(images, candidate.url, url, { requireProductKeyword: true });
          registerFallbackCandidate(candidate.url, {
            width: candidate.width || width,
            height,
            aspectRatio,
            density: candidate.density,
          });
        });
      }
    });

  $("source")
    .toArray()
    .forEach((element) => {
      const el = $(element);
      const srcsetValues = [el.attr("srcset"), el.attr("data-srcset"), el.attr("data-src")].filter(Boolean);
      for (const srcset of srcsetValues) {
        extractSrcsetCandidates(srcset).forEach((candidate) => {
          addImageCandidate(images, candidate.url, url, { requireProductKeyword: true });
          registerFallbackCandidate(candidate.url, {
            width: candidate.width,
            density: candidate.density,
          });
        });
      }
    });

  $("script[type='application/ld+json']")
    .toArray()
    .forEach((element) => {
      const text = $(element).text();
      if (!text) return;
      try {
        const json = JSON.parse(text);
        const nodes = Array.isArray(json) ? json : [json];
        const toArray = (value) => (Array.isArray(value) ? value : value ? [value] : []);
        nodes.forEach((node) => {
          if (!node || typeof node !== "object") return;

          if (node.price) {
            pushPriceValue(node.price);
          }
          if (node.priceCurrency) {
            pushCurrencyValue(node.priceCurrency);
          }

          const imageField = node.image || node.images || node.photo || node.thumbnailUrl;
          toArray(imageField).forEach((imageValue) => {
            if (!imageValue) return;
            if (typeof imageValue === "string") {
              addImageCandidate(images, imageValue, url, {
                requireProductKeyword: false,
              });
              registerFallbackCandidate(imageValue, {});
            } else if (typeof imageValue === "object") {
              const imageUrl =
                imageValue.url ||
                imageValue.contentUrl ||
                imageValue.image ||
                imageValue.thumbnailUrl ||
                imageValue['@id'] ||
                null;
              const imageWidth = parseDimension(imageValue.width || imageValue.widthInPixels);
              const imageHeight = parseDimension(imageValue.height || imageValue.heightInPixels);
              if (imageUrl) {
                addImageCandidate(images, imageUrl, url, {
                  requireProductKeyword: false,
                });
                registerFallbackCandidate(imageUrl, {
                  width: imageWidth,
                  height: imageHeight,
                  aspectRatio: imageWidth && imageHeight ? imageWidth / imageHeight : null,
                });
              }
            }
          });

          const offerFields = [...toArray(node.offers), ...toArray(node.aggregateOffer)];
          offerFields.forEach((offer) => {
            if (!offer || typeof offer !== "object") return;
            if (offer.price) {
              pushPriceValue(offer.price);
            }
            if (offer.priceCurrency) {
              pushCurrencyValue(offer.priceCurrency);
            }
            const priceSpecFields = toArray(offer.priceSpecification);
            priceSpecFields.forEach((spec) => {
              if (!spec || typeof spec !== "object") return;
              if (spec.price) {
                pushPriceValue(spec.price);
              }
              if (spec.priceCurrency) {
                pushCurrencyValue(spec.priceCurrency);
              }
            });
          });
        });
      } catch {
        // ignore invalid JSON-LD
      }
    });

  let uniqueImages = dedupeImages(images);
  if (uniqueImages.length < 5 && fallbackCandidateMap.size) {
    const fallbackCandidates = Array.from(fallbackCandidateMap.values())
      .sort((a, b) => b.score - a.score)
      .map((candidate) => candidate.url);
    for (const candidate of fallbackCandidates) {
      if (uniqueImages.includes(candidate)) continue;
      uniqueImages.push(candidate);
      if (uniqueImages.length >= MAX_IMAGE_RESULTS) break;
    }
    uniqueImages = dedupeImages(uniqueImages);
  }

  if (!uniqueImages.length) {
    const fallbackImages = [];
    $("img")
      .toArray()
      .forEach((element) => {
        const el = $(element);
        const src = el.attr("src") || el.attr("data-src");
        addImageCandidate(fallbackImages, src, url, { requireProductKeyword: false });
        const srcsetValues = [el.attr("srcset"), el.attr("data-srcset"), el.attr("data-sources")].filter(Boolean);
        for (const srcset of srcsetValues) {
          extractSrcsetCandidates(srcset).forEach((candidate) => {
            addImageCandidate(fallbackImages, candidate.url, url, { requireProductKeyword: false });
          });
        }
      });
    $("source")
      .toArray()
      .forEach((element) => {
        const el = $(element);
        const srcsetValues = [el.attr("srcset"), el.attr("data-srcset"), el.attr("data-src")].filter(Boolean);
        for (const srcset of srcsetValues) {
          extractSrcsetCandidates(srcset).forEach((candidate) => {
            addImageCandidate(fallbackImages, candidate.url, url, { requireProductKeyword: false });
          });
        }
      });
    uniqueImages = dedupeImages(fallbackImages);
  }

  if (uniqueImages.length > MAX_IMAGE_RESULTS) {
    uniqueImages = uniqueImages.slice(0, MAX_IMAGE_RESULTS);
  }

  const rankedImages = Array.from(new Set(uniqueImages))
    .map((imageUrl) => ({ url: imageUrl, score: scoreImage(imageUrl) }))
    .sort((a, b) => b.score - a.score);
  const bestRankedImages = rankedImages.filter((entry) => entry.score > 0).slice(0, BEST_IMAGE_LIMIT);
  const bestImages = bestRankedImages.map((entry) => entry.url);

  const priceAfterImages = findPriceInTexts(priceValues, Array.from(currencyValues));
  if (!price && priceAfterImages) {
    price = priceAfterImages;
  }

  if (!price && priceValues.length && currencyValues.size) {
    const [firstCurrency] = Array.from(currencyValues);
    const numericCandidate = priceValues
      .map((value) => value.match(/\d[\d.,]*/)?.[0] || null)
      .find(Boolean);
    if (numericCandidate) {
      price = combinePriceWithCurrency(numericCandidate, firstCurrency);
    }
  }

  return { title, description, price: price || null, images: bestImages };
}

function isValidResult(result) {
  return Boolean(result && result.title && Array.isArray(result.images) && result.images.length > 0);
}

async function launchBrowser(proxyUrl) {
  return puppeteer.launch(createBrowserLaunchOptions(proxyUrl));
}

async function loadCookiesForDomain(domain) {
  if (!domain) return [];
  if (!cookieCache.has(domain)) {
    const cookiePath = path.resolve("cookies", `${domain}_cookies.json`);
    const loader = (async () => {
      try {
        const raw = await fs.readFile(cookiePath, "utf8");
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) {
          return parsed.filter((cookie) => cookie && typeof cookie === "object");
        }
      } catch (err) {
        if (err?.code !== "ENOENT") {
          return [];
        }
      }
      return [];
    })();
    cookieCache.set(domain, loader);
  }
  const cookies = await cookieCache.get(domain);
  if (!Array.isArray(cookies) || cookies.length === 0) {
    return [];
  }
  return cookies.map((cookie) => ({ ...cookie }));
}

function roundDuration(seconds) {
  return Number(seconds.toFixed(3));
}

function buildSuccessPayload(data, meta) {
  return {
    ok: true,
    title: data.title || null,
    description: data.description || null,
    price: data.price || null,
    images: data.images || [],
    meta,
  };
}

async function runStage1(url) {
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
      browser = await launchBrowser();
      usingSharedBrowser = false;
    }
    if (!browser) {
      throw new Error("Browser launch failed");
    }
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
    const navigationDurationSeconds =
      navigationMeta?.durationSeconds ??
      navigationError?.navigationDurationSeconds ??
      roundDuration((performance.now() - navigationStart) / 1000);
    const navigationWaitUntil = navigationMeta?.waitUntil ?? navigationError?.navigationWaitUntil ?? null;
    const navigationTimedOut =
      (navigationMeta && navigationMeta.navigationTimedOut) || Boolean(navigationError?.navigationTimedOut);
    await delay(randomBetween(...HUMAN_DELAY_RANGE));
    const html = await page.content();
    const extracted = extractFromHtmlContent(html, url);
    if (isValidResult(extracted)) {
      const durationSeconds = roundDuration((performance.now() - stageStart) / 1000);
      return buildSuccessPayload(extracted, {
        stage: "stage1",
        blocked: false,
        fallbackUsed: false,
        durationSeconds,
        network: { durationSeconds },
        userAgent,
        navigationWaitUntil,
        navigationTimedOut,
      });
    }
    if (!lastErrorMessage) {
      lastErrorMessage = "Stage1 produced no valid result";
    }
    if (navigationError) {
      throw navigationError;
    }
  } catch (err) {
    lastError = err;
    const message = err?.message || lastErrorMessage || "Stage1 failed";
    lastErrorMessage = message;
  } finally {
    try {
      pageSetup?.disableInterception?.();
    } catch {
      // ignore
    }
    if (page) {
      await page.close().catch(() => {});
    }
    if (browser && !usingSharedBrowser) {
      await browser.close().catch(() => {});
    }
    if (usingSharedBrowser && browser && !browser.isConnected?.()) {
      sharedBrowserPromise = null;
    }
  }
  return { ok: false, stage: "stage1", error: lastErrorMessage || lastError?.message || "Stage1 failed" };
}

function parseProxyPool(value) {
  if (!value) return [];
  const entries = `${value}`
    .split(/[\s,;\n]+/)
    .map((part) => part.trim())
    .filter(Boolean);
  const seen = new Set();
  const result = [];
  for (let entry of entries) {
    if (!/^[a-z][\w+.-]*:\/\//i.test(entry)) {
      entry = `http://${entry}`;
    }
    const lower = entry.toLowerCase();
    if (seen.has(lower)) continue;
    seen.add(lower);
    result.push(entry);
  }
  return result;
}

function resolveProxyConfiguration() {
  const primary = parseProxyPool(process.env.SCRAPER_PROXY_POOL);
  const fallbackEnv = process.env.SCRAPER_PROXY_FALLBACK || process.env.SCRAPER_PROXY || "";
  const fallbackCandidates = parseProxyPool(fallbackEnv);
  const seen = new Set(primary.map((entry) => entry.toLowerCase()));
  const fallback = [];
  for (const entry of fallbackCandidates) {
    const lower = entry.toLowerCase();
    if (seen.has(lower)) continue;
    seen.add(lower);
    fallback.push(entry);
  }
  return {
    primary,
    fallback,
    combined: [...primary, ...fallback],
  };
}

async function runStage2(url) {
  const proxyConfig = resolveProxyConfiguration();
  const proxies = shuffleList(proxyConfig.primary);
  if (proxyConfig.fallback.length) {
    proxies.push(...proxyConfig.fallback);
  }
  if (!proxies.length) {
    return { ok: false, stage: "stage2", error: "SCRAPER_PROXY_POOL missing" };
  }
  const domain = (() => {
    try {
      return new URL(url).hostname;
    } catch {
      return null;
    }
  })();

  const stageStart = performance.now();
  let attempts = 0;
  let lastError = null;
  let lastErrorMessage = null;
  for (const proxyRaw of proxies) {
    let proxy;
    try {
      proxy = new URL(proxyRaw);
    } catch (err) {
      lastError = err;
      lastErrorMessage = err?.message ? `Invalid proxy URL: ${err.message}` : "Invalid proxy URL";
      continue;
    }
    attempts += 1;
    const attemptNumber = attempts;
    const proxyServer = `${proxy.protocol}//${proxy.hostname}${proxy.port ? `:${proxy.port}` : ""}`;
    let browser;
    let page;
    let pageSetup = null;
    try {
      browser = await launchBrowser(proxyServer);
      if (!browser) {
        throw new Error("Browser launch failed");
      }
      page = await browser.newPage();
      pageSetup = await configurePage(page, url);
      const { userAgent } = pageSetup;
      if (proxy.username || proxy.password) {
        await page.authenticate({
          username: decodeURIComponent(proxy.username || ""),
          password: decodeURIComponent(proxy.password || ""),
        });
      }
      const cookies = await loadCookiesForDomain(domain);
      if (cookies.length) {
        try {
          await page.setCookie(...cookies);
        } catch (err) {
          if (!lastErrorMessage) {
            lastErrorMessage = err?.message
              ? `Failed to apply cookies: ${err.message}`
              : "Failed to apply cookies";
          }
        }
      }
      await delay(randomBetween(400, 900));
      let navigationMeta = null;
      let navigationError = null;
      const navigationStart = performance.now();
      try {
        navigationMeta = await navigatePage(page, url);
      } catch (err) {
        navigationError = err;
        lastError = err;
        const navMessage = err?.message || "Unknown navigation error";
        lastErrorMessage = `Navigation error on attempt ${attemptNumber}: ${navMessage}`;
      }
      const navigationDurationSeconds =
        navigationMeta?.durationSeconds ??
        navigationError?.navigationDurationSeconds ??
        roundDuration((performance.now() - navigationStart) / 1000);
      const navigationWaitUntil = navigationMeta?.waitUntil ?? navigationError?.navigationWaitUntil ?? null;
      const navigationTimedOut =
        (navigationMeta && navigationMeta.navigationTimedOut) || Boolean(navigationError?.navigationTimedOut);
      await delay(randomBetween(...HUMAN_DELAY_RANGE));
      const html = await page.content();
      const extracted = extractFromHtmlContent(html, url);
      if (isValidResult(extracted)) {
        const durationSeconds = roundDuration((performance.now() - stageStart) / 1000);
        return buildSuccessPayload(extracted, {
          stage: "stage2",
          blocked: false,
          fallbackUsed: false,
          durationSeconds,
          network: { durationSeconds },
          attempts,
          proxy: proxyServer,
          userAgent,
          navigationWaitUntil,
          navigationTimedOut,
        });
      }
      if (!lastErrorMessage) {
        lastErrorMessage = "Stage2 produced no valid result";
      }
      if (navigationError) {
        throw navigationError;
      }
      await delay(randomBetween(800, 1500));
    } catch (err) {
      lastError = err;
      const message = err?.message || lastErrorMessage || "Stage2 attempt failed";
      lastErrorMessage = message;
    } finally {
      try {
        pageSetup?.disableInterception?.();
      } catch {
        // ignore
      }
      if (page) {
        await page.close().catch(() => {});
      }
      if (browser) {
        await browser.close().catch(() => {});
      }
    }
  }
  return {
    ok: false,
    stage: "stage2",
    attempts,
    error: lastErrorMessage || lastError?.message || "Stage2 failed",
  };
}

async function runStage3(url) {
  const apiKey = process.env.BRIGHTDATA_API_KEY;
  if (!apiKey) {
    return { ok: false, stage: "stage3", error: "BRIGHTDATA_API_KEY missing" };
  }
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
    const response = await axios.post(
      "https://api.brightdata.com/request",
      payload,
      { headers, timeout: NAVIGATION_TIMEOUT }
    );

    const responseData = response.data;

    const htmlCandidates = [];
    if (typeof responseData === "string") {
      htmlCandidates.push(responseData);
    }
    if (Buffer.isBuffer(responseData)) {
      htmlCandidates.push(responseData.toString("utf8"));
    }
    if (responseData && typeof responseData === "object") {
      const html =
        responseData?.solution?.response?.body ||
        responseData?.solution?.content ||
        responseData?.response?.body ||
        responseData?.body ||
        "";
      if (typeof html === "string") {
        htmlCandidates.push(html);
      } else if (Buffer.isBuffer(html)) {
        htmlCandidates.push(html.toString("utf8"));
      }
    }

    const htmlContent = htmlCandidates.find(
      (candidate) => typeof candidate === "string" && candidate.trim().length > 0
    );

    if (!htmlContent) {
      return { ok: false, stage: "stage3", attempts, error: "Empty response body" };
    }

    const extracted = extractFromHtmlContent(htmlContent, url);

    if (!isValidResult(extracted)) {
      return { ok: false, stage: "stage3", attempts, error: "Invalid BrightData extraction" };
    }

    const durationSeconds = roundDuration((performance.now() - stageStart) / 1000);
    return buildSuccessPayload(extracted, {
      stage: "brightdata",
      fallbackUsed: true,
      blocked: false,
      costEstimate: 0.0015,
      durationSeconds,
      network: { durationSeconds },
      attempts,
    });
  } catch (err) {
    const statusText = err?.response?.status ? ` (status ${err.response.status})` : "";
    const message = err?.message ? `${err.message}${statusText}` : `BrightData request failed${statusText}`;
    return {
      ok: false,
      stage: "stage3",
      attempts,
      error: message,
    };
  }
}

async function scrapeWithStages(url) {
  if (!url) {
    throw new Error("URL is required");
  }
  const requestStart = performance.now();
  const steps = { stage1: "skipped", stage2: "skipped", stage3: "skipped" };

  const resolveStageStatus = (result, attempted, allowMissingAsSkipped) => {
    if (!attempted) {
      return "skipped";
    }
    if (result?.ok) {
      return "success";
    }
    if (allowMissingAsSkipped) {
      const errorText = `${result?.error || ""}`.toLowerCase();
      if (errorText.includes("missing") || errorText.includes("skip")) {
        return "skipped";
      }
    }
    return "failed";
  };

  const stage1Result = await runStage1(url);
  steps.stage1 = resolveStageStatus(stage1Result, true, false);
  let finalResult = null;
  let finalStage = stage1Result?.meta?.stage || "stage1";

  let stage2Result = null;
  let stage2Attempted = false;
  if (stage1Result?.ok) {
    finalResult = stage1Result;
  } else {
    stage2Attempted = true;
    stage2Result = await runStage2(url);
    if (stage2Result?.ok) {
      finalResult = stage2Result;
      finalStage = stage2Result?.meta?.stage || "stage2";
    }
  }
  steps.stage2 = resolveStageStatus(stage2Result, stage2Attempted, true);

  let stage3Result = null;
  let stage3Attempted = false;
  if (!finalResult) {
    stage3Attempted = true;
    stage3Result = await runStage3(url);
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
    finalResult?.meta?.blocked ||
      finalResult?.status === "blocked" ||
      stage1Result?.status === "blocked" ||
      stage2Result?.status === "blocked" ||
      stage3Result?.status === "blocked"
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
    stageMeta: {
      stage1: stage1Result?.meta || null,
      stage2: stage2Result?.meta || null,
      stage3: stage3Result?.meta || null,
    },
  };

  if (!finalResult.ok) {
    logEntry.errors = {
      stage1: stage1Result?.error || null,
      stage2: stage2Result?.error || null,
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
    proxyPoolConfigured: resolveProxyConfiguration().combined.length > 0,
    brightDataConfigured: Boolean(process.env.BRIGHTDATA_API_KEY),
  });
});

app.get("/scrape", async (req, res) => {
  const { url } = req.query;
  if (!url) {
    res.status(400).json({ ok: false, error: "Missing url query parameter" });
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
if (!Number.isFinite(PORT) || PORT <= 0) {
  PORT = 8080;
}

app.listen(PORT, "0.0.0.0");
