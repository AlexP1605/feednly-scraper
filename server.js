import express from "express";
import * as cheerio from "cheerio";
import axios from "axios";
import pRetry from "p-retry";
import NodeCache from "node-cache";
import { access } from "node:fs/promises";
import { constants as fsConstants } from "node:fs";
import { Buffer } from "node:buffer";

const app = express();
app.set("etag", false);

process.on("uncaughtException", (err) => console.error("❌ Uncaught exception:", err));
process.on("unhandledRejection", (reason) => console.error("⚠️ Unhandled rejection:", reason));

const DEBUG = process.env.DEBUG === "true";

function debugLog(...args) {
  if (DEBUG) {
    console.log("🪲", ...args);
  }
}

const portValue = process.env.PORT;
let PORT = Number.parseInt(`${portValue ?? ""}`.trim(), 10);
if (!Number.isFinite(PORT) || PORT <= 0) {
  console.warn(`Invalid PORT "${portValue}", fallback to 8080.`);
  PORT = 8080;
}

let MAX_RETRIES = Number.parseInt(process.env.MAX_RETRIES || "2", 10);
if (!Number.isFinite(MAX_RETRIES) || MAX_RETRIES < 0) MAX_RETRIES = 2;
const DISABLE_PUPPETEER = process.env.DISABLE_PUPPETEER === "true";

const DEFAULT_WAIT_AFTER_LOAD = Math.max(
  0,
  Number.parseInt(process.env.SCRAPER_WAIT_AFTER_LOAD_MS || "1500", 10) || 0
);
const NAVIGATION_TIMEOUT = Math.max(
  1000,
  Number.parseInt(process.env.SCRAPER_NAVIGATION_TIMEOUT_MS || "60000", 10) ||
    60000
);
const WAIT_JITTER_RATIO = 0.2;

const RAW_PROXY_POOL = (process.env.SCRAPER_PROXY_POOL || "")
  .split(",")
  .map((entry) => entry.trim())
  .filter(Boolean);
const FALLBACK_PROXY = process.env.SCRAPER_PROXY || null;

const CACHE_TTL = Math.max(
  30,
  Number.parseInt(process.env.SCRAPER_CACHE_TTL || "180", 10) || 180
);
const cache = new NodeCache({ stdTTL: CACHE_TTL, checkperiod: 120, useClones: false });

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36",
];

function pickUserAgent() {
  if (!USER_AGENTS.length) {
    return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36";
  }
  const index = Math.floor(Math.random() * USER_AGENTS.length);
  return USER_AGENTS[index];
}

function pickViewport() {
  const widths = [1280, 1366, 1440, 1536, 1680, 1920];
  const heights = [720, 768, 900, 960, 1080];
  const width = widths[Math.floor(Math.random() * widths.length)] + Math.floor(Math.random() * 40);
  const height = heights[Math.floor(Math.random() * heights.length)] + Math.floor(Math.random() * 60);
  const deviceScaleFactor = Math.random() < 0.2 ? 2 : 1;
  return { width, height, deviceScaleFactor };
}

function pickNavigatorOverrides() {
  const languagesPool = [
    ["en-US", "en"],
    ["fr-FR", "fr", "en"],
    ["es-ES", "es", "en"],
    ["de-DE", "de", "en"],
  ];
  const platforms = ["Win32", "MacIntel", "Linux x86_64"];
  const hardwarePool = [4, 6, 8];
  const deviceMemoryPool = [4, 8, 16];
  const maxTouchPointsPool = [0, 1, 2];
  const vendorPool = ["Google Inc.", "Apple Computer, Inc.", "Mozilla Foundation"];
  const pluginsPool = [2, 3, 4, 5];
  return {
    languages: languagesPool[Math.floor(Math.random() * languagesPool.length)],
    platform: platforms[Math.floor(Math.random() * platforms.length)],
    hardwareConcurrency: hardwarePool[Math.floor(Math.random() * hardwarePool.length)],
    deviceMemory: deviceMemoryPool[Math.floor(Math.random() * deviceMemoryPool.length)],
    maxTouchPoints:
      maxTouchPointsPool[Math.floor(Math.random() * maxTouchPointsPool.length)],
    vendor: vendorPool[Math.floor(Math.random() * vendorPool.length)],
    pluginsLength: pluginsPool[Math.floor(Math.random() * pluginsPool.length)],
  };
}

function cleanHeaders(headers) {
  const result = {};
  for (const [key, value] of Object.entries(headers || {})) {
    if (value !== undefined && value !== null && `${value}`.length > 0) {
      result[key] = value;
    }
  }
  return result;
}

function buildSecChUaHeaders(userAgent) {
  const ua = `${userAgent || ""}`;
  const lower = ua.toLowerCase();
  const headers = {};
  if (lower.includes("chrome/")) {
    const chromeMatch = ua.match(/Chrome\/(\d+)/i);
    const version = chromeMatch?.[1] || "123";
    const platform = lower.includes("mac os")
      ? '"macOS"'
      : lower.includes("linux")
      ? '"Linux"'
      : '"Windows"';
    headers["Sec-CH-UA"] = `"Chromium";v="${version}", "Google Chrome";v="${version}", "Not=A?Brand";v="8"`;
    headers["Sec-CH-UA-Mobile"] = "?0";
    headers["Sec-CH-UA-Platform"] = platform;
    headers["Sec-CH-UA-Full-Version-List"] = `"Chromium";v="${version}.0.0.0", "Google Chrome";v="${version}.0.0.0", "Not=A?Brand";v="8.0.0.0"`;
    if (platform === '"Windows"') {
      headers["Sec-CH-UA-Platform-Version"] = '"15.0.0"';
    }
  } else if (lower.includes("safari") && lower.includes("mac os")) {
    headers["Sec-CH-UA"] = '"Not/A)Brand";v="99", "Safari";v="16", "Chromium";v="118"';
    headers["Sec-CH-UA-Mobile"] = "?0";
    headers["Sec-CH-UA-Platform"] = '"macOS"';
  } else if (lower.includes("firefox/")) {
    headers["Sec-CH-UA"] = '"Not/A)Brand";v="99", "Firefox";v="125", "Chromium";v="118"';
    headers["Sec-CH-UA-Mobile"] = "?0";
    headers["Sec-CH-UA-Platform"] = lower.includes("linux") ? '"Linux"' : '"Windows"';
  }
  return cleanHeaders(headers);
}

function buildNavigationHeaders(url) {
  try {
    const target = new URL(url);
    return {
      Referer: `${target.origin}/`,
    };
  } catch {
    return {};
  }
}

const CHROMIUM_CANDIDATES = [
  process.env.PUPPETEER_EXECUTABLE_PATH,
  process.env.CHROME_EXECUTABLE_PATH,
  process.env.CHROMIUM_PATH,
  process.env.CHROME_PATH,
  "/usr/bin/chromium",
  "/usr/bin/google-chrome",
  "/opt/chrome/chrome",
];

let puppeteerModulePromise = null;
let chromeAwsLambdaPromise = null;
const executableChecks = new Map();

async function loadPuppeteer() {
  if (!puppeteerModulePromise) {
    puppeteerModulePromise = (async () => {
      async function loadVanillaPuppeteer() {
        return import("puppeteer")
          .then((mod) => mod?.default ?? mod)
          .catch(async (err) => {
            console.warn(
              "⚠️ Failed to load puppeteer, trying puppeteer-core:",
              err.message
            );
            return import("puppeteer-core")
              .then((mod) => mod?.default ?? mod)
              .catch((innerErr) => {
                puppeteerModulePromise = null;
                console.error("❌ Unable to load Puppeteer module:", innerErr);
                throw innerErr;
              });
          });
      }

      try {
        const puppeteerExtra = await import("puppeteer-extra").then(
          (mod) => mod?.default ?? mod
        );
        try {
          const stealthFactory = await import(
            "puppeteer-extra-plugin-stealth"
          ).then((mod) => mod?.default ?? mod);
          if (stealthFactory && typeof puppeteerExtra.use === "function") {
            puppeteerExtra.use(stealthFactory());
            debugLog("Stealth plugin enabled for Puppeteer");
          }
        } catch (pluginErr) {
          console.warn(
            "⚠️ Puppeteer stealth plugin unavailable:",
            pluginErr.message
          );
        }
        return puppeteerExtra;
      } catch (extraErr) {
        console.warn(
          "⚠️ Failed to load puppeteer-extra, falling back to puppeteer:",
          extraErr.message
        );
        return loadVanillaPuppeteer();
      }
    })();
  }
  return puppeteerModulePromise;
}

async function loadChromeAwsLambdaExecutable() {
  if (!chromeAwsLambdaPromise) {
    chromeAwsLambdaPromise = import("chrome-aws-lambda")
      .then(async (mod) => {
        const candidate = mod?.executablePath || mod?.default?.executablePath;
        if (typeof candidate === "function") {
          try {
            return await candidate();
          } catch (err) {
            debugLog("chrome-aws-lambda executablePath failed:", err.message);
            return null;
          }
        }
        return candidate || null;
      })
      .catch((err) => {
        debugLog("chrome-aws-lambda not available:", err.message);
        return null;
      });
  }
  return chromeAwsLambdaPromise;
}

async function probeExecutable(path) {
  if (!path) return null;
  if (!executableChecks.has(path)) {
    executableChecks.set(
      path,
      access(path, fsConstants.X_OK)
        .then(() => path)
        .catch(() => null)
    );
  }
  return executableChecks.get(path);
}

async function resolveChromiumExecutable() {
  for (const candidate of CHROMIUM_CANDIDATES) {
    const resolved = await probeExecutable(candidate);
    if (resolved) return resolved;
  }
  const lambdaPath = await loadChromeAwsLambdaExecutable();
  if (lambdaPath) {
    const resolved = await probeExecutable(lambdaPath);
    if (resolved) return resolved;
  }
  return undefined;
}

function normalizeProxyConfig(rawProxy) {
  if (!rawProxy) {
    return {
      key: "default",
      launchArg: null,
      credentials: null,
      original: null,
      protocol: null,
      hostname: null,
      port: null,
    };
  }
  try {
    const parsed = new URL(rawProxy);
    const launchArg = `${parsed.protocol}//${parsed.hostname}${
      parsed.port ? `:${parsed.port}` : ""
    }`;
    const credentials =
      parsed.username || parsed.password
        ? {
            username: decodeURIComponent(parsed.username),
            password: decodeURIComponent(parsed.password),
          }
        : null;
    return {
      key: `${parsed.protocol}//${parsed.hostname}:${parsed.port || ""}@$${
        credentials?.username || ""
      }:${credentials?.password || ""}`.replace(/:+$/, ""),
      launchArg,
      credentials,
      original: rawProxy,
      protocol: parsed.protocol.replace(":", ""),
      hostname: parsed.hostname,
      port: parsed.port ? Number.parseInt(parsed.port, 10) : null,
    };
  } catch {
    return {
      key: rawProxy,
      launchArg: rawProxy,
      credentials: null,
      original: rawProxy,
      protocol: null,
      hostname: null,
      port: null,
    };
  }
}

function pickProxyConfig() {
  if (RAW_PROXY_POOL.length) {
    const raw = RAW_PROXY_POOL[Math.floor(Math.random() * RAW_PROXY_POOL.length)];
    return normalizeProxyConfig(raw);
  }
  return normalizeProxyConfig(FALLBACK_PROXY);
}

async function launchBrowser(proxyConfig) {
  const puppeteer = await loadPuppeteer();
  const executablePath = await resolveChromiumExecutable();
  const args = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-accelerated-2d-canvas",
    "--disable-gpu",
    "--no-zygote",
    "--single-process",
    "--hide-scrollbars",
    "--mute-audio",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-renderer-backgrounding",
    "--disable-blink-features=AutomationControlled",
    "--no-first-run",
    "--no-default-browser-check",
    "--lang=fr-FR,fr",
  ];

  if (proxyConfig?.launchArg) {
    args.push(`--proxy-server=${proxyConfig.launchArg}`);
  }

  const launchOptions = {
    headless: true,
    args,
    executablePath,
    ignoreHTTPSErrors: true,
  };

  debugLog("Launching Chromium", { executablePath, proxy: proxyConfig?.original });
  const browser = await puppeteer.launch(launchOptions);
  debugLog("Chromium launched");
  return browser;
}

class BrowserPool {
  constructor() {
    this.pool = new Map();
    this.cleanupTimer = setInterval(() => {
      this.cleanup().catch((err) => console.warn("⚠️ Browser cleanup failed:", err.message));
    }, 60000);
    this.cleanupTimer.unref?.();
  }

  async acquire(proxyConfig) {
    const key = proxyConfig?.key || "default";
    let entry = this.pool.get(key);
    if (!entry) {
      entry = {
        browserPromise: launchBrowser(proxyConfig),
        activePages: 0,
        lastUsed: Date.now(),
        proxyConfig,
      };
      this.pool.set(key, entry);
    }
    const browser = await entry.browserPromise;
    const page = await browser.newPage();
    entry.activePages += 1;
    entry.lastUsed = Date.now();
    return { page, browser, key, entry };
  }

  async release(key, page) {
    const entry = this.pool.get(key);
    try {
      if (page && !page.isClosed()) {
        await page.close();
      }
    } catch (err) {
      console.warn("⚠️ Failed to close Puppeteer page:", err.message);
    }
    if (entry) {
      entry.activePages = Math.max(0, entry.activePages - 1);
      entry.lastUsed = Date.now();
    }
  }

  async cleanup() {
    const now = Date.now();
    for (const [key, entry] of this.pool.entries()) {
      if (!entry) continue;
      if (entry.activePages === 0 && now - entry.lastUsed > 2 * 60 * 1000) {
        try {
          const browser = await entry.browserPromise;
          await browser.close();
        } catch (err) {
          debugLog("Browser close error during cleanup:", err.message);
        }
        this.pool.delete(key);
      }
    }
  }

  getHealth() {
    const summary = [];
    let totalActivePages = 0;
    for (const entry of this.pool.values()) {
      if (!entry) continue;
      totalActivePages += entry.activePages;
      summary.push({
        proxy: entry.proxyConfig?.original || null,
        activePages: entry.activePages,
        lastUsed: entry.lastUsed,
      });
    }
    return {
      openBrowsers: summary.length,
      totalActivePages,
      instances: summary,
    };
  }

  async shutdown() {
    clearInterval(this.cleanupTimer);
    for (const entry of this.pool.values()) {
      try {
        const browser = await entry.browserPromise;
        await browser.close();
      } catch (err) {
        debugLog("Browser close error during shutdown:", err.message);
      }
    }
    this.pool.clear();
  }
}

const browserPool = new BrowserPool();

function createEmptyProduct() {
  return {
    title: null,
    description: null,
    price: null,
    images: [],
    variants: [],
  };
}

function ensureArray(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  return [value];
}

function normalizeUrl(src, baseUrl) {
  if (!src) return null;
  let value = `${src}`.trim();
  if (!value) return null;
  if (value.startsWith("data:")) return null;
  if (value.startsWith("//")) value = "https:" + value;
  if (value.startsWith("/")) {
    try {
      const base = new URL(baseUrl);
      return base.origin + value;
    } catch {
      return value;
    }
  }
  if (!/^https?:/i.test(value)) {
    try {
      const base = new URL(baseUrl);
      return base.origin + (value.startsWith(".") ? value.slice(1) : `/${value}`);
    } catch {
      return value;
    }
  }
  return value;
}

function flattenJsonLd(node, seen = new Set()) {
  const result = [];
  if (!node || typeof node !== "object") return result;
  const stack = [node];
  while (stack.length) {
    const current = stack.pop();
    if (!current || typeof current !== "object") continue;
    if (seen.has(current)) continue;
    seen.add(current);
    if (!Array.isArray(current)) result.push(current);
    const values = Array.isArray(current) ? current : Object.values(current);
    for (const value of values) {
      if (value && typeof value === "object") stack.push(value);
    }
  }
  return result;
}

function parseJsonLdScripts(jsonLdScripts) {
  const nodes = [];
  for (const script of jsonLdScripts || []) {
    if (typeof script !== "string") continue;
    const trimmed = script.trim();
    if (!trimmed) continue;
    try {
      const parsed = JSON.parse(trimmed);
      nodes.push(...flattenJsonLd(parsed));
    } catch (err) {
      debugLog("Invalid JSON-LD script skipped:", err.message);
    }
  }
  return nodes;
}

function typeMatches(node, targetType) {
  const type = node?.["@type"];
  if (!type) return false;
  const lowerTarget = `${targetType}`.toLowerCase();
  if (Array.isArray(type)) {
    return type.some((t) => `${t}`.toLowerCase() === lowerTarget);
  }
  return `${type}`.toLowerCase() === lowerTarget;
}

function derivePriceFromNode(node) {
  if (!node || typeof node !== "object") return null;
  const candidates = [
    node.price,
    node.priceValue,
    node.priceAmount,
    node.priceSpecification?.price,
    node.offers?.price,
    node.offers?.priceSpecification?.price,
    node.offers?.priceSpecification?.priceCurrency
      ? `${node.offers?.priceSpecification?.price}`
      : null,
    node.offers?.offers?.price,
    node.lowPrice,
    node.highPrice,
    node.amount,
  ];
  for (const candidate of candidates) {
    if (
      candidate !== undefined &&
      candidate !== null &&
      `${candidate}`.toString().trim().length
    ) {
      return `${candidate}`.toString().trim();
    }
  }
  return null;
}

function collectImagesFromNode(node) {
  if (!node || typeof node !== "object") return [];
  const imageFields = [
    node.image,
    node.images,
    node.imageUrl,
    node.photo,
    node.photos,
    node.thumbnailUrl,
    node.thumbnail,
    node.contentUrl,
  ];
  const urls = [];
  for (const field of imageFields) {
    for (const value of ensureArray(field)) {
      if (typeof value === "string" && value.trim()) {
        urls.push(value.trim());
      } else if (value && typeof value === "object") {
        if (typeof value.url === "string") urls.push(value.url.trim());
        if (typeof value.contentUrl === "string") urls.push(value.contentUrl.trim());
        if (typeof value.imageUrl === "string") urls.push(value.imageUrl.trim());
      }
    }
  }
  return urls;
}

function collectVariantsFromNodes(nodes) {
  const variants = [];
  for (const node of nodes || []) {
    if (!node || typeof node !== "object") continue;
    const offers = ensureArray(node.offers || node.itemListElement || node.variants);
    for (const offer of offers) {
      if (!offer || typeof offer !== "object") continue;
      const variant = {};
      if (offer.sku) variant.sku = `${offer.sku}`;
      if (offer.name) variant.name = `${offer.name}`;
      if (offer.color) variant.color = `${offer.color}`;
      if (offer.size) variant.size = `${offer.size}`;
      const price = derivePriceFromNode(offer);
      if (price) variant.price = price;
      if (offer.url) variant.url = `${offer.url}`;
      if (Object.keys(variant).length) variants.push(variant);
    }
  }
  return variants;
}

function dedupeVariants(variants) {
  if (!variants?.length) return [];
  const seen = new Set();
  const result = [];
  for (const variant of variants) {
    if (!variant || typeof variant !== "object") continue;
    const key = JSON.stringify(
      Object.keys(variant)
        .sort()
        .reduce((acc, cur) => {
          acc[cur] = variant[cur];
          return acc;
        }, {})
    );
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(variant);
  }
  return result;
}

function extractFromJsonLdNodes(nodes) {
  if (!nodes?.length) return createEmptyProduct();
  let title = null;
  let description = null;
  let price = null;
  const images = [];
  const variants = collectVariantsFromNodes(nodes);

  for (const node of nodes) {
    if (!title && typeof node.name === "string" && node.name.trim()) {
      title = node.name.trim();
    }
    if (
      !description &&
      typeof node.description === "string" &&
      node.description.trim()
    ) {
      description = node.description.trim();
    }

    if (!price) {
      const derivedPrice = derivePriceFromNode(node);
      if (derivedPrice) price = derivedPrice;
    }

    for (const imageUrl of collectImagesFromNode(node)) {
      images.push(imageUrl);
    }

    if (!title && typeMatches(node, "Product")) {
      const productName = node.alternateName || node.title;
      if (typeof productName === "string" && productName.trim()) {
        title = productName.trim();
      }
    }
  }

  return { title, description, price, images, variants }
}

function flattenObjectDeep(value, seen = new Set()) {
  const result = [];
  if (!value || typeof value !== "object" || seen.has(value)) return result;
  seen.add(value);
  if (!Array.isArray(value)) {
    result.push(value);
  }
  const values = Array.isArray(value) ? value : Object.values(value);
  for (const nested of values) {
    if (nested && typeof nested === "object") {
      result.push(...flattenObjectDeep(nested, seen));
    }
  }
  return result;
}

function parseNextData(raw) {
  if (!raw || typeof raw !== "string") return null;
  try {
    return JSON.parse(raw);
  } catch (err) {
    debugLog("Unable to parse __NEXT_DATA__ payload:", err.message);
    return null;
  }
}

function extractFromNextData(nextData) {
  if (!nextData) return createEmptyProduct();
  let title = null;
  let description = null;
  let price = null;
  const images = [];
  const variants = [];

  const nodes = flattenObjectDeep(nextData);
  for (const node of nodes) {
    if (!node || typeof node !== "object") continue;
    if (!title && typeof node.name === "string" && node.name.trim()) {
      title = node.name.trim();
    }
    if (
      !description &&
      typeof node.description === "string" &&
      node.description.trim()
    ) {
      description = node.description.trim();
    }
    if (!price) {
      const candidatePrice = derivePriceFromNode(node);
      if (candidatePrice) price = candidatePrice;
    }
    for (const url of collectImagesFromNode(node)) {
      images.push(url);
    }
    if (!title && typeof node.title === "string" && node.title.trim()) {
      title = node.title.trim();
    }

    if (node.sku || node.size || node.color || node.variant) {
      const variant = {};
      if (node.sku) variant.sku = `${node.sku}`;
      if (node.size) variant.size = `${node.size}`;
      if (node.color) variant.color = `${node.color}`;
      if (node.variant) variant.name = `${node.variant}`;
      const variantPrice = derivePriceFromNode(node);
      if (variantPrice) variant.price = variantPrice;
      if (Object.keys(variant).length) variants.push(variant);
    }
  }

  return { title, description, price, images, variants };
}

function parseSrcSet(value) {
  if (!value || typeof value !== "string") return [];
  return value
    .split(",")
    .map((entry) => entry.trim().split(" ")[0])
    .filter(Boolean);
}

function dedupeImages(images) {
  const seen = new Set();
  const result = [];
  for (const image of images || []) {
    if (!image) continue;
    const key = image.trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    result.push(key);
  }
  return result;
}

function extractPriceFromHtml($) {
  const selectors = [
    "meta[property='product:price:amount']",
    "meta[name='product:price:amount']",
    "meta[name='price']",
    "meta[itemprop='price']",
    "[itemprop='price']",
    "span[class*='price']",
    "div[class*='price']",
    "p[class*='price']",
    "span[data-price]",
    "div[data-price]",
  ];
  for (const selector of selectors) {
    const element = $(selector).first();
    if (!element.length) continue;
    const content = element.attr("content") || element.attr("data-price") || element.text();
    if (content && content.trim()) {
      return content.trim();
    }
  }
  return null;
}

function collectMetaImages($, url) {
  const imageSelectors = [
    "meta[property='og:image']",
    "meta[property='og:image:url']",
    "meta[name='twitter:image']",
    "meta[name='twitter:image:src']",
    "link[rel='image_src']",
    "link[rel='apple-touch-icon']",
    "link[rel='apple-touch-icon-precomposed']",
  ];
  const images = [];
  for (const selector of imageSelectors) {
    const elements = $(selector).toArray();
    for (const element of elements) {
      const $el = $(element);
      const attr = $el.attr("content") || $el.attr("href");
      const normalized = normalizeUrl(attr, url);
      if (normalized) images.push(normalized);
    }
  }
  return images;
}

function extractJsonLdScriptsFromHtml(html) {
  const $ = cheerio.load(html);
  return $("script[type='application/ld+json']")
    .map((_, el) => $(el).text())
    .get();
}

function extractInlineNextData(html) {
  if (!html) return null;
  const nextMatch = html.match(
    /<script[^>]*id=["']__NEXT_DATA__["'][^>]*>(.*?)<\/script>/s
  );
  if (nextMatch) return nextMatch[1];
  const windowMatch = html.match(/window\.__NEXT_DATA__\s*=\s*(\{.*?\})<\//s);
  if (windowMatch) return windowMatch[1];
  const nuxtMatch = html.match(
    /<script[^>]*id=["']__NUXT__["'][^>]*>(.*?)<\/script>/s
  );
  if (nuxtMatch) return nuxtMatch[1];
  return null;
}

function extractFromHtml(html, url, jsonLdScripts = null, nextDataPayload = null) {
  if (!html) return createEmptyProduct();
  const $ = cheerio.load(html);
  let title =
    $("meta[property='og:title']").attr("content") || $("title").first().text() || null;
  let description =
    $("meta[property='og:description']").attr("content") ||
    $("meta[name='description']").attr("content") ||
    null;
  let price = extractPriceFromHtml($);

  if (title) {
    title = title.replace(/Prix Fnac/gi, "").replace(/\s{2,}/g, " ").trim();
  }

  const scripts = jsonLdScripts ?? extractJsonLdScriptsFromHtml(html);
  const jsonLdNodes = parseJsonLdScripts(scripts);
  const jsonLdExtraction = extractFromJsonLdNodes(jsonLdNodes);

  const inlineNextData =
    nextDataPayload || extractInlineNextData(html) || $("#__NEXT_DATA__").html() || $("#__NUXT__").html();
  const nextData = parseNextData(inlineNextData);
  const nextDataExtraction = extractFromNextData(nextData);

  if (!title) title = jsonLdExtraction.title || nextDataExtraction.title || title;
  if (!description)
    description =
      jsonLdExtraction.description || nextDataExtraction.description || description;
  if (!price) price = jsonLdExtraction.price || nextDataExtraction.price || price;

  const images = [];
  images.push(...collectMetaImages($, url));

  $("img, source, picture").each((_, el) => {
    const $el = $(el);
    const srcAttributes = [
      $el.attr("src"),
      $el.attr("data-src"),
      $el.attr("data-original"),
      $el.attr("data-lazy"),
      $el.attr("data-img"),
      $el.attr("content"),
    ];
    const srcsetAttributes = [
      $el.attr("srcset"),
      $el.attr("data-srcset"),
      $el.attr("data-sizes"),
    ];
    for (const src of srcAttributes) {
      const normalized = normalizeUrl(src, url);
      if (normalized) images.push(normalized);
    }
    for (const srcset of srcsetAttributes) {
      for (const candidate of parseSrcSet(srcset)) {
        const normalized = normalizeUrl(candidate, url);
        if (normalized) images.push(normalized);
      }
    }
  });

  $('[style*="background-image"]').each((_, el) => {
    const style = $(el).attr("style");
    if (!style) return;
    const matches = style.match(/url\(([^)]+)\)/gi) || [];
    for (const match of matches) {
      const urlMatch = match.match(/url\((['"]?)(.*?)\1\)/i);
      const candidate = urlMatch?.[2];
      const normalized = normalizeUrl(candidate, url);
      if (normalized) images.push(normalized);
    }
  });

  images.push(
    ...ensureArray(jsonLdExtraction.images).map((img) => normalizeUrl(img, url))
  );
  images.push(
    ...ensureArray(nextDataExtraction.images).map((img) => normalizeUrl(img, url))
  );

  const cleanedImages = dedupeImages(images.filter(Boolean)).slice(0, 150);
  const variants = dedupeVariants([
    ...ensureArray(jsonLdExtraction.variants),
    ...ensureArray(nextDataExtraction.variants),
  ]);

  return { title, description, price, images: cleanedImages, variants };
}

function safeJsonParse(body) {
  if (typeof body !== "string") return null;
  try {
    return JSON.parse(body);
  } catch {
    return null;
  }
}

function extractFromNetworkPayloads(payloads) {
  if (!payloads?.length) return createEmptyProduct();
  let merged = createEmptyProduct();
  for (const payload of payloads) {
    const parsed = safeJsonParse(payload?.body);
    if (!parsed) continue;
    const extraction = extractFromNextData(parsed);
    merged = mergeProductData(merged, extraction);
  }
  return merged;
}

function mergeProductData(...sources) {
  const result = createEmptyProduct();
  for (const source of sources) {
    if (!source) continue;
    if (!result.title && source.title) result.title = source.title;
    if (!result.description && source.description)
      result.description = source.description;
    if (!result.price && source.price) result.price = source.price;
    if (source.images?.length) {
      result.images = dedupeImages([...result.images, ...source.images]);
    }
    if (source.variants?.length) {
      result.variants = dedupeVariants([...result.variants, ...source.variants]);
    }
  }
  return result;
}

function hasProductSignals(product) {
  if (!product) return false;
  return (
    !!(product.title && product.title.trim()) ||
    !!(product.description && product.description.trim()) ||
    !!(product.price && `${product.price}`.trim()) ||
    (Array.isArray(product.images) && product.images.length > 0)
  );
}

function detectDynamicPage(html) {
  if (!html) return true;
  if (/<script[^>]+id=["']__NEXT_DATA__/i.test(html)) return true;
  if (/<script[^>]+id=["']__NUXT__/i.test(html)) return true;
  if (/window\.__NUXT__=/i.test(html)) return true;
  if (/data-reactroot/i.test(html)) return true;
  if (/ng-version=|<app-root/i.test(html)) return true;
  if (/Please enable JavaScript/i.test(html)) return true;
  const scriptCount = (html.match(/<script/gi) || []).length;
  const bodyText = html.replace(/<[^>]+>/g, "");
  if (bodyText.trim().length < 200 && scriptCount > 30) return true;
  return false;
}

async function fetchWithAxios(url, proxyConfig) {
  const userAgent = pickUserAgent();
  const secChHeaders = buildSecChUaHeaders(userAgent);
  const navigationHeaders = buildNavigationHeaders(url);
  const requestConfig = {
    headers: {
      "User-Agent": userAgent,
      ...secChHeaders,
      ...navigationHeaders,
      "Accept-Encoding": "gzip, deflate, br",
      "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
      Accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
      DNT: "1",
      Connection: "keep-alive",
      "Upgrade-Insecure-Requests": "1",
      "Sec-Fetch-Dest": "document",
      "Sec-Fetch-Mode": "navigate",
      "Sec-Fetch-Site": "none",
      "Sec-Fetch-User": "?1",
    },
    timeout: 30000,
    maxRedirects: 5,
    decompress: true,
    responseType: "text",
    validateStatus: (status) => status >= 200 && status < 400,
  };

  if (proxyConfig?.protocol === "http" || proxyConfig?.protocol === "https") {
    requestConfig.proxy = {
      protocol: proxyConfig.protocol,
      host: proxyConfig.hostname,
      port: proxyConfig.port || (proxyConfig.protocol === "https" ? 443 : 80),
    };
    if (proxyConfig.credentials) {
      requestConfig.proxy.auth = proxyConfig.credentials;
    }
  } else if (proxyConfig?.original) {
    requestConfig.proxy = false;
    if (proxyConfig.credentials) {
      requestConfig.headers["Proxy-Authorization"] =
        "Basic " +
        Buffer.from(
          `${proxyConfig.credentials.username}:${proxyConfig.credentials.password}`
        ).toString("base64");
    }
  }

  const res = await axios.get(url, requestConfig);
  debugLog("Axios fetch complete", { status: res.status, proxy: proxyConfig?.original });
  const html = res.data;
  const jsonLdScripts = extractJsonLdScriptsFromHtml(html);
  const nextDataPayload = extractInlineNextData(html);
  const dynamic = detectDynamicPage(html);
  return { html, jsonLdScripts, nextDataPayload, userAgent, isLikelyDynamic: dynamic };
}

async function waitForSelectors(page, selectors, timeout) {
  if (!selectors?.length) return;
  for (const selector of selectors) {
    try {
      if (selector.startsWith("xpath:")) {
        const xpathExpression = selector.slice("xpath:".length).trim();
        await page.waitForXPath(xpathExpression, { timeout });
      } else {
        await page.waitForSelector(selector, { timeout });
      }
    } catch (err) {
      console.warn(`⚠️ Timeout waiting for selector "${selector}":`, err.message);
    }
  }
}

async function scrapeWithPuppeteer(url, options = {}) {
  if (DISABLE_PUPPETEER)
    throw new Error("Puppeteer disabled by env var DISABLE_PUPPETEER");

  const proxyConfig = pickProxyConfig();
  const { page, key } = await browserPool.acquire(proxyConfig);
  const userAgent = pickUserAgent();
  const viewport = pickViewport();
  const navigatorOverrides = pickNavigatorOverrides();
  const waitSelectors = ensureArray(options.waitSelectors);
  const waitAfterLoadMs = Math.max(0, options.waitAfterLoadMs ?? DEFAULT_WAIT_AFTER_LOAD);
  const waitJitter = Math.round(waitAfterLoadMs * WAIT_JITTER_RATIO * (Math.random() * 2 - 1));
  const effectiveWaitAfterLoad = Math.max(0, waitAfterLoadMs + waitJitter);
  const networkPayloads = [];

  try {
    if (DEBUG) {
      page.on("console", (msg) =>
        console.log(`🖥️ [browser:${msg.type()}]`, msg.text())
      );
      page.on("pageerror", (err) => console.warn("⚠️ Page error:", err));
      page.on("error", (err) => console.warn("⚠️ Page crashed:", err));
    }

    page.on("response", async (response) => {
      try {
        const request = response.request();
        const resourceType = request.resourceType();
        if (!resourceType || !["xhr", "fetch", "document"].includes(resourceType)) {
          return;
        }
        const headers = response.headers();
        const contentType = headers["content-type"] || headers["Content-Type"];
        if (!contentType || !contentType.includes("application/json")) return;
        const text = await response.text();
        if (!text || text.length > 1_000_000) return;
        networkPayloads.push({ url: response.url(), body: text });
      } catch (err) {
        debugLog("Failed to read network response:", err.message);
      }
    });

    await page.setUserAgent(userAgent);
    await page.setExtraHTTPHeaders({
      "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
      "Upgrade-Insecure-Requests": "1",
    });
    await page.setViewport(viewport);
    await page.setDefaultNavigationTimeout(NAVIGATION_TIMEOUT);
    await page.setDefaultTimeout(NAVIGATION_TIMEOUT);

    await page.evaluateOnNewDocument((overrides) => {
      Object.defineProperty(navigator, "languages", {
        get: () => overrides.languages,
      });
      Object.defineProperty(navigator, "platform", {
        get: () => overrides.platform,
      });
      Object.defineProperty(navigator, "hardwareConcurrency", {
        get: () => overrides.hardwareConcurrency,
      });
      if (overrides.deviceMemory !== undefined) {
        Object.defineProperty(navigator, "deviceMemory", {
          get: () => overrides.deviceMemory,
        });
      }
      if (overrides.maxTouchPoints !== undefined) {
        Object.defineProperty(navigator, "maxTouchPoints", {
          get: () => overrides.maxTouchPoints,
        });
      }
      if (overrides.vendor) {
        Object.defineProperty(navigator, "vendor", {
          get: () => overrides.vendor,
        });
      }
      if (overrides.pluginsLength) {
        Object.defineProperty(navigator, "plugins", {
          get: () =>
            Array.from({ length: overrides.pluginsLength }).map((_, index) => ({
              name: `Plugin ${index + 1}`,
              filename: `plugin${index + 1}.dll`,
              description: `Fake plugin ${index + 1}`,
            })),
        });
      }
      Object.defineProperty(navigator, "webdriver", {
        get: () => false,
      });
      Object.defineProperty(navigator, "pdfViewerEnabled", {
        get: () => true,
      });
      window.chrome = window.chrome || { runtime: {} };
      try {
        const originalQuery = window.navigator.permissions?.query;
        if (originalQuery) {
          window.navigator.permissions.query = (parameters) => {
            if (parameters && parameters.name === "notifications") {
              const state =
                typeof Notification !== "undefined" && Notification.permission
                  ? Notification.permission
                  : "default";
              return Promise.resolve({ state });
            }
            return originalQuery(parameters);
          };
        }
      } catch (err) {
        void err;
      }
    }, navigatorOverrides);

    if (proxyConfig?.credentials) {
      await page.authenticate(proxyConfig.credentials);
    }

    await page.waitForTimeout(50 + Math.floor(Math.random() * 200));

    debugLog("🌐 Navigating with Puppeteer", {
      url,
      proxy: proxyConfig?.original,
      userAgent,
      viewport,
    });

    await page.goto(url, {
      waitUntil: ["domcontentloaded", "networkidle2"],
      timeout: NAVIGATION_TIMEOUT,
    });

    try {
      await page.waitForNetworkIdle({
        idleTime: 750,
        timeout: Math.min(NAVIGATION_TIMEOUT, 20000),
      });
    } catch (err) {
      debugLog("Network idle wait skipped:", err.message);
    }

    if (waitSelectors.length) {
      await waitForSelectors(page, waitSelectors, Math.min(NAVIGATION_TIMEOUT, 30000));
    }

    if (effectiveWaitAfterLoad) {
      await page.waitForTimeout(effectiveWaitAfterLoad);
    }

    const html = await page.content();
    const jsonLd = await page.$$eval(
      'script[type="application/ld+json"]',
      (scripts) => scripts.map((s) => s.innerText)
    );

    const nextDataPayload = await page
      .evaluate(() => {
        const nextScript = document.querySelector("#__NEXT_DATA__");
        if (nextScript?.textContent) {
          return nextScript.textContent;
        }
        if (typeof window !== "undefined" && window.__NEXT_DATA__) {
          try {
            return JSON.stringify(window.__NEXT_DATA__);
          } catch {
            return null;
          }
        }
        const nuxtScript = document.querySelector("#__NUXT__");
        if (nuxtScript?.textContent) {
          return nuxtScript.textContent;
        }
        if (typeof window !== "undefined" && window.__NUXT__) {
          try {
            return JSON.stringify(window.__NUXT__);
          } catch {
            return null;
          }
        }
        return null;
      })
      .catch(() => null);

    debugLog("✅ Puppeteer page loaded", {
      url,
      userAgent,
      waitAfter: effectiveWaitAfterLoad,
      proxy: proxyConfig?.original,
    });

    return {
      html,
      jsonLd,
      nextData: nextDataPayload,
      networkPayloads,
      meta: {
        userAgent,
        viewport,
        proxy: proxyConfig?.original,
        waitAfter: effectiveWaitAfterLoad,
      },
    };
  } catch (err) {
    console.error("❌ Puppeteer scraping error:", err);
    throw err;
  } finally {
    await browserPool.release(key, page);
  }
}

async function scrapeProduct(url, options) {
  const cacheEntry = cache.get(url);
  if (cacheEntry) {
    debugLog("Cache hit", { url });
    return { ...cacheEntry, cached: true };
  }

  let axiosExtraction = createEmptyProduct();
  let puppeteerExtraction = createEmptyProduct();
  let axiosError = null;
  let puppeteerError = null;
  let puppeteerMeta = null;
  let axiosMeta = null;
  let dynamicHint = false;

  const proxyForAxios = pickProxyConfig();

  try {
    const axiosResult = await pRetry(() => fetchWithAxios(url, proxyForAxios), {
      retries: MAX_RETRIES,
    });
    dynamicHint = axiosResult.isLikelyDynamic;
    axiosMeta = { userAgent: axiosResult.userAgent, proxy: proxyForAxios?.original };
    axiosExtraction = extractFromHtml(
      axiosResult.html,
      url,
      axiosResult.jsonLdScripts,
      axiosResult.nextDataPayload
    );
  } catch (err) {
    axiosError = err;
    console.warn("⚠️ Axios failed:", err.message);
  }

  const shouldUsePuppeteer =
    !DISABLE_PUPPETEER &&
    (dynamicHint || axiosError || !hasProductSignals(axiosExtraction));
  const attemptedPuppeteer = shouldUsePuppeteer;

  if (shouldUsePuppeteer) {
    try {
      const puppeteerResult = await pRetry(() => scrapeWithPuppeteer(url, options), {
        retries: MAX_RETRIES,
      });
      puppeteerMeta = puppeteerResult.meta;
      const htmlExtraction = extractFromHtml(
        puppeteerResult.html,
        url,
        puppeteerResult.jsonLd,
        puppeteerResult.nextData
      );
      const networkExtraction = extractFromNetworkPayloads(
        puppeteerResult.networkPayloads
      );
      puppeteerExtraction = mergeProductData(htmlExtraction, networkExtraction);
    } catch (err) {
      puppeteerError = err;
      console.warn("⚠️ Puppeteer failed:", err.message);
    }
  }

  let finalProduct = createEmptyProduct();
  let source = "axios";

  if (hasProductSignals(puppeteerExtraction)) {
    finalProduct = puppeteerExtraction;
    source = "puppeteer";
  } else if (hasProductSignals(axiosExtraction)) {
    finalProduct = axiosExtraction;
    source = "axios";
  } else {
    finalProduct = mergeProductData(axiosExtraction, puppeteerExtraction);
    if (!hasProductSignals(finalProduct)) {
      if (axiosError && attemptedPuppeteer && puppeteerError) {
        throw puppeteerError || axiosError;
      }
      if (axiosError && !attemptedPuppeteer) {
        throw axiosError;
      }
      if (!axiosError && attemptedPuppeteer && puppeteerError && !hasProductSignals(axiosExtraction)) {
        throw puppeteerError;
      }
    }
    source = attemptedPuppeteer && !puppeteerError ? "puppeteer" : "axios";
  }

  const payload = {
    ok: true,
    ...finalProduct,
    source,
    fetchedAt: Date.now(),
    meta: {
      axios: axiosMeta,
      puppeteer: puppeteerMeta,
      axiosError: axiosError ? axiosError.message : null,
      puppeteerError: puppeteerError ? puppeteerError.message : null,
    },
    cached: false,
  };

  cache.set(url, payload);
  return payload;
}

app.get("/", (req, res) => {
  res.json({ ok: true, status: "feednly-scraper", uptime: process.uptime() });
});

app.get("/health", (req, res) => {
  const browserHealth = browserPool.getHealth();
  const cacheStats = cache.getStats();
  res.json({
    ok: true,
    browser: browserHealth,
    cache: cacheStats,
    uptime: process.uptime(),
  });
});

app.get("/scrape", async (req, res) => {
  res.set("Cache-Control", "no-store");
  const url = req.query.url;
  if (!url) {
    return res
      .status(400)
      .json({ ok: false, error: "Missing URL query param (?url=https://...)" });
  }

  const waitForParam = req.query.waitFor;
  const waitSelectors = Array.isArray(waitForParam)
    ? waitForParam.flatMap((v) => `${v}`.split(","))
    : typeof waitForParam === "string"
    ? waitForParam.split(",")
    : [];
  const trimmedSelectors = waitSelectors
    .map((sel) => sel.trim())
    .filter((sel) => sel.length > 0);

  const waitAfterLoadMs = Math.max(
    0,
    Number.parseInt(req.query.waitAfterLoadMs ?? "", 10) || DEFAULT_WAIT_AFTER_LOAD
  );

  try {
    const result = await scrapeProduct(url, {
      waitSelectors: trimmedSelectors,
      waitAfterLoadMs,
    });
    res.json(result);
  } catch (err) {
    console.error("Scrape error:", err);
    res.status(500).json({ ok: false, error: err.message || "Scrape failed" });
  }
});

const server = app.listen(PORT, "0.0.0.0", () => {
  console.log(`✅ Feednly Scraper running on port ${PORT}`);
  console.log("ℹ️ Waiting for incoming requests...");
});

server.on("error", (err) => {
  console.error("❌ HTTP server failed to start:", err);
  process.exit(1);
});

async function gracefulShutdown() {
  console.log("⏬ Shutting down scraper...");
  try {
    await browserPool.shutdown();
  } catch (err) {
    console.warn("⚠️ Browser pool shutdown error:", err.message);
  }
  server.close(() => process.exit(0));
}

process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);

setTimeout(async () => {
  if (DISABLE_PUPPETEER) {
    console.log("⏭️ Puppeteer preload skipped (disabled via env).");
    return;
  }
  try {
    console.log("⏳ Preloading Puppeteer...");
    const proxyConfig = pickProxyConfig();
    const { page, key } = await browserPool.acquire(proxyConfig);
    await browserPool.release(key, page);
    console.log("✅ Puppeteer preloaded successfully!");
  } catch (err) {
    console.warn("⚠️ Puppeteer preload failed:", err.message);
  }
}, 1000);
