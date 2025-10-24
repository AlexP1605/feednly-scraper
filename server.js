import express from "express";
import * as cheerio from "cheerio";
import axios from "axios";
import NodeCache from "node-cache";
import { wrapper as applyAxiosCookieJarSupport } from "axios-cookiejar-support";
import { CookieJar, Cookie } from "tough-cookie";
import { HttpsProxyAgent } from "https-proxy-agent";
import { HttpProxyAgent } from "http-proxy-agent";
import { access } from "node:fs/promises";
import { constants as fsConstants } from "node:fs";

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

function logEvent(event, data = {}) {
  try {
    console.log(
      JSON.stringify({
        timestamp: new Date().toISOString(),
        event,
        ...data,
      })
    );
  } catch (err) {
    console.log(event, data);
  }
}

const axiosClient = applyAxiosCookieJarSupport(axios.create());
axiosClient.defaults.withCredentials = true;

const cookieJars = new Map();

function getCookieJarKey(url) {
  try {
    const parsed = new URL(url);
    return parsed.hostname;
  } catch {
    return "default";
  }
}

function getCookieJarForUrl(url) {
  const key = getCookieJarKey(url);
  if (!cookieJars.has(key)) {
    cookieJars.set(key, new CookieJar());
  }
  return cookieJars.get(key);
}

const portValue = process.env.PORT;
let PORT = Number.parseInt(`${portValue ?? ""}`.trim(), 10);
if (!Number.isFinite(PORT) || PORT <= 0) {
  console.warn(`Invalid PORT "${portValue}", fallback to 8080.`);
  PORT = 8080;
}

const MAX_RETRIES = 0;
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

const ALLOWED_RESOURCE_TYPES = new Set([
  "document",
  "xhr",
  "fetch",
  "script",
  "image",
]);

const BLOCKED_RESOURCE_TYPES = new Set([
  "font",
  "stylesheet",
  "media",
  "manifest",
  "eventsource",
  "beacon",
  "imageset",
  "other",
  "websocket",
]);

const BLOCKED_URL_KEYWORDS = [
  "analytics",
  "tracking",
  "track",
  "doubleclick",
  "googletagmanager",
  "tagmanager",
  "facebook",
  "pixel",
  "beacon",
  "gtm.js",
  "optimizely",
  "segment.io",
  "sentry",
  "appdynamics",
];

const BLOCKED_IMAGE_KEYWORDS = [
  "logo",
  "icon",
  "banner",
  "promo",
  "adservice",
  "analytics",
  "datadome",
  "kameleoon",
  "optimizely",
  "google",
  "doubleclick",
  "pixel",
  "tracker",
  "badge",
  "header",
  "footer",
];

const PRODUCT_IMAGE_KEYWORDS = [
  "product",
  "media",
  "item",
  "pdp",
  "zoom",
  "gallery",
  "img/",
  "/p/",
  "/photo/",
  "/images/",
];

const PRODUCT_IMAGE_JSON_KEYS = new Set([
  "image",
  "images",
  "media",
  "gallery",
  "thumbnail",
  "thumbnails",
  "src",
  "srcset",
  "url",
  "urls",
]);

function urlContainsKeyword(url, keywords) {
  if (!url) return false;
  const value = typeof url === "string" ? url : `${url}`;
  if (!value) return false;
  const lower = value.toLowerCase();
  return keywords.some((keyword) => lower.includes(keyword));
}

function isBlockedImageUrl(url) {
  return urlContainsKeyword(url, BLOCKED_IMAGE_KEYWORDS);
}

function isProductImageUrl(url) {
  if (!url) return false;
  if (isBlockedImageUrl(url)) return false;
  return urlContainsKeyword(url, PRODUCT_IMAGE_KEYWORDS);
}

function filterProductImageList(images) {
  const prepared = (images || [])
    .map((img) => (typeof img === "string" ? img.trim() : `${img}`.trim()))
    .filter((img) => img && isProductImageUrl(img));
  return dedupeImages(prepared);
}

const sleep = (ms) =>
  new Promise((resolve) => {
    setTimeout(resolve, Math.max(0, Number.isFinite(ms) ? ms : 0));
  });

function buildNavigatorLanguages(preferredLanguage) {
  const primary = `${preferredLanguage || "en-US"}`.trim() || "en-US";
  const normalizedPrimary = primary.replace("_", "-");
  const [baseLanguage] = normalizedPrimary.split("-");
  const fallbacks = [normalizedPrimary];
  if (baseLanguage && baseLanguage.length && baseLanguage.toLowerCase() !== normalizedPrimary.toLowerCase()) {
    fallbacks.push(baseLanguage.toLowerCase());
  }
  if (!fallbacks.includes("en-US")) {
    fallbacks.push("en-US");
  }
  if (!fallbacks.includes("en")) {
    fallbacks.push("en");
  }
  return fallbacks;
}

function buildAcceptLanguageHeader(preferredLanguage) {
  const languages = buildNavigatorLanguages(preferredLanguage);
  return languages
    .map((lang, index) => {
      if (index === 0) return lang;
      const weight = Math.max(0, 1 - index * 0.1);
      const clampedWeight = weight > 0 ? weight : 0.1;
      return `${lang};q=${clampedWeight.toFixed(1)}`;
    })
    .join(",");
}

const RAW_PROXY_POOL = (process.env.SCRAPER_PROXY_POOL || "")
  .split(/[\s,]+/)
  .map((entry) => entry.trim())
  .filter(Boolean);
const FALLBACK_PROXY =
  process.env.SCRAPER_PROXY_FALLBACK || process.env.SCRAPER_PROXY || null;
const PROXY_FAILURE_COOLDOWN_MS = Math.max(
  30_000,
  Number.parseInt(process.env.SCRAPER_PROXY_FAILURE_COOLDOWN_MS || "120000", 10) ||
    120_000
);
const PROXY_MAX_FAILURES = Math.max(
  1,
  Number.parseInt(process.env.SCRAPER_PROXY_MAX_FAILURES || "3", 10) || 3
);

const CACHE_TTL = Math.max(
  30,
  Number.parseInt(process.env.SCRAPER_CACHE_TTL || "180", 10) || 180
);
const cache = new NodeCache({ stdTTL: CACHE_TTL, checkperiod: 120, useClones: false });

const MAX_AXIOS_REDIRECTS = Math.max(
  0,
  Number.parseInt(process.env.SCRAPER_AXIOS_MAX_REDIRECTS || "10", 10) || 10
);

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

function pickNavigatorOverrides(preferredLanguage) {
  const languages = buildNavigatorLanguages(preferredLanguage);
  const platforms = ["Win32", "MacIntel", "Linux x86_64"];
  const hardwarePool = [4, 6, 8];
  const deviceMemoryPool = [4, 8, 16];
  const maxTouchPointsPool = [0, 1, 2];
  const vendorPool = ["Google Inc.", "Apple Computer, Inc.", "Mozilla Foundation"];
  const pluginsPool = [2, 3, 4, 5];
  return {
    languages,
    primaryLanguage: languages[0] || "en-US",
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

function normalizeHeaderArray(value) {
  if (!value) return [];
  return Array.isArray(value) ? value : [value];
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

function extractCountryCodeFromProxy(rawProxy, credentials) {
  const samples = [];
  if (credentials?.username) samples.push(credentials.username);
  if (rawProxy) samples.push(rawProxy);
  for (const sample of samples) {
    const match = /-cc-([a-z]{2})/i.exec(sample);
    if (match) {
      return match[1].toUpperCase();
    }
  }
  return null;
}

function normalizeProxyConfig(rawProxy, source = "pool") {
  if (!rawProxy) return null;
  let value = `${rawProxy}`.trim();
  if (!value) return null;
  if (!/^https?:\/\//i.test(value)) {
    value = `http://${value}`;
  }
  try {
    const parsed = new URL(value);
    const credentials =
      parsed.username || parsed.password
        ? {
            username: decodeURIComponent(parsed.username),
            password: decodeURIComponent(parsed.password),
          }
        : null;
    const launchArg = `${parsed.protocol}//${parsed.hostname}${
      parsed.port ? `:${parsed.port}` : ""
    }`;
    const proxyUrl = credentials
      ? `${parsed.protocol}//${encodeURIComponent(
          credentials.username
        )}:${encodeURIComponent(credentials.password)}@${parsed.hostname}${
          parsed.port ? `:${parsed.port}` : ""
        }`
      : `${parsed.protocol}//${parsed.hostname}${
          parsed.port ? `:${parsed.port}` : ""
        }`;
    const countryCode = extractCountryCodeFromProxy(rawProxy, credentials);
    return {
      key: `${parsed.protocol}//${parsed.hostname}:${parsed.port || ""}`.replace(
        /:+$/,
        ""
      ),
      launchArg,
      proxyUrl,
      credentials,
      original: rawProxy,
      protocol: parsed.protocol.replace(":", ""),
      hostname: parsed.hostname,
      port: parsed.port ? Number.parseInt(parsed.port, 10) : null,
      countryCode,
      source,
    };
  } catch (err) {
    console.warn("⚠️ Invalid proxy entry skipped:", rawProxy, err.message);
    return null;
  }
}

class ProxyManager {
  constructor(poolEntries, fallbackEntry) {
    this.pool = poolEntries
      .map((entry) => normalizeProxyConfig(entry, "pool"))
      .filter(Boolean);
    this.fallback = fallbackEntry
      ? [normalizeProxyConfig(fallbackEntry, "fallback")].filter(Boolean)
      : [];
    this.blacklist = new Map();
    this.stats = new Map();
  }

  _isBlacklisted(key) {
    const now = Date.now();
    const info = this.blacklist.get(key);
    if (!info) return false;
    if (info.until && info.until > now) return true;
    this.blacklist.delete(key);
    return false;
  }

  _touchStats(proxy) {
    if (!proxy) return;
    const stats = this.stats.get(proxy.key) || {
      successes: 0,
      failures: 0,
      lastFailure: null,
      lastSuccess: null,
      lastUsed: null,
    };
    stats.lastUsed = Date.now();
    this.stats.set(proxy.key, stats);
  }

  getProxy(options = {}) {
    const {
      requireResidential = false,
      excludeKeys = new Set(),
      allowFallback = true,
      countryCode = null,
    } = options;
    const normalizedCountry = countryCode ? `${countryCode}`.trim().toUpperCase() : null;

    const isUsable = (proxy) =>
      proxy && !excludeKeys.has(proxy.key) && !this._isBlacklisted(proxy.key);

    const pickFromList = (list) => {
      if (!list.length) return null;
      const choice = list[Math.floor(Math.random() * list.length)];
      this._touchStats(choice);
      return choice;
    };

    const filterByCountry = (collection) => {
      const usable = collection.filter(isUsable);
      if (!usable.length) return [];
      if (!normalizedCountry) return usable;
      const countryMatches = usable.filter(
        (proxy) => proxy.countryCode && proxy.countryCode === normalizedCountry
      );
      if (countryMatches.length) {
        return countryMatches;
      }
      return usable;
    };

    const availablePool = filterByCountry(this.pool);
    const poolChoice = pickFromList(availablePool);
    if (poolChoice) {
      return poolChoice;
    }

    if (!allowFallback && requireResidential) {
      return null;
    }

    const availableFallback = filterByCountry(this.fallback);
    if (availableFallback.length && (!requireResidential || allowFallback)) {
      return pickFromList(availableFallback);
    }
    return null;
  }

  hasResidentialProxy() {
    return this.pool.length > 0 || this.fallback.length > 0;
  }

  reportSuccess(proxy) {
    if (!proxy) return;
    const stats = this.stats.get(proxy.key) || {
      successes: 0,
      failures: 0,
      lastFailure: null,
      lastSuccess: null,
      lastUsed: null,
    };
    stats.successes += 1;
    stats.lastSuccess = Date.now();
    stats.failures = Math.max(0, stats.failures - 1);
    this.stats.set(proxy.key, stats);
    this.blacklist.delete(proxy.key);
  }

  reportFailure(proxy, reason) {
    if (!proxy) return;
    const stats = this.stats.get(proxy.key) || {
      successes: 0,
      failures: 0,
      lastFailure: null,
      lastSuccess: null,
      lastUsed: null,
    };
    stats.failures += 1;
    stats.lastFailure = Date.now();
    this.stats.set(proxy.key, stats);
    if (stats.failures >= PROXY_MAX_FAILURES || reason === "hard-fail") {
      this.blacklist.set(proxy.key, {
        until: Date.now() + PROXY_FAILURE_COOLDOWN_MS,
        reason,
      });
    }
  }

  getDiagnostics() {
    const now = Date.now();
    const entries = [...this.pool, ...this.fallback].map((proxy) => {
      const stats = this.stats.get(proxy.key) || {};
      const blacklistInfo = this.blacklist.get(proxy.key) || null;
      return {
        proxy: proxy.original,
        source: proxy.source,
        lastUsed: stats.lastUsed || null,
        successes: stats.successes || 0,
        failures: stats.failures || 0,
        blacklistedUntil:
          blacklistInfo && blacklistInfo.until > now ? blacklistInfo.until : null,
        lastFailureReason: blacklistInfo?.reason || null,
      };
    });
    return {
      total: entries.length,
      residentialPool: this.pool.length,
      fallbackPool: this.fallback.length,
      blacklisted: entries.filter((entry) => entry.blacklistedUntil).length,
      entries,
    };
  }
}

const proxyManager = new ProxyManager(RAW_PROXY_POOL, FALLBACK_PROXY);

const COUNTRY_LANGUAGE_MAP = {
  FR: "fr-FR",
  DE: "de-DE",
  ES: "es-ES",
  US: "en-US",
};

const KNOWN_DOMAIN_COUNTRY_MAP = new Map(
  Object.entries({
    "fnac.com": "FR",
    "darty.com": "FR",
    "amazon.com": "US",
    "walmart.com": "US",
    "target.com": "US",
    "otto.de": "DE",
    "mediamarkt.de": "DE",
  })
);

const PATH_COUNTRY_MAP = {
  "fr": "FR",
  "fr-fr": "FR",
  "de": "DE",
  "de-de": "DE",
  "es": "ES",
  "es-es": "ES",
  "us": "US",
  "en-us": "US",
};

const DEFAULT_LANGUAGE = "en-US";

function normalizeHostname(hostname) {
  const lower = `${hostname || ""}`.toLowerCase();
  return lower.startsWith("www.") ? lower.slice(4) : lower;
}

function detectCountryFromKnownDomain(hostname) {
  const normalized = normalizeHostname(hostname);
  for (const [domain, country] of KNOWN_DOMAIN_COUNTRY_MAP.entries()) {
    if (normalized === domain || normalized.endsWith(`.${domain}`)) {
      return country;
    }
  }
  return null;
}

function detectCountryFromPath(pathname) {
  if (!pathname) return null;
  const segments = pathname
    .split("/")
    .map((segment) => segment.trim().toLowerCase())
    .filter(Boolean);
  if (!segments.length) return null;
  const candidates = segments.slice(0, 2);
  for (const segment of candidates) {
    if (PATH_COUNTRY_MAP[segment]) {
      return PATH_COUNTRY_MAP[segment];
    }
  }
  return null;
}

function detectCountryFromTld(hostname) {
  const normalized = normalizeHostname(hostname);
  const parts = normalized.split(".");
  if (!parts.length) return null;
  const lastPart = parts[parts.length - 1];
  if (lastPart.length === 2) {
    const tldCountry = lastPart.toUpperCase();
    if (COUNTRY_LANGUAGE_MAP[tldCountry]) {
      return tldCountry;
    }
  }
  return null;
}

function determineCountryAndLanguage(url) {
  try {
    const parsed = new URL(url);
    const hostname = parsed.hostname;
    const pathname = parsed.pathname || "/";
    const countryFromDomain = detectCountryFromKnownDomain(hostname);
    const countryFromPath = detectCountryFromPath(pathname);
    const countryFromTld = detectCountryFromTld(hostname);
    const countryCode = countryFromDomain || countryFromPath || countryFromTld || null;
    const language = COUNTRY_LANGUAGE_MAP[countryCode] || DEFAULT_LANGUAGE;
    return { countryCode, language };
  } catch {
    return { countryCode: null, language: DEFAULT_LANGUAGE };
  }
}

function pickProxyAndLangForUrl(url) {
  const detection = determineCountryAndLanguage(url);
  let proxyConfig = null;
  if (proxyManager.hasResidentialProxy()) {
    if (detection.countryCode) {
      proxyConfig = proxyManager.getProxy({
        requireResidential: true,
        countryCode: detection.countryCode,
      });
    }
    if (!proxyConfig) {
      proxyConfig = proxyManager.getProxy({ requireResidential: true });
    }
    if (!proxyConfig) {
      proxyConfig = proxyManager.getProxy();
    }
  }

  const countryCode = detection.countryCode || null;
  const language = detection.language || COUNTRY_LANGUAGE_MAP[countryCode] || DEFAULT_LANGUAGE;

  const selection = {
    proxyUrl: proxyConfig?.proxyUrl || null,
    countryCode,
    language,
  };

  if (proxyConfig) {
    selection.proxyConfig = proxyConfig;
    selection.proxyCountryCode = proxyConfig.countryCode || null;
  }

  return selection;
}

function buildProxyAgents(proxyConfig) {
  if (!proxyConfig?.proxyUrl) return {};
  try {
    const proxyUrl = proxyConfig.proxyUrl;
    const agent = proxyUrl.startsWith("https://")
      ? new HttpsProxyAgent(proxyUrl)
      : new HttpProxyAgent(proxyUrl);
    return { httpAgent: agent, httpsAgent: agent };
  } catch (err) {
    console.warn("⚠️ Failed to create proxy agent:", err.message);
    return {};
  }
}

async function jarToPuppeteerCookies(jar, targetUrl) {
  if (!jar || !targetUrl) return [];
  try {
    const cookies = await jar.getCookies(targetUrl, {
      allPaths: true,
    });
    return cookies.map((cookie) => ({
      name: cookie.key,
      value: cookie.value,
      domain: cookie.domain || new URL(targetUrl).hostname,
      path: cookie.path || "/",
      expires:
        cookie.expires && cookie.expires !== "Infinity"
          ? Math.floor(cookie.expires.getTime() / 1000)
          : undefined,
      httpOnly: cookie.httpOnly,
      secure: cookie.secure,
      sameSite:
        cookie.sameSite === "lax"
          ? "Lax"
          : cookie.sameSite === "strict"
          ? "Strict"
          : cookie.sameSite === "none"
          ? "None"
          : undefined,
    }));
  } catch (err) {
    debugLog("jarToPuppeteerCookies failed:", err.message);
    return [];
  }
}

async function persistPuppeteerCookies(page, jar) {
  if (!page || !jar) return;
  try {
    const cookies = await page.cookies();
    let fallbackHost = null;
    try {
      fallbackHost = new URL(page.url()).host;
    } catch {
      fallbackHost = null;
    }
    for (const cookie of cookies) {
      const cookieString = `${cookie.name}=${cookie.value}`;
      const protocol = cookie.secure ? "https" : "http";
      const domain = cookie.domain?.startsWith(".")
        ? cookie.domain.slice(1)
        : cookie.domain;
      const cookieUrl = `${protocol}://${domain || fallbackHost || ""}${
        cookie.path || "/"
      }`;
      const toughCookie = new Cookie({
        key: cookie.name,
        value: cookie.value,
        domain: cookie.domain || domain,
        path: cookie.path || "/",
        secure: cookie.secure,
        httpOnly: cookie.httpOnly,
        expires:
          typeof cookie.expires === "number" && cookie.expires > 0
            ? new Date(cookie.expires * 1000)
            : "Infinity",
        sameSite: cookie.sameSite?.toLowerCase?.(),
      });
      await jar.setCookie(toughCookie, cookieUrl);
      debugLog("Cookie persisted", { cookie: cookieString, url: cookieUrl });
    }
  } catch (err) {
    debugLog("persistPuppeteerCookies failed:", err.message);
  }
}

const CHROMIUM_LAUNCH_BASE_ARGS = [
  "--no-sandbox",
  "--disable-setuid-sandbox",
  "--disable-dev-shm-usage",
  "--disable-accelerated-2d-canvas",
  "--disable-gpu",
  "--disable-software-rasterizer",
  "--disable-background-networking",
  "--disable-background-timer-throttling",
  "--disable-backgrounding-occluded-windows",
  "--disable-breakpad",
  "--disable-client-side-phishing-detection",
  "--disable-component-update",
  "--disable-default-apps",
  "--disable-domain-reliability",
  "--disable-hang-monitor",
  "--disable-ipc-flooding-protection",
  "--disable-popup-blocking",
  "--disable-prompt-on-repost",
  "--disable-renderer-backgrounding",
  "--disable-sync",
  "--no-first-run",
  "--no-default-browser-check",
  "--metrics-recording-only",
  "--force-color-profile=srgb",
  "--hide-scrollbars",
  "--mute-audio",
  "--no-zygote",
  "--single-process",
  "--ignore-certificate-errors",
  "--disable-extensions",
  "--disable-features=TranslateUI,BlinkGenPropertyTrees",
  "--disable-blink-features=AutomationControlled",
  "--password-store=basic",
  "--use-mock-keychain",
];

async function launchBrowser(proxyConfig, language = DEFAULT_LANGUAGE) {
  const puppeteer = await loadPuppeteer();
  const executablePath = await resolveChromiumExecutable();
  const normalizedLanguage = `${language || DEFAULT_LANGUAGE}`.replace("_", "-");
  const navigatorLanguages = buildNavigatorLanguages(normalizedLanguage);
  const primaryLanguage = navigatorLanguages[0] || "en-US";
  const baseLanguage = primaryLanguage.split("-")[0] || primaryLanguage;
  const langArgument =
    baseLanguage && baseLanguage.toLowerCase() !== primaryLanguage.toLowerCase()
      ? `${primaryLanguage},${baseLanguage}`
      : primaryLanguage;
  const args = [...CHROMIUM_LAUNCH_BASE_ARGS, `--lang=${langArgument}`];

  if (proxyConfig?.launchArg) {
    args.push(`--proxy-server=${proxyConfig.launchArg}`);
  }

  const launchOptions = {
    headless: "new",
    args,
    executablePath,
    ignoreHTTPSErrors: true,
    dumpio: false,
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

  _attachBrowserLifecycle(key, entry) {
    entry.browserPromise
      .then((browser) => {
        browser.once("disconnected", () => {
          console.warn("⚠️ Browser disconnected, recycling", { key });
          this.markUnhealthy(key).catch((err) =>
            console.warn("⚠️ Failed to recycle browser after disconnect:", err.message)
          );
        });
      })
      .catch((err) => {
        console.error("❌ Browser launch failed:", err.message);
        this.pool.delete(key);
      });
  }

  _createEntry(key, proxyConfig, language) {
    const entry = {
      browserPromise: launchBrowser(proxyConfig, language),
      activePages: 0,
      lastUsed: Date.now(),
      proxyConfig,
      language,
    };

    this._attachBrowserLifecycle(key, entry);

    this.pool.set(key, entry);
    return entry;
  }

  async acquire(proxyConfig, language = DEFAULT_LANGUAGE, attempt = 0) {
    if (attempt > 3) {
      throw new Error("Unable to acquire Puppeteer page after multiple attempts");
    }

    const normalizedLanguage = `${language || DEFAULT_LANGUAGE}`.replace("_", "-");
    const key = `${proxyConfig?.key || "default"}|${normalizedLanguage}`;
    let entry = this.pool.get(key) || this._createEntry(key, proxyConfig, normalizedLanguage);

    try {
      const browser = await entry.browserPromise;
      if (!browser?.isConnected?.()) {
        throw new Error("Browser disconnected");
      }

      const page = await browser.newPage();
      page.once("close", () => {
        const latestEntry = this.pool.get(key);
        if (latestEntry) {
          latestEntry.activePages = Math.max(0, latestEntry.activePages - 1);
          latestEntry.lastUsed = Date.now();
        }
      });
      page.on("error", (err) => {
        console.warn("⚠️ Puppeteer page error, recycling browser:", err.message);
        this.markUnhealthy(key).catch(() => {});
      });

      entry.activePages += 1;
      entry.lastUsed = Date.now();
      return { page, browser, key, entry };
    } catch (err) {
      console.warn("⚠️ Failed to acquire page, restarting browser:", err.message);
      await this.markUnhealthy(key).catch(() => {});
      return this.acquire(proxyConfig, language, attempt + 1);
    }
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
      entry.lastUsed = Date.now();
    }
  }

  async markUnhealthy(key) {
    const entry = this.pool.get(key);
    if (!entry) return;
    if (entry.restarting) return entry.restarting;

    entry.restarting = (async () => {
      try {
        const browser = await entry.browserPromise.catch(() => null);
        if (browser) {
          try {
            await browser.close();
          } catch (closeErr) {
            debugLog("Browser close error during restart:", closeErr.message);
          }
        }
      } finally {
        entry.browserPromise = launchBrowser(entry.proxyConfig, entry.language);
        this._attachBrowserLifecycle(key, entry);
        entry.lastUsed = Date.now();
        entry.restarting = null;
      }
      return entry.browserPromise;
    })();

    return entry.restarting;
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
        language: entry.language || DEFAULT_LANGUAGE,
        restarting: Boolean(entry.restarting),
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
    node.media,
    node.gallery,
    node.assets,
    node.photos,
    node.imageUrl,
    node.photo,
    node.thumbnailUrl,
    node.thumbnail,
    node.contentUrl,
    node.src,
    node.srcset,
    node.url,
  ];
  const urls = [];
  for (const field of imageFields) {
    for (const value of ensureArray(field)) {
      if (typeof value === "string" && value.trim()) {
        const trimmed = value.trim();
        if (isProductImageUrl(trimmed)) {
          urls.push(trimmed);
        }
      } else if (value && typeof value === "object") {
        const candidates = [value.url, value.contentUrl, value.imageUrl];
        for (const candidate of candidates) {
          if (typeof candidate === "string" && candidate.trim()) {
            const normalized = candidate.trim();
            if (isProductImageUrl(normalized)) {
              urls.push(normalized);
            }
          }
        }
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
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  if (typeof raw !== "string") return null;
  try {
    return JSON.parse(raw);
  } catch (err) {
    debugLog("Unable to parse state payload:", err.message);
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
      if (normalized && isProductImageUrl(normalized)) images.push(normalized);
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

function extractInlineWindowState(html, variableName) {
  if (!html || !variableName) return null;
  const pattern = new RegExp(
    `window\\.${variableName.replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&")}\\s*=\\s*(\\{.*?\\})<\\/`,
    "s"
  );
  const match = html.match(pattern);
  if (match) return match[1];
  return null;
}

function extractFromHtml(
  html,
  url,
  jsonLdScripts = null,
  nextDataPayload = null,
  additionalStatePayloads = []
) {
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

  const inlinePreloadedState = extractInlineWindowState(html, "__PRELOADED_STATE__");
  const inlineInitialProps = extractInlineWindowState(html, "__INITIAL_PROPS__");

  const stateExtractions = [];
  const payloadsToProcess = [
    inlinePreloadedState,
    inlineInitialProps,
    ...ensureArray(additionalStatePayloads),
  ].filter(Boolean);
  for (const payload of payloadsToProcess) {
    const parsedState = parseNextData(payload);
    if (parsedState) {
      stateExtractions.push(extractFromNextData(parsedState));
    }
  }
  const combinedStateExtraction = mergeProductData(...stateExtractions);

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
      if (normalized && isProductImageUrl(normalized)) images.push(normalized);
    }
    for (const srcset of srcsetAttributes) {
      for (const candidate of parseSrcSet(srcset)) {
        const normalized = normalizeUrl(candidate, url);
        if (normalized && isProductImageUrl(normalized)) images.push(normalized);
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
      if (normalized && isProductImageUrl(normalized)) images.push(normalized);
    }
  });

  images.push(
    ...ensureArray(jsonLdExtraction.images)
      .map((img) => normalizeUrl(img, url))
      .filter((img) => img && isProductImageUrl(img))
  );
  images.push(
    ...ensureArray(nextDataExtraction.images)
      .map((img) => normalizeUrl(img, url))
      .filter((img) => img && isProductImageUrl(img))
  );
  images.push(
    ...ensureArray(combinedStateExtraction.images)
      .map((img) => normalizeUrl(img, url))
      .filter((img) => img && isProductImageUrl(img))
  );

  const cleanedImages = filterProductImageList(images.filter(Boolean)).slice(0, 150);
  const variants = dedupeVariants([
    ...ensureArray(jsonLdExtraction.variants),
    ...ensureArray(nextDataExtraction.variants),
    ...ensureArray(combinedStateExtraction.variants),
  ]);

  const merged = mergeProductData(jsonLdExtraction, nextDataExtraction, combinedStateExtraction);
  if (!merged.title && title) merged.title = title;
  if (!merged.description && description) merged.description = description;
  if (!merged.price && price) merged.price = price;
  merged.images = filterProductImageList([...(merged.images || []), ...cleanedImages]).slice(
    0,
    150
  );
  merged.variants = dedupeVariants([...(merged.variants || []), ...variants]);

  return merged;
}

function safeJsonParse(body) {
  if (typeof body !== "string") return null;
  try {
    return JSON.parse(body);
  } catch {
    return null;
  }
}

function jsonContainsProductImageSignals(value, depth = 0, visited = new Set()) {
  if (!value || typeof value !== "object" || visited.has(value) || depth > 50) {
    return false;
  }
  visited.add(value);
  if (!Array.isArray(value)) {
    for (const key of Object.keys(value)) {
      if (PRODUCT_IMAGE_JSON_KEYS.has(key.toLowerCase())) {
        return true;
      }
    }
  }
  const entries = Array.isArray(value) ? value : Object.values(value);
  for (const entry of entries) {
    if (jsonContainsProductImageSignals(entry, depth + 1, visited)) {
      return true;
    }
  }
  return false;
}

function hasEssentialProductData(product) {
  if (!product) return false;
  const hasTitle = Boolean(product.title && product.title.trim());
  const hasDescription = Boolean(product.description && product.description.trim());
  const hasPrice = Boolean(product.price && `${product.price}`.trim());
  const hasImages = Array.isArray(product.images) && product.images.length > 0;
  return hasTitle && hasDescription && hasPrice && hasImages;
}

function extractFromNetworkPayloads(payloads) {
  if (!payloads?.length) return createEmptyProduct();
  let merged = createEmptyProduct();
  for (const payload of payloads) {
    let extraction = null;
    if (payload?.product) {
      extraction = payload.product;
    } else {
      const parsed = payload?.parsed || safeJsonParse(payload?.body);
      if (!parsed) continue;
      extraction = extractFromNextData(parsed);
    }
    if (!extraction) continue;
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
  result.images = filterProductImageList(result.images).slice(0, 150);
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

const IMPORTANT_HEADER_KEYS = [
  "set-cookie",
  "server",
  "x-datadome",
  "x-perimeterx",
  "cf-ray",
  "cf-chl",
  "cf-cache-status",
  "x-request-id",
  "x-amzn-requestid",
  "location",
];

function extractImportantHeaders(headers = {}) {
  const result = {};
  for (const [key, value] of Object.entries(headers)) {
    const lower = key.toLowerCase();
    if (
      IMPORTANT_HEADER_KEYS.some((needle) =>
        lower.startsWith(needle.toLowerCase())
      )
    ) {
      result[lower] = value;
    }
  }
  if (headers["content-type"]) {
    result["content-type"] = headers["content-type"];
  } else if (headers["Content-Type"]) {
    result["content-type"] = headers["Content-Type"];
  }
  return result;
}

function analyzeAntiBotSignals({ html, pageUrl, documentResponses = [] }) {
  const reasons = [];
  const lowerHtml = typeof html === "string" ? html.toLowerCase() : "";
  if (lowerHtml.includes("datadome")) reasons.push("datadome");
  if (lowerHtml.includes("perimeterx")) reasons.push("perimeterx");
  if (lowerHtml.includes("cloudflare")) reasons.push("cloudflare");
  if (lowerHtml.includes("verify you are human")) reasons.push("human-check");
  if (lowerHtml.includes("captcha")) reasons.push("captcha");
  if (/<title>\s*fnac\.com\s*<\/title>/i.test(html || ""))
    reasons.push("fnac-homepage");
  if (/<title>\s*zara\.com\s*<\/title>/i.test(html || ""))
    reasons.push("zara-homepage");
  if (pageUrl && /challenge|captcha|blocked|verify/i.test(pageUrl)) {
    reasons.push("redirect-challenge");
  }
  for (const response of documentResponses) {
    if (!response) continue;
    if ([403, 429, 503].includes(response.status)) {
      reasons.push(`http-${response.status}`);
    }
    const headers = response.headers || {};
    for (const key of Object.keys(headers)) {
      if (key.toLowerCase().includes("datadome")) {
        reasons.push("header-datadome");
      }
      if (key.toLowerCase().includes("perimeterx")) {
        reasons.push("header-perimeterx");
      }
      if (key.toLowerCase().includes("cf-ray")) {
        reasons.push("header-cloudflare");
      }
    }
  }
  return { detected: reasons.length > 0, reasons: Array.from(new Set(reasons)) };
}

async function fetchWithAxios(url, options = {}) {
  const userAgent = options.userAgent || pickUserAgent();
  const proxyConfig = options.proxyConfig || null;
  const jar = options.jar || getCookieJarForUrl(url);
  const secChHeaders = buildSecChUaHeaders(userAgent);
  const navigationHeaders = buildNavigationHeaders(url);
  const acceptLanguage = options.acceptLanguage || buildAcceptLanguageHeader(options.language);
  const redirectEntries = [];
  const visitedUrls = [url];
  const requestConfig = {
    headers: {
      "User-Agent": userAgent,
      ...secChHeaders,
      ...navigationHeaders,
      "Accept-Encoding": "gzip, deflate, br",
      "Accept-Language": acceptLanguage,
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
    maxRedirects: MAX_AXIOS_REDIRECTS,
    decompress: true,
    responseType: "text",
    validateStatus: (status) => status >= 200 && status < 400,
    jar,
    withCredentials: true,
  };

  requestConfig.beforeRedirect = (_options, responseDetails) => {
    const fromUrl = responseDetails?.responseUrl || null;
    const statusCode = responseDetails?.statusCode;
    const locationHeader = normalizeHeaderArray(responseDetails?.headers?.location);
    const setCookieHeader = normalizeHeaderArray(
      responseDetails?.headers?.["set-cookie"]
    );

    if (fromUrl) {
      visitedUrls.push(fromUrl);
    }
    for (const location of locationHeader) {
      if (location) {
        visitedUrls.push(location);
      }
    }

    redirectEntries.push({
      fromUrl,
      statusCode,
      location: locationHeader,
      setCookie: setCookieHeader,
    });
  };

  if (proxyConfig) {
    const agents = buildProxyAgents(proxyConfig);
    requestConfig.httpAgent = agents.httpAgent;
    requestConfig.httpsAgent = agents.httpsAgent;
    requestConfig.proxy = false;
  }

  let res;
  try {
    res = await axiosClient.get(url, requestConfig);
    if (proxyConfig) {
      proxyManager.reportSuccess(proxyConfig);
    }
  } catch (err) {
    const statusCode = err.response?.status;
    if (proxyConfig && (statusCode === 403 || statusCode === 429)) {
      proxyManager.reportFailure(proxyConfig, `status-${statusCode}`);
    } else if (proxyConfig && err.code) {
      proxyManager.reportFailure(proxyConfig, err.code);
    }
    throw err;
  }
  const redirectCount = res.request?._redirectable?._redirectCount ?? 0;
  const finalUrl =
    res.request?.res?.responseUrl ||
    res.request?.responseURL ||
    res.request?._redirectable?._currentUrl ||
    url;
  const finalSetCookie = normalizeHeaderArray(res.headers?.["set-cookie"]);
  const finalLocation = normalizeHeaderArray(res.headers?.location);
  const uniqueVisitedUrls = Array.from(
    new Set([...visitedUrls, finalUrl].filter(Boolean))
  );

  debugLog("Axios redirect diagnostics", {
    requestedUrl: url,
    finalUrl,
    redirectCount,
    visitedUrls: uniqueVisitedUrls,
    hops: redirectEntries,
    finalHeaders: {
      setCookie: finalSetCookie,
      location: finalLocation,
    },
    proxy: proxyConfig?.original,
  });

  logEvent("axios_complete", {
    url,
    finalUrl,
    status: res.status,
    redirectCount,
    userAgent,
    proxy: proxyConfig?.original || null,
  });
  const html = res.data;
  const jsonLdScripts = extractJsonLdScriptsFromHtml(html);
  const nextDataPayload = extractInlineNextData(html);
  const preloadedStatePayload = extractInlineWindowState(html, "__PRELOADED_STATE__");
  const initialPropsPayload = extractInlineWindowState(html, "__INITIAL_PROPS__");
  const windowStates = [preloadedStatePayload, initialPropsPayload].filter(Boolean);
  const dynamic = detectDynamicPage(html);
  const result = {
    html,
    jsonLdScripts,
    nextDataPayload,
    windowStates,
    userAgent,
    isLikelyDynamic: dynamic,
    redirectCount,
    visitedUrls: uniqueVisitedUrls,
  };

  if (finalUrl && finalUrl !== url) {
    result.finalUrl = finalUrl;
  }

  return result;
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

const CONSENT_TEXT_MATCHERS = [
  "accept",
  "agree",
  "accepter",
  "j'accepte",
  "tout accepter",
  "accept all",
  "allow all",
];

const CONSENT_SELECTORS = [
  "#onetrust-accept-btn-handler",
  "button#onetrust-accept-btn-handler",
  "button#truste-consent-button",
  "button[data-testid='uc-accept-all']",
  "button[data-testid='cookie-accept-all']",
  "button[aria-label*='accept']",
  "button[aria-label*='Accepter']",
  "button[aria-label*='autoriser']",
  "button[class*='accept']",
  "button[class*='Agree']",
  "button[mode='primary']",
  "button[aria-haspopup='dialog'][data-testid*='accept']",
];

async function handleConsentInterstitial(page) {
  for (const selector of CONSENT_SELECTORS) {
    try {
      const element = await page.$(selector);
      if (element) {
        await element.click({ delay: 25 });
        await sleep(500);
        return { clicked: true, selector };
      }
    } catch (err) {
      debugLog("Consent click failed", { selector, error: err.message });
    }
  }

  const textMatch = await page
    .evaluate((matchers) => {
      const lowerMatchers = matchers.map((m) => m.toLowerCase());
      const candidates = Array.from(
        document.querySelectorAll(
          "button, [role='button'], input[type='button'], input[type='submit']"
        )
      );
      for (const candidate of candidates) {
        const text = (candidate.innerText || candidate.value || "").trim();
        if (!text) continue;
        const lowerText = text.toLowerCase();
        if (lowerMatchers.some((matcher) => lowerText.includes(matcher))) {
          candidate.click();
          return text;
        }
      }
      return null;
    }, CONSENT_TEXT_MATCHERS)
    .catch(() => null);

  if (textMatch) {
    await sleep(750);
    return { clicked: true, selector: `text:${textMatch}` };
  }

  return { clicked: false, selector: null };
}

async function scrapeWithPuppeteer(url, options = {}) {
  if (DISABLE_PUPPETEER)
    throw new Error("Puppeteer disabled by env var DISABLE_PUPPETEER");

  const proxyConfig = options.proxyConfig || null;
  const jar = options.jar || getCookieJarForUrl(url);
  const userAgent = options.userAgent || pickUserAgent();
  const viewport = options.viewport || pickViewport();
  const preferredLanguage = options.preferredLanguage || "en-US";
  const acceptLanguage = options.acceptLanguage || buildAcceptLanguageHeader(preferredLanguage);
  const navigatorOverrides =
    options.navigatorOverrides || pickNavigatorOverrides(preferredLanguage);
  if (!navigatorOverrides.languages) {
    navigatorOverrides.languages = buildNavigatorLanguages(preferredLanguage);
  }
  if (!navigatorOverrides.primaryLanguage) {
    navigatorOverrides.primaryLanguage = navigatorOverrides.languages[0] || preferredLanguage;
  }
  const acquisitionLanguage = navigatorOverrides.primaryLanguage || preferredLanguage;
  const waitSelectors = ensureArray(options.waitSelectors);
  const waitAfterLoadMs = Math.max(0, options.waitAfterLoadMs ?? DEFAULT_WAIT_AFTER_LOAD);
  const waitJitter = Math.round(
    waitAfterLoadMs * WAIT_JITTER_RATIO * (Math.random() * 2 - 1)
  );
  const effectiveWaitAfterLoad = Math.max(0, waitAfterLoadMs + waitJitter);
  const dumpNetwork = options.dumpNetwork === true;

  const puppeteerModule = await loadPuppeteer();
  const TimeoutError = puppeteerModule?.errors?.TimeoutError || null;

  const { page, key } = await browserPool.acquire(proxyConfig, acquisitionLanguage);
  const networkPayloads = [];
  const networkDebug = [];
  const documentResponses = [];
  const navigationChain = [];
  const secChHeaders = buildSecChUaHeaders(userAgent);
  let consent = { clicked: false, selector: null };
  const requestStats = {
    interceptedRequests: 0,
    allowedRequests: 0,
    blockedRequests: 0,
    transferredBytes: 0,
  };
  let documentBytesRecorded = false;
  let networkProductSnapshot = createEmptyProduct();
  let earlyCompletionTriggered = false;

  try {
    if (DEBUG) {
      page.on("console", (msg) =>
        console.log(`🖥️ [browser:${msg.type()}]`, msg.text())
      );
      page.on("pageerror", (err) => console.warn("⚠️ Page error:", err));
      page.on("error", (err) => console.warn("⚠️ Page crashed:", err));
    }

    page.on("framenavigated", (frame) => {
      if (frame === page.mainFrame()) {
        navigationChain.push({ url: frame.url(), ts: Date.now() });
      }
    });

    page.on("request", (request) => {
      requestStats.interceptedRequests += 1;
      try {
        const resourceType = request.resourceType();
        const requestUrl = request.url();
        const lowerUrl = requestUrl.toLowerCase();
        let allow = ALLOWED_RESOURCE_TYPES.has(resourceType) &&
          !BLOCKED_RESOURCE_TYPES.has(resourceType);

        if (allow && BLOCKED_URL_KEYWORDS.some((keyword) => lowerUrl.includes(keyword))) {
          allow = false;
        }

        if (allow && resourceType === "image") {
          const allowedImage = isProductImageUrl(requestUrl);
          const blockedImage = urlContainsKeyword(lowerUrl, BLOCKED_IMAGE_KEYWORDS);
          allow = allowedImage && !blockedImage;
        }

        if (earlyCompletionTriggered && resourceType !== "document") {
          allow = false;
        }

        if (allow) {
          requestStats.allowedRequests += 1;
          request.continue();
        } else {
          requestStats.blockedRequests += 1;
          request.abort();
        }
      } catch (err) {
        debugLog("Request interception fallback:", err.message);
        try {
          request.abort();
        } catch {}
      }
    });

    page.on("response", async (response) => {
      try {
        const request = response.request();
        const resourceType = request.resourceType();
        const headers = response.headers();
        const normalizedHeaders = extractImportantHeaders(headers);
        const lengthHeader = headers["content-length"] || headers["Content-Length"];
        const parsedLength = lengthHeader ? Number.parseInt(lengthHeader, 10) : NaN;
        if (Number.isFinite(parsedLength)) {
          requestStats.transferredBytes += parsedLength;
        }
        if (resourceType === "document") {
          documentResponses.push({
            url: response.url(),
            status: response.status(),
            headers: normalizedHeaders,
          });
          if (Number.isFinite(parsedLength)) {
            documentBytesRecorded = true;
          }
          return;
        }
        if (!resourceType || !["xhr", "fetch"].includes(resourceType)) {
          return;
        }
        const contentType = (
          headers["content-type"] || headers["Content-Type"] || ""
        ).toLowerCase();
        if (!contentType.includes("json") && !contentType.includes("javascript")) {
          if (dumpNetwork) {
            const text = await response.text().catch(() => "");
            networkDebug.push({
              url: response.url(),
              status: response.status(),
              headers: normalizedHeaders,
              bodySnippet: text ? text.slice(0, 1000) : "",
            });
          }
          return;
        }
        const text = await response.text();
        if (!text || text.length > 1_000_000) return;
        const parsed = safeJsonParse(text);
        if (!parsed || !jsonContainsProductImageSignals(parsed)) {
          return;
        }
        if (!Number.isFinite(parsedLength)) {
          requestStats.transferredBytes += Buffer.byteLength(text, "utf8");
        }
        const extraction = extractFromNextData(parsed);
        if (extraction && hasProductSignals(extraction)) {
          networkProductSnapshot = mergeProductData(networkProductSnapshot, extraction);
          if (!earlyCompletionTriggered && hasEssentialProductData(networkProductSnapshot)) {
            earlyCompletionTriggered = true;
            page.evaluate(() => {
              try {
                if (typeof window !== "undefined" && window.stop) {
                  window.stop();
                }
              } catch {}
            }).catch(() => {});
          }
        }
        networkPayloads.push({
          url: response.url(),
          status: response.status(),
          product: extraction,
          body: dumpNetwork ? text : undefined,
        });
        if (dumpNetwork) {
          networkDebug.push({
            url: response.url(),
            status: response.status(),
            headers: normalizedHeaders,
            bodySnippet: text.slice(0, 2000),
          });
        }
      } catch (err) {
        debugLog("Failed to read network response:", err.message);
      }
    });

    await page.setUserAgent(userAgent);
    await page.setExtraHTTPHeaders({
      ...secChHeaders,
      "Accept-Language": acceptLanguage,
      "Upgrade-Insecure-Requests": "1",
      "Sec-Fetch-Dest": "document",
      "Sec-Fetch-Mode": "navigate",
      "Sec-Fetch-Site": "none",
      "Sec-Fetch-User": "?1",
      DNT: "1",
    });
    await page.setViewport(viewport);
    await page.setJavaScriptEnabled(true);
    await page.setDefaultNavigationTimeout(NAVIGATION_TIMEOUT);
    await page.setDefaultTimeout(NAVIGATION_TIMEOUT);
    await page.setRequestInterception(true);

    await page.evaluateOnNewDocument(
      ({ overrides, ua }) => {
        try {
          Object.defineProperty(navigator, "languages", {
            get: () => overrides.languages,
          });
          Object.defineProperty(navigator, "language", {
            get: () => overrides.primaryLanguage || overrides.languages?.[0] || "en-US",
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
          Object.defineProperty(navigator, "webdriver", {
            get: () => false,
          });
          Object.defineProperty(navigator, "pdfViewerEnabled", {
            get: () => true,
          });
          Object.defineProperty(navigator, "userAgent", {
            get: () => ua,
          });
          if (!window.chrome) {
            Object.defineProperty(window, "chrome", {
              value: { runtime: {} },
            });
          }
          if (!navigator.plugins || navigator.plugins.length === 0) {
            const length = overrides.pluginsLength || 3;
            Object.defineProperty(navigator, "plugins", {
              get: () =>
                Array.from({ length }).map((_, index) => ({
                  name: `Plugin ${index + 1}`,
                  filename: `plugin${index + 1}.so`,
                  description: `Fake plugin ${index + 1}`,
                })),
            });
          }
          const originalPermissions = navigator.permissions?.query;
          if (originalPermissions) {
            navigator.permissions.query = (parameters) => {
              if (parameters && parameters.name === "notifications") {
                const state =
                  typeof Notification !== "undefined" && Notification.permission
                    ? Notification.permission
                    : "default";
                return Promise.resolve({ state });
              }
              return originalPermissions(parameters);
            };
          }
          if (!navigator.userAgentData) {
            Object.defineProperty(navigator, "userAgentData", {
              get: () => ({
                brands: [
                  { brand: "Chromium", version: "123" },
                  { brand: "Google Chrome", version: "123" },
                  { brand: "Not=A?Brand", version: "8" },
                ],
                mobile: false,
                getHighEntropyValues: async () => ({
                  platform: overrides.platform,
                  platformVersion: "15.0.0",
                  architecture: "x86",
                  model: "",
                  uaFullVersion: (ua.match(/Chrome\/([\d.]+)/) || [])[1] || "123.0.0.0",
                }),
              }),
            });
          }
          const patchCanvas = () => {
            const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
            HTMLCanvasElement.prototype.toDataURL = function (...args) {
              const context = this.getContext("2d");
              if (context) {
                const shift = 0.000001;
                context.fillStyle = `rgba(0,0,0,${shift})`;
                context.fillRect(0, 0, 1, 1);
              }
              return originalToDataURL.apply(this, args);
            };
            const originalGetImageData = CanvasRenderingContext2D.prototype.getImageData;
            CanvasRenderingContext2D.prototype.getImageData = function (...args) {
              const data = originalGetImageData.apply(this, args);
              return data;
            };
          };
          const patchWebGL = () => {
            const proto = WebGLRenderingContext?.prototype;
            if (!proto) return;
            const originalGetParameter = proto.getParameter;
            proto.getParameter = function (parameter) {
              if (parameter === 37445) return "Intel Inc.";
              if (parameter === 37446) return "Intel Iris OpenGL";
              return originalGetParameter.call(this, parameter);
            };
          };
          const patchAudio = () => {
            const AudioCtx = window.AudioContext || window.webkitAudioContext;
            if (!AudioCtx) return;
            const audioProto = AudioCtx.prototype;
            const originalCreateAnalyser = audioProto.createAnalyser;
            audioProto.createAnalyser = function (...args) {
              const analyser = originalCreateAnalyser.apply(this, args);
              const originalGetFloatFrequencyData = analyser.getFloatFrequencyData;
              analyser.getFloatFrequencyData = function (array) {
                const result = originalGetFloatFrequencyData.call(this, array);
                for (let i = 0; i < array.length; i += 100) {
                  array[i] = array[i] + Math.random() * 0.0001;
                }
                return result;
              };
              return analyser;
            };
          };
          patchCanvas();
          patchWebGL();
          patchAudio();
        } catch (err) {
          console.debug("init script error", err);
        }
      },
      { overrides: navigatorOverrides, ua: userAgent }
    );

    if (proxyConfig?.credentials) {
      await page.authenticate(proxyConfig.credentials);
    }

    const preloadCookies = await jarToPuppeteerCookies(jar, url);
    if (preloadCookies.length) {
      try {
        await page.setCookie(...preloadCookies);
      } catch (err) {
        debugLog("Failed to set initial cookies:", err.message);
      }
    }

    await sleep(50 + Math.floor(Math.random() * 200));

    logEvent("puppeteer_navigate", {
      url,
      proxy: proxyConfig?.original || null,
      userAgent,
      viewport,
    });

    try {
      await page.goto(url, {
        waitUntil: ["domcontentloaded", "networkidle2"],
        timeout: NAVIGATION_TIMEOUT,
      });
    } catch (err) {
      const isTimeoutError =
        (TimeoutError && err instanceof TimeoutError) ||
        err?.name === "TimeoutError" ||
        /timeout/i.test(err?.message || "");

      if (!isTimeoutError) {
        throw err;
      }

      debugLog(
        "Navigation timed out waiting for network idle, retrying with domcontentloaded",
        err.message
      );
      logEvent("puppeteer_navigate_retry", {
        url,
        proxy: proxyConfig?.original || null,
        userAgent,
        viewport,
        reason: err.message,
      });

      await page.goto(url, {
        waitUntil: "domcontentloaded",
        timeout: NAVIGATION_TIMEOUT,
      });
    }

    if (!earlyCompletionTriggered) {
      try {
        await page.waitForNetworkIdle({
          idleTime: 750,
          timeout: Math.min(NAVIGATION_TIMEOUT, 20000),
        });
      } catch (err) {
        debugLog("Network idle wait skipped:", err.message);
      }
    }

    if (!earlyCompletionTriggered && waitSelectors.length) {
      await waitForSelectors(
        page,
        waitSelectors,
        Math.min(NAVIGATION_TIMEOUT, 30000)
      );
    }

    if (!earlyCompletionTriggered) {
      consent = await handleConsentInterstitial(page);
    }
    if (!earlyCompletionTriggered && consent.clicked) {
      try {
        await page.waitForNetworkIdle({
          idleTime: 750,
          timeout: Math.min(NAVIGATION_TIMEOUT, 15000),
        });
      } catch (err) {
        debugLog("Consent reload wait skipped:", err.message);
      }
    }

    if (!earlyCompletionTriggered && effectiveWaitAfterLoad) {
      await sleep(effectiveWaitAfterLoad);
    }

    const pageUrl = page.url();
    const html = await page.content();
    if (!documentBytesRecorded) {
      requestStats.transferredBytes += Buffer.byteLength(html || "", "utf8");
    }
    const jsonLd = await page.$$eval(
      'script[type="application/ld+json"]',
      (scripts) => scripts.map((s) => s.innerText)
    );

    const windowPayloads = await page
      .evaluate(() => {
        const safeSerialize = (value) => {
          if (!value) return null;
          try {
            return JSON.stringify(value);
          } catch (err) {
            return null;
          }
        };
        const payload = {};
        const nextScript = document.querySelector("#__NEXT_DATA__");
        if (nextScript?.textContent) {
          payload.nextData = nextScript.textContent;
        } else if (typeof window !== "undefined" && window.__NEXT_DATA__) {
          payload.nextData = safeSerialize(window.__NEXT_DATA__);
        }
        const nuxtScript = document.querySelector("#__NUXT__");
        if (nuxtScript?.textContent) {
          payload.nextData = payload.nextData || nuxtScript.textContent;
        } else if (typeof window !== "undefined" && window.__NUXT__) {
          payload.nextData = payload.nextData || safeSerialize(window.__NUXT__);
        }
        const preloadedScript = document.querySelector("#__PRELOADED_STATE__");
        if (preloadedScript?.textContent) {
          payload.preloadedState = preloadedScript.textContent;
        } else if (typeof window !== "undefined" && window.__PRELOADED_STATE__) {
          payload.preloadedState = safeSerialize(window.__PRELOADED_STATE__);
        }
        const initialPropsScript = document.querySelector("#__INITIAL_PROPS__");
        if (initialPropsScript?.textContent) {
          payload.initialProps = initialPropsScript.textContent;
        } else if (typeof window !== "undefined" && window.__INITIAL_PROPS__) {
          payload.initialProps = safeSerialize(window.__INITIAL_PROPS__);
        }
        return payload;
      })
      .catch(() => ({}));

    const nextDataPayload = windowPayloads?.nextData || null;
    const additionalWindowStates = [
      windowPayloads?.preloadedState || null,
      windowPayloads?.initialProps || null,
    ].filter(Boolean);

    const antiBotAnalysis = analyzeAntiBotSignals({
      html,
      pageUrl,
      documentResponses,
    });

    logEvent("puppeteer_complete", {
      url,
      pageUrl,
      proxy: proxyConfig?.original || null,
      userAgent,
      antiBotDetected: antiBotAnalysis.detected,
      antiBotReasons: antiBotAnalysis.reasons,
      networkPayloads: networkPayloads.length,
    });

    return {
      html,
      jsonLd,
      nextData: nextDataPayload,
      windowStates: additionalWindowStates,
      networkPayloads,
      networkDebug: dumpNetwork ? networkDebug : undefined,
      documentResponses,
      navigationChain,
      consent,
      requestStats,
      antiBotDetected: antiBotAnalysis.detected,
      antiBotReasons: antiBotAnalysis.reasons,
      meta: {
        userAgent,
        viewport,
        proxy: proxyConfig?.original || null,
        waitAfter: effectiveWaitAfterLoad,
        pageUrl,
        antiBotDetected: antiBotAnalysis.detected,
        antiBotReasons: antiBotAnalysis.reasons,
        consent,
        documentResponses,
        navigationChain,
      },
    };
  } catch (err) {
    console.error("❌ Puppeteer scraping error:", err);
    throw err;
  } finally {
    try {
      page.removeAllListeners("request");
      page.removeAllListeners("response");
      page.removeAllListeners("requestfailed");
      page.removeAllListeners("requestfinished");
      await page.setRequestInterception(false).catch(() => {});
    } catch {}
    try {
      await persistPuppeteerCookies(page, jar);
    } catch (err) {
      debugLog("Cookie persist failed:", err.message);
    }
    await browserPool.release(key, page);
  }
}

async function scrapeProduct(url, options = {}) {
  const scrapeStartedAt = Date.now();
  const dumpNetwork = options.dumpNetwork === true;
  if (!dumpNetwork) {
    const cacheEntry = cache.get(url);
    if (cacheEntry) {
      debugLog("Cache hit", { url });
      return cacheEntry;
    }
  }

  const jar = getCookieJarForUrl(url);
  const localeSelection = pickProxyAndLangForUrl(url);
  const selectedProxyConfig = localeSelection.proxyConfig || null;
  const sessionUserAgent = pickUserAgent();
  const sessionViewport = pickViewport();
  const sessionNavigator = pickNavigatorOverrides(localeSelection.language);
  const acceptLanguageHeader = buildAcceptLanguageHeader(localeSelection.language);

  const countryLabel = (localeSelection.countryCode || "RANDOM")
    .toString()
    .toUpperCase();
  console.log(`🌍 Using proxy ${countryLabel} | Lang ${localeSelection.language} | URL ${url}`);

  let axiosExtraction = createEmptyProduct();
  let axiosError = null;
  let axiosAntiBot = { detected: false, reasons: [] };
  let axiosHasStructuredData = false;
  let axiosBytes = 0;
  let axiosSuccess = false;
  let dynamicHint = false;

  try {
    const axiosResult = await fetchWithAxios(url, {
      proxyConfig: selectedProxyConfig,
      userAgent: sessionUserAgent,
      jar,
      acceptLanguage: acceptLanguageHeader,
      language: localeSelection.language,
    });
    axiosBytes = Buffer.byteLength(axiosResult.html || "", "utf8");
    axiosExtraction = extractFromHtml(
      axiosResult.html,
      url,
      axiosResult.jsonLdScripts,
      axiosResult.nextDataPayload,
      axiosResult.windowStates
    );
    axiosAntiBot = analyzeAntiBotSignals({
      html: axiosResult.html,
      pageUrl: axiosResult.finalUrl,
    });
    axiosHasStructuredData = Boolean(
      (axiosResult.jsonLdScripts && axiosResult.jsonLdScripts.length > 0) ||
        axiosResult.nextDataPayload
    );
    dynamicHint = Boolean(axiosResult.isLikelyDynamic);
    axiosSuccess = true;
  } catch (err) {
    axiosError = err;
    logEvent("axios_error", {
      url,
      error: err.message,
      proxy: selectedProxyConfig?.original || null,
    });
  }

  const shouldUsePuppeteer =
    !DISABLE_PUPPETEER &&
    !axiosHasStructuredData &&
    (axiosError || !hasProductSignals(axiosExtraction) || axiosAntiBot.detected || dynamicHint);

  let puppeteerExtraction = createEmptyProduct();
  let puppeteerStats = null;
  let puppeteerError = null;

  if (shouldUsePuppeteer) {
    try {
      const puppeteerResult = await scrapeWithPuppeteer(url, {
        ...options,
        waitSelectors: options.waitSelectors,
        waitAfterLoadMs: options.waitAfterLoadMs,
        proxyConfig: selectedProxyConfig,
        userAgent: sessionUserAgent,
        viewport: sessionViewport,
        navigatorOverrides: sessionNavigator,
        preferredLanguage: localeSelection.language,
        acceptLanguage: acceptLanguageHeader,
        jar,
        dumpNetwork,
      });
      puppeteerStats = puppeteerResult.requestStats || null;
      const htmlExtraction = extractFromHtml(
        puppeteerResult.html,
        url,
        puppeteerResult.jsonLd,
        puppeteerResult.nextData,
        puppeteerResult.windowStates
      );
      const networkExtraction = extractFromNetworkPayloads(
        puppeteerResult.networkPayloads
      );
      puppeteerExtraction = mergeProductData(htmlExtraction, networkExtraction);
    } catch (err) {
      puppeteerError = err;
      logEvent("puppeteer_error", {
        url,
        error: err.message,
        proxy: selectedProxyConfig?.original || null,
      });
    }
  }

  const finalProduct = mergeProductData(axiosExtraction, puppeteerExtraction);

  if (!hasProductSignals(finalProduct)) {
    if (axiosError && puppeteerError) {
      throw puppeteerError || axiosError;
    }
    if (axiosError && !shouldUsePuppeteer) {
      throw axiosError;
    }
    if (puppeteerError) {
      throw puppeteerError;
    }
  }

  const payload = {
    ok: true,
    title: finalProduct.title || null,
    description: finalProduct.description || null,
    price: finalProduct.price || null,
    images: finalProduct.images || [],
  };

  const interceptedRequests =
    (puppeteerStats?.interceptedRequests || 0) + (axiosSuccess ? 1 : 0);
  const totalBytes = (puppeteerStats?.transferredBytes || 0) + axiosBytes;
  const proxyCountry = selectedProxyConfig?.countryCode
    ? String(selectedProxyConfig.countryCode).toUpperCase()
    : "";
  const proxyLabel = selectedProxyConfig
    ? `${proxyCountry} Proxy`.trim() || "Proxy"
    : "Direct";
  const durationSeconds = (Date.now() - scrapeStartedAt) / 1000;
  const transferMB = Number((totalBytes / (1024 * 1024)).toFixed(3));
  payload.meta = {
    ...(payload.meta || {}),
    network: {
      requests: interceptedRequests,
      transferMB,
      durationSeconds,
      proxy: proxyLabel,
    },
  };

  if (!dumpNetwork) {
    cache.set(url, payload);
  }

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
    proxies: proxyManager.getDiagnostics(),
    cookies: { stores: cookieJars.size },
    uptime: process.uptime(),
  });
});

app.get("/debug", (req, res) => {
  res.json({
    ok: true,
    browser: browserPool.getHealth(),
    cache: cache.getStats(),
    proxies: proxyManager.getDiagnostics(),
    cookies: { stores: cookieJars.size },
    config: {
      disablePuppeteer: DISABLE_PUPPETEER,
      maxRetries: MAX_RETRIES,
      navigationTimeout: NAVIGATION_TIMEOUT,
      proxyPoolSize: RAW_PROXY_POOL.length,
    },
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

  const dumpNetwork =
    req.query.dumpNetwork === "1" || req.query.dumpNetwork === "true";

  try {
    const result = await scrapeProduct(url, {
      waitSelectors: trimmedSelectors,
      waitAfterLoadMs,
      dumpNetwork,
    });
    return res.json(result);
  } catch (err) {
    console.error("Scrape error:", err);
    return res
      .status(500)
      .json({ ok: false, error: err.message || "Scrape failed" });
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
    const preloadProxy = proxyManager.getProxy();
    const { page, key } = await browserPool.acquire(preloadProxy, DEFAULT_LANGUAGE);
    await browserPool.release(key, page);
    console.log("✅ Puppeteer preloaded successfully!");
  } catch (err) {
    console.warn("⚠️ Puppeteer preload failed:", err.message);
  }
}, 1000);
