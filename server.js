import express from "express";
import * as cheerio from "cheerio";
import puppeteer from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";

const app = express();
app.set("etag", false);

const DEBUG = process.env.DEBUG === "true";

function debugLog(...args) {
  if (DEBUG) {
    console.log("🪲", ...args);
  }
}

puppeteer.use(StealthPlugin());

const MAX_ATTEMPTS = 3;
const NAVIGATION_TIMEOUT = Math.max(
  1000,
  Number.parseInt(process.env.SCRAPER_NAVIGATION_TIMEOUT_MS || "45000", 10) ||
    45000
);

const HUMAN_DELAY_RANGE = [2000, 3200];

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
  "other",
  "eventsource",
  "websocket",
  "manifest",
]);

const BLOCKED_URL_PATTERNS = [
  "analytics",
  "doubleclick",
  "tracking",
  "facebook",
  "pixel",
  "segment",
  "hotjar",
  "optimizely",
  "datadome",
  "bam.nr-data.net",
  "google-analytics",
  "googletagmanager",
  "sentry",
  "logrocket",
  "akamai",
];

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

const ANTI_BOT_PATTERNS = [
  /bm-verify=/i,
  /_sec\/verify/i,
  /cf-chl-bypass/i,
  /datadome/i,
  /akam-logo/i,
  /interstitial\/ic\.html/i,
  /captcha/i,
  /please enable javascript/i,
];

const STRATEGY_WEIGHTS = {
  structuredData: 0.4,
  openGraph: 0.2,
  windowGlobals: 0.2,
  network: 0.1,
  regex: 0.1,
  dom: 0.1,
};

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
];

const VIEWPORT_WIDTHS = [1280, 1366, 1440, 1536, 1680, 1920];
const VIEWPORT_HEIGHTS = [720, 768, 900, 960, 1080];

const JSON_SEARCH_KEYS = new Set([
  "image",
  "images",
  "media",
  "gallery",
  "photos",
  "picture",
  "price",
  "amount",
  "name",
  "title",
  "description",
]);

const IMAGE_REGEX = /https?:\/\/[^"]+\.(?:jpe?g|png|webp)/gi;

const PROXY_POOL = (process.env.SCRAPER_PROXY_POOL || process.env.SCRAPER_PROXIES || "")
  .split(/[\s,]+/)
  .map((entry) => entry.trim())
  .filter(Boolean)
  .map(parseProxyEntry)
  .filter(Boolean);

function randomItem(list) {
  return list[Math.floor(Math.random() * list.length)];
}

function randomBetween(min, max) {
  return min + Math.random() * (max - min);
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

function buildNavigatorOverrides() {
  const platforms = ["Win32", "MacIntel", "Linux x86_64"];
  const hardwarePool = [4, 6, 8];
  const deviceMemoryPool = [4, 8, 16];
  const maxTouchPointsPool = [0, 1, 2];
  return {
    platform: randomItem(platforms),
    hardwareConcurrency: randomItem(hardwarePool),
    deviceMemory: randomItem(deviceMemoryPool),
    maxTouchPoints: randomItem(maxTouchPointsPool),
  };
}

function parseProxyEntry(raw) {
  try {
    const parsed = new URL(raw);
    return {
      raw,
      serverArg: `${parsed.protocol}//${parsed.host}`,
      label: `${parsed.protocol}//${parsed.hostname}${parsed.port ? `:${parsed.port}` : ""}`,
      username: parsed.username ? decodeURIComponent(parsed.username) : null,
      password: parsed.password ? decodeURIComponent(parsed.password) : null,
    };
  } catch (err) {
    debugLog("Invalid proxy entry skipped", raw, err.message);
    return null;
  }
}

async function performHumanLikeDelays(page) {
  try {
    const viewport = page.viewport() || { width: 1280, height: 720 };
    const startX = Math.floor(Math.random() * Math.max(1, viewport.width - 200));
    const startY = Math.floor(Math.random() * Math.max(1, viewport.height - 200));
    await page.mouse.move(startX, startY, { steps: 5 });
    await page.waitForTimeout(120 + Math.floor(Math.random() * 180));
    await page.mouse.move(startX + Math.floor(Math.random() * 120), startY + 60, {
      steps: 6,
    });
    await page.waitForTimeout(80 + Math.floor(Math.random() * 160));
    await page.evaluate(() => {
      try {
        const distance = 200 + Math.floor(Math.random() * 200);
        window.scrollBy({ top: distance, behavior: "smooth" });
      } catch {
        window.scrollBy(0, 200);
      }
    });
    await page.waitForTimeout(140 + Math.floor(Math.random() * 220));
  } catch (err) {
    debugLog("Human-like delays failed", err.message);
  }
}

function normalizeUrl(value, baseUrl) {
  if (!value) return null;
  let result = `${value}`.trim();
  if (!result) return null;
  if (result.startsWith("//")) {
    return `https:${result}`;
  }
  if (/^https?:/i.test(result)) {
    return result;
  }
  if (result.startsWith("data:")) return null;
  try {
    return new URL(result, baseUrl).toString();
  } catch {
    return result;
  }
}

function isProductImageUrl(url) {
  if (!url) return false;
  const lower = url.toLowerCase();
  if (!/\.(jpg|jpeg|png|webp)(?:$|\?)/.test(lower)) return false;
  if (PLACEHOLDER_KEYWORDS.some((keyword) => lower.includes(keyword))) return false;
  return PRODUCT_IMAGE_KEYWORDS.some((keyword) => lower.includes(keyword));
}

function dedupe(values) {
  const seen = new Set();
  const result = [];
  for (const value of values || []) {
    if (!value) continue;
    const key = value.trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    result.push(key);
  }
  return result;
}

function detectBotProtection(html) {
  if (!html) return { detected: false, matches: [] };
  const matches = ANTI_BOT_PATTERNS.filter((pattern) => pattern.test(html)).map(
    (pattern) => pattern.source
  );
  return { detected: matches.length > 0, matches };
}

function extractStructuredData($, html, url) {
  const scripts = $("script[type='application/ld+json']")
    .map((_, el) => $(el).text())
    .get();
  const nodes = [];
  for (const script of scripts) {
    if (!script) continue;
    try {
      const parsed = JSON.parse(script);
      flattenJsonLd(parsed, nodes);
    } catch (err) {
      debugLog("Invalid JSON-LD skipped", err.message);
    }
  }

  const microdataNodes = extractMicrodata($, url);
  nodes.push(...microdataNodes);

  const result = collectProductFields(nodes, url);
  return { ...result, weight: STRATEGY_WEIGHTS.structuredData, source: "structuredData" };
}

function flattenJsonLd(node, result, seen = new Set()) {
  if (!node || typeof node !== "object" || seen.has(node)) return;
  seen.add(node);
  if (!Array.isArray(node)) {
    result.push(node);
  }
  const values = Array.isArray(node) ? node : Object.values(node);
  for (const value of values) {
    if (value && typeof value === "object") {
      flattenJsonLd(value, result, seen);
    }
  }
}

function extractMicrodata($, url) {
  const nodes = [];
  $("[itemtype]").each((_, el) => {
    const type = ($(el).attr("itemtype") || "").toLowerCase();
    if (!type.includes("product")) return;
    const node = {};
    $(el)
      .find("[itemprop]")
      .each((__, child) => {
        const prop = ($(child).attr("itemprop") || "").trim();
        if (!prop) return;
        const text = $(child).attr("content") || $(child).text();
        if (!text) return;
        node[prop] = text.trim();
      });
    if (Object.keys(node).length) {
      nodes.push(node);
    }
  });
  return nodes;
}

function collectProductFields(nodes, baseUrl) {
  const images = [];
  let title = null;
  let description = null;
  let price = null;

  for (const node of nodes || []) {
    if (!node || typeof node !== "object") continue;
    if (!title && typeof node.name === "string" && node.name.trim()) {
      title = node.name.trim();
    }
    if (!description && typeof node.description === "string" && node.description.trim()) {
      description = node.description.trim();
    }
    if (!price) {
      const candidate = derivePrice(node);
      if (candidate) price = candidate;
    }
    const imageFields = [
      node.image,
      node.images,
      node.media,
      node.photo,
      node.thumbnail,
      node.imageUrl,
      node.url,
    ];
    for (const field of imageFields) {
      if (!field) continue;
      if (Array.isArray(field)) {
        for (const value of field) {
          const normalized = normalizeUrl(value, baseUrl);
          if (normalized && isProductImageUrl(normalized)) images.push(normalized);
        }
      } else if (typeof field === "string") {
        const normalized = normalizeUrl(field, baseUrl);
        if (normalized && isProductImageUrl(normalized)) images.push(normalized);
      } else if (typeof field === "object") {
        const candidates = [field.url, field.contentUrl, field.image];
        for (const candidate of candidates) {
          const normalized = normalizeUrl(candidate, baseUrl);
          if (normalized && isProductImageUrl(normalized)) images.push(normalized);
        }
      }
    }
  }

  return { title, description, price, images };
}

function derivePrice(node) {
  const candidates = [
    node.price,
    node.priceValue,
    node.priceAmount,
    node.priceSpecification?.price,
    node.offers?.price,
    node.offers?.priceSpecification?.price,
    node.offers?.priceCurrency ? node.offers?.priceSpecification?.price : null,
    node.lowPrice,
    node.highPrice,
    node.amount,
  ];
  for (const candidate of candidates) {
    if (candidate === undefined || candidate === null) continue;
    const value = `${candidate}`.trim();
    if (value) return value;
  }
  return null;
}

function extractOpenGraph($, url) {
  const title =
    $("meta[property='og:title']").attr("content") ||
    $("meta[name='twitter:title']").attr("content") ||
    $("title").first().text() ||
    null;
  const description =
    $("meta[property='og:description']").attr("content") ||
    $("meta[name='description']").attr("content") ||
    $("meta[name='twitter:description']").attr("content") ||
    null;
  const price =
    $("meta[property='product:price:amount']").attr("content") ||
    $("meta[name='product:price:amount']").attr("content") ||
    $("meta[itemprop='price']").attr("content") ||
    null;
  const imageSelectors = [
    "meta[property='og:image']",
    "meta[property='og:image:url']",
    "meta[name='twitter:image']",
    "meta[name='twitter:image:src']",
    "link[rel='image_src']",
  ];
  const images = [];
  for (const selector of imageSelectors) {
    $(selector)
      .toArray()
      .forEach((element) => {
        const $el = $(element);
        const candidate = $el.attr("content") || $el.attr("href");
        const normalized = normalizeUrl(candidate, url);
        if (normalized && isProductImageUrl(normalized)) {
          images.push(normalized);
        }
      });
  }
  return { title, description, price, images, weight: STRATEGY_WEIGHTS.openGraph, source: "openGraph" };
}

function extractWindowGlobals(windowPayloads, url) {
  const nodes = [];
  for (const payload of Object.values(windowPayloads || {})) {
    if (!payload) continue;
    try {
      const parsed = typeof payload === "string" ? JSON.parse(payload) : payload;
      flattenJsonLd(parsed, nodes);
    } catch (err) {
      debugLog("Failed to parse window payload", err.message);
    }
  }
  const result = collectProductFields(nodes, url);
  return { ...result, weight: STRATEGY_WEIGHTS.windowGlobals, source: "windowGlobals" };
}

function extractNetworkPayloads(payloads, url) {
  const nodes = [];
  for (const payload of payloads || []) {
    const { parsed, text } = payload;
    if (parsed) {
      flattenJsonLd(parsed, nodes);
    } else if (text) {
      try {
        const parsedText = JSON.parse(text);
        flattenJsonLd(parsedText, nodes);
      } catch (err) {
        debugLog("Network payload parse failed", err.message);
      }
    }
  }
  const result = collectProductFields(nodes, url);
  return { ...result, weight: STRATEGY_WEIGHTS.network, source: "network" };
}

function extractRegexImages(html, url) {
  if (!html) return { images: [], weight: STRATEGY_WEIGHTS.regex, source: "regex" };
  const matches = html.match(IMAGE_REGEX) || [];
  const images = matches
    .map((match) => normalizeUrl(match, url))
    .filter((candidate) => candidate && isProductImageUrl(candidate));
  return { images, weight: STRATEGY_WEIGHTS.regex, source: "regex" };
}

function extractDomHeuristics($, url) {
  let title = null;
  let description = null;
  let price = null;
  const images = [];

  const h1s = $("h1")
    .map((_, el) => $(el).text().trim())
    .get()
    .filter(Boolean);
  if (h1s.length) {
    title = h1s.reduce((longest, current) => (current.length > longest.length ? current : longest));
  }
  if (!title) {
    title = $("title").first().text().trim() || null;
  }

  const descriptionCandidates = [];
  $("meta[name='description']").each((_, el) => {
    const content = $(el).attr("content");
    if (content) descriptionCandidates.push(content.trim());
  });
  $(".product, [class*='product'], [class*='description'], [class*='detail']")
    .find("p")
    .each((_, el) => {
      const text = $(el).text().replace(/\s+/g, " ").trim();
      if (text.length > 40) descriptionCandidates.push(text);
    });
  if (descriptionCandidates.length) {
    description = descriptionCandidates.reduce((longest, current) =>
      current.length > longest.length ? current : longest
    );
  }

  const priceSelectors = [
    "[class*='price']",
    "[id*='price']",
    "span[data-price]",
    "div[data-price]",
    "span[itemprop='price']",
  ];
  const priceRegex = /(\$|€|£|¥|₹|₩|₽)\s?\d+[\d.,]*/;
  for (const selector of priceSelectors) {
    const el = $(selector).first();
    if (!el.length) continue;
    const text = el.attr("content") || el.attr("data-price") || el.text();
    if (text && priceRegex.test(text)) {
      price = text.trim();
      break;
    }
  }
  if (!price) {
    const match = ($("body").text() || "").match(priceRegex);
    if (match) price = match[0];
  }

  $(".product img, .gallery img, [class*='image'] img, picture source").each((_, el) => {
    const $el = $(el);
    const sources = [
      $el.attr("src"),
      $el.attr("data-src"),
      $el.attr("data-original"),
      $el.attr("data-image"),
      $el.attr("data-large-image"),
    ];
    for (const src of sources) {
      const normalized = normalizeUrl(src, url);
      if (normalized && isProductImageUrl(normalized)) {
        images.push(normalized);
      }
    }
    const srcset = $el.attr("srcset") || $el.attr("data-srcset");
    if (srcset) {
      srcset.split(",").forEach((entry) => {
        const candidate = entry.trim().split(" ")[0];
        const normalized = normalizeUrl(candidate, url);
        if (normalized && isProductImageUrl(normalized)) {
          images.push(normalized);
        }
      });
    }
  });

  return { title, description, price, images, weight: STRATEGY_WEIGHTS.dom, source: "dom" };
}

function mergeStrategyResults(results) {
  const merged = { title: null, description: null, price: null, images: [] };
  const weightsUsed = new Set();
  const fieldWeights = { title: 0, description: 0, price: 0, images: 0 };
  const imageWeightMap = new Map();

  for (const result of results) {
    if (!result) continue;
    const { weight = 0, title, description, price, images } = result;
    if (title && weight > fieldWeights.title) {
      merged.title = title;
      fieldWeights.title = weight;
    }
    if (description && weight > fieldWeights.description) {
      merged.description = description;
      fieldWeights.description = weight;
    }
    if (price && weight > fieldWeights.price) {
      merged.price = price;
      fieldWeights.price = weight;
    }
    if (Array.isArray(images) && images.length) {
      for (const image of images) {
        if (!image) continue;
        const currentWeight = imageWeightMap.get(image) || 0;
        if (weight > currentWeight) {
          imageWeightMap.set(image, weight);
        }
      }
    }
    if (weight > 0) weightsUsed.add(weight);
  }

  const imagesWithWeights = Array.from(imageWeightMap.entries())
    .map(([url, weight]) => ({ url, weight, resolution: estimateImageResolution(url) }))
    .sort((a, b) => {
      if (b.weight !== a.weight) return b.weight - a.weight;
      return b.resolution - a.resolution;
    })
    .map((entry) => entry.url);

  merged.images = imagesWithWeights.slice(0, 30);
  const confidence = Math.min(
    1,
    [...weightsUsed].reduce((sum, current) => sum + current, 0)
  );

  return { product: merged, confidence };
}

function estimateImageResolution(url) {
  if (!url) return 0;
  const widthMatch = url.match(/(?:w|width|wid|size|s)=([0-9]{2,4})/i);
  if (widthMatch) return Number.parseInt(widthMatch[1], 10);
  const dimensionMatch = url.match(/([0-9]{3,4})x([0-9]{3,4})/);
  if (dimensionMatch) return Number.parseInt(dimensionMatch[1], 10);
  return 0;
}

async function createBrowser(proxy) {
  const launchArgs = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-features=TranslateUI,BlinkGenPropertyTrees",
    "--disable-blink-features=AutomationControlled",
  ];
  if (proxy?.serverArg) {
    launchArgs.push(`--proxy-server=${proxy.serverArg}`);
  }
  const browser = await puppeteer.launch({
    headless: "new",
    args: launchArgs,
    ignoreHTTPSErrors: true,
  });
  return browser;
}

async function gatherWindowPayloads(page) {
  return page
    .evaluate(() => {
      const result = {};
      const push = (key, value) => {
        if (!value) return;
        try {
          result[key] = JSON.stringify(value);
        } catch (err) {
          try {
            result[key] = JSON.stringify(value, null, 0);
          } catch {
            result[key] = null;
          }
        }
      };
      const directCandidates = {
        __NEXT_DATA__: window.__NEXT_DATA__,
        __NUXT__: window.__NUXT__,
        __INITIAL_STATE__: window.__INITIAL_STATE__,
        __PRELOADED_STATE__: window.__PRELOADED_STATE__,
        __INITIAL_PROPS__: window.__INITIAL_PROPS__,
      };
      for (const [key, value] of Object.entries(directCandidates)) {
        if (value) push(key, value);
      }
      const dynamicKeys = Object.keys(window).filter((key) => {
        const lower = key.toLowerCase();
        if (directCandidates[key] !== undefined) return false;
        return (
          lower.includes("state") ||
          lower.includes("data") ||
          lower.includes("product") ||
          lower.includes("store") ||
          lower.startsWith("__")
        );
      });
      for (const key of dynamicKeys) {
        const value = window[key];
        if (!value || typeof value === "function") continue;
        push(key, value);
      }
      return result;
    })
    .catch(() => ({}));
}

async function scrapeAttempt(url, attempt, proxy) {
  const browser = await createBrowser(proxy);
  const page = await browser.newPage();
  const userAgent = pickUserAgent();
  const viewport = pickViewport();
  const navigatorOverrides = buildNavigatorOverrides();
  const networkStats = { requests: 0, bytes: 0, start: performance.now() };
  const networkPayloads = [];
  let interstitialDetected = false;
  let blockedReasons = [];

  try {
    await page.setUserAgent(userAgent);
    await page.setViewport(viewport);
    await page.setJavaScriptEnabled(true);

    await page.setRequestInterception(true);
    page.on("request", (request) => {
      try {
        const resourceType = request.resourceType();
        const url = request.url();
        if (BLOCKED_RESOURCE_TYPES.has(resourceType)) {
          return request.abort();
        }
        if (!ALLOWED_RESOURCE_TYPES.has(resourceType)) {
          return request.abort();
        }
        if (BLOCKED_URL_PATTERNS.some((keyword) => url.toLowerCase().includes(keyword))) {
          return request.abort();
        }
        networkStats.requests += 1;
        return request.continue();
      } catch (err) {
        debugLog("Request interception error", err.message);
        try {
          request.continue();
        } catch {}
      }
    });

    page.on("response", async (response) => {
      try {
        const headers = response.headers();
        const contentLength = headers["content-length"]
          ? Number.parseInt(headers["content-length"], 10)
          : 0;
        if (Number.isFinite(contentLength) && contentLength > 0) {
          networkStats.bytes += contentLength;
        }
        const url = response.url();
        const contentType = headers["content-type"] || "";
        const shouldParse =
          /application\/json/i.test(contentType) ||
          /product|media|item|api|detail/i.test(url);
        if (shouldParse) {
          const text = await response.text();
          if (text) {
            try {
              const parsed = JSON.parse(text);
              if (jsonContainsProductSignals(parsed)) {
                networkPayloads.push({ url, parsed });
              }
            } catch {
              networkPayloads.push({ url, text });
            }
          }
        }
      } catch (err) {
        debugLog("Response handling failed", err.message);
      }
    });

    await page.evaluateOnNewDocument(({ overrides, ua }) => {
      Object.defineProperty(navigator, "webdriver", { get: () => false });
      Object.defineProperty(navigator, "platform", { get: () => overrides.platform });
      Object.defineProperty(navigator, "hardwareConcurrency", {
        get: () => overrides.hardwareConcurrency,
      });
      Object.defineProperty(navigator, "deviceMemory", {
        get: () => overrides.deviceMemory,
      });
      Object.defineProperty(navigator, "maxTouchPoints", {
        get: () => overrides.maxTouchPoints,
      });
      Object.defineProperty(navigator, "userAgent", { get: () => ua });
      if (!navigator.plugins || navigator.plugins.length === 0) {
        Object.defineProperty(navigator, "plugins", {
          get: () =>
            [1, 2, 3].map((index) => ({
              name: `Plugin ${index}`,
              filename: `plugin${index}.so`,
              description: `Fake plugin ${index}`,
            })),
        });
      }
      if (!window.chrome) {
        Object.defineProperty(window, "chrome", { value: { runtime: {} } });
      }
    }, { overrides: navigatorOverrides, ua: userAgent });

    if (proxy?.username && proxy?.password) {
      await page.authenticate({ username: proxy.username, password: proxy.password });
    }

    await page.goto(url, { waitUntil: "domcontentloaded", timeout: NAVIGATION_TIMEOUT });
    await delay(randomBetween(...HUMAN_DELAY_RANGE));
    await performHumanLikeDelays(page);

    const html = await page.content();
    const botDetection = detectBotProtection(html);
    if (botDetection.detected) {
      interstitialDetected = true;
      blockedReasons = botDetection.matches;
      await delay(randomBetween(3000, 5000));
      try {
        await performHumanLikeDelays(page);
        await page.reload({ waitUntil: "domcontentloaded", timeout: NAVIGATION_TIMEOUT });
      } catch (err) {
        debugLog("Reload after interstitial failed", err.message);
      }
    }

    const finalHtml = interstitialDetected ? await page.content() : html;
    const $ = cheerio.load(finalHtml);
    const windowPayloads = await gatherWindowPayloads(page);
    const jsonLd = extractStructuredData($, finalHtml, url);
    const openGraph = extractOpenGraph($, url);
    const windowExtraction = extractWindowGlobals(windowPayloads, url);
    const networkExtraction = extractNetworkPayloads(networkPayloads, url);
    const regexExtraction = extractRegexImages(finalHtml, url);
    const domExtraction = extractDomHeuristics($, url);

    const merged = mergeStrategyResults([
      jsonLd,
      openGraph,
      windowExtraction,
      networkExtraction,
      regexExtraction,
      domExtraction,
    ]);

    const durationSeconds = (performance.now() - networkStats.start) / 1000;
    const domain = safeDomain(page.url() || url);
    const blocked = interstitialDetected && merged.product.images.length === 0;

    return {
      ok: !blocked,
      blocked,
      interstitialDetected,
      blockedReasons,
      html: finalHtml,
      product: merged.product,
      confidence: merged.confidence,
      jsonLd,
      openGraph,
      windowExtraction,
      networkExtraction,
      regexExtraction,
      domExtraction,
      stats: {
        requests: networkStats.requests,
        transferMB: Number((networkStats.bytes / (1024 * 1024)).toFixed(3)),
        durationSeconds,
        proxy: proxy?.label || "none",
      },
      debug: {
        domain,
        userAgent,
        viewport,
        attempt,
        proxy: proxy?.raw || null,
      },
    };
  } catch (err) {
    debugLog("Attempt failed", err.message);
    throw err;
  } finally {
    await page.close().catch(() => {});
    await browser.close().catch(() => {});
  }
}

function safeDomain(url) {
  try {
    return new URL(url).hostname;
  } catch {
    return null;
  }
}

function jsonContainsProductSignals(value, depth = 0) {
  if (!value || typeof value !== "object" || depth > 6) return false;
  if (Array.isArray(value)) {
    return value.some((item) => jsonContainsProductSignals(item, depth + 1));
  }
  for (const [key, val] of Object.entries(value)) {
    if (JSON_SEARCH_KEYS.has(key.toLowerCase())) return true;
    if (val && typeof val === "object" && jsonContainsProductSignals(val, depth + 1)) {
      return true;
    }
  }
  return false;
}

async function scrapeProduct(url) {
  if (!url) {
    throw new Error("URL is required");
  }
  const attempts = [];
  let lastError = null;
  let blockedReasons = [];

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt += 1) {
    const proxy = PROXY_POOL.length ? PROXY_POOL[(attempt - 1) % PROXY_POOL.length] : null;
    try {
      const result = await scrapeAttempt(url, attempt, proxy);
      attempts.push(result);
      if (result.ok && result.product.images.length > 0) {
        return buildPayload(result, attempt - 1, blockedReasons);
      }
      if (result.blocked) {
        blockedReasons = result.blockedReasons || blockedReasons;
        continue;
      }
      if (result.product.images.length > 0 || result.product.title || result.product.price) {
        return buildPayload(result, attempt - 1, blockedReasons);
      }
    } catch (err) {
      lastError = err;
      debugLog(`Attempt ${attempt} failed`, err.message);
    }
  }

  const finalAttempt = attempts.at(-1);
  if (finalAttempt?.blocked) {
    return {
      ok: false,
      status: "blocked",
      error: "Bot protection (Akamai/Cloudflare/etc.)",
      meta: {
        network: finalAttempt.stats,
        imagesFound: 0,
        confidence: 0,
        blocked: true,
        debug: {
          retries: MAX_ATTEMPTS - 1,
          interstitialDetected: true,
          domain: finalAttempt.debug.domain,
        },
      },
    };
  }

  if (lastError) {
    throw lastError;
  }

  const fallbackResult = finalAttempt || { product: { images: [] }, stats: { requests: 0, transferMB: 0, durationSeconds: 0, proxy: "none" }, confidence: 0, debug: { domain: safeDomain(url) } };
  return buildPayload(fallbackResult, MAX_ATTEMPTS - 1, blockedReasons);
}

function buildPayload(result, retries, blockedReasons) {
  const imagesFound = result.product.images.length;
  const payload = {
    ok: true,
    title: result.product.title || null,
    description: result.product.description || null,
    price: result.product.price || null,
    images: result.product.images,
    meta: {
      network: result.stats,
      imagesFound,
      confidence: Number(result.confidence.toFixed(3)),
      blocked: Boolean(result.blocked),
      debug: {
        retries,
        interstitialDetected: Boolean(result.interstitialDetected),
        domain: result.debug.domain,
        blockedReasons,
      },
    },
  };
  if (payload.meta.confidence < 0.5) {
    payload.meta.debug.lowConfidence = true;
  }
  if (result.blocked && imagesFound === 0) {
    payload.ok = false;
    payload.status = "blocked";
    payload.error = "Anti-bot protection detected";
  }
  return payload;
}

app.get("/", (_req, res) => {
  res.json({ ok: true, status: "feednly-scraper", uptime: process.uptime() });
});

app.get("/health", (_req, res) => {
  res.json({ ok: true, uptime: process.uptime(), proxies: PROXY_POOL.length });
});

app.get("/scrape", async (req, res) => {
  const { url } = req.query;
  if (!url) {
    res.status(400).json({ ok: false, error: "Missing url query parameter" });
    return;
  }
  try {
    const result = await scrapeProduct(`${url}`);
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

app.listen(PORT, "0.0.0.0", () => {
  debugLog(`Feednly Scraper listening on port ${PORT}`);
});
