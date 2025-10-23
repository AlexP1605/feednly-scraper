import express from "express";
import * as cheerio from "cheerio";
import axios from "axios";
import pRetry from "p-retry";
import { access } from "node:fs/promises";
import { constants as fsConstants } from "node:fs";
import { Buffer } from "node:buffer";

const app = express();
app.set("etag", false); // 🔥 désactive les réponses 304 (cache)

process.on("uncaughtException", (err) => console.error("❌ Uncaught exception:", err));
process.on("unhandledRejection", (reason) => console.error("⚠️ Unhandled rejection:", reason));

const portValue = process.env.PORT;
let PORT = Number.parseInt(`${portValue ?? ""}`.trim(), 10);
if (!Number.isFinite(PORT) || PORT <= 0) {
  console.warn(`Invalid PORT "${portValue}", fallback to 8080.`);
  PORT = 8080;
}

const PROXY = process.env.SCRAPER_PROXY || null;
let MAX_RETRIES = parseInt(process.env.MAX_RETRIES || "2", 10);
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
const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
];

function pickUserAgent() {
  if (!USER_AGENTS.length) {
    return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36";
  }
  const index = Math.floor(Math.random() * USER_AGENTS.length);
  return USER_AGENTS[index];
}

const CHROMIUM_OVERRIDE = process.env.PUPPETEER_EXECUTABLE_PATH || "/usr/bin/chromium";

let puppeteerModulePromise = null;
let chromiumProbePromise = null;

async function loadPuppeteer() {
  if (!puppeteerModulePromise) {
    puppeteerModulePromise = import("puppeteer")
      .then((mod) => mod?.default ?? mod)
      .catch((err) => {
        puppeteerModulePromise = null;
        console.error("❌ Failed to load Puppeteer module:", err);
        throw err;
      });
  }
  return puppeteerModulePromise;
}

async function resolveChromiumExecutable() {
  if (!CHROMIUM_OVERRIDE) return null;
  if (!chromiumProbePromise) {
    chromiumProbePromise = access(CHROMIUM_OVERRIDE, fsConstants.X_OK)
      .then(() => CHROMIUM_OVERRIDE)
      .catch((err) => {
        console.warn(
          `⚠️ Chromium not accessible at ${CHROMIUM_OVERRIDE}, falling back.`,
          err
        );
        return null;
      });
  }
  return chromiumProbePromise;
}

async function launchBrowser() {
  const puppeteer = await loadPuppeteer();
  const executablePath = (await resolveChromiumExecutable()) || undefined;
  const launchOptions = {
    headless: true,
    args: [
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
    ],
    executablePath,
  };

  if (PROXY) {
    launchOptions.args.push(`--proxy-server=${PROXY}`);
  }

  console.log("🚀 Launching Chromium...");
  const browser = await puppeteer.launch(launchOptions);
  console.log("✅ Chromium started successfully!");
  return browser;
}

function normalizeUrl(src, baseUrl) {
  if (!src) return null;
  src = src.trim();
  if (src.startsWith("//")) return "https:" + src;
  if (src.startsWith("data:")) return null;
  if (src.startsWith("/")) {
    try {
      const base = new URL(baseUrl);
      return base.origin + src;
    } catch {
      return src;
    }
  }
  if (!src.startsWith("http")) {
    try {
      const base = new URL(baseUrl);
      return base.origin + "/" + src;
    } catch {
      return src;
    }
  }
  return src;
}

async function fetchWithAxios(url) {
  const userAgent = pickUserAgent();
  const requestConfig = {
    headers: {
      "User-Agent": userAgent,
      "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
      Accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    },
    timeout: 25000,
    maxRedirects: 5,
    decompress: true,
    responseType: "text",
    validateStatus: (status) => status >= 200 && status < 400,
  };

  if (PROXY) {
    try {
      const proxyUrl = new URL(PROXY);
      if (proxyUrl.username || proxyUrl.password) {
        requestConfig.headers["Proxy-Authorization"] =
          "Basic " +
          Buffer.from(
            `${decodeURIComponent(proxyUrl.username)}:${decodeURIComponent(
              proxyUrl.password
            )}`
          ).toString("base64");
      }
      if (proxyUrl.protocol.startsWith("http")) {
        const defaultPort = proxyUrl.protocol === "https:" ? 443 : 80;
        requestConfig.proxy = {
          protocol: proxyUrl.protocol.replace(":", ""),
          host: proxyUrl.hostname,
          port: proxyUrl.port ? Number.parseInt(proxyUrl.port, 10) : defaultPort,
        };
      } else {
        requestConfig.proxy = false;
      }
    } catch (err) {
      console.warn("⚠️ Failed to configure proxy for Axios:", err.message);
    }
  }

  const res = await axios.get(url, requestConfig);
  console.log("🕵️ Axios user agent:", userAgent);
  return res.data;
}

function extractJsonLdScriptsFromHtml(html) {
  const $ = cheerio.load(html);
  return $("script[type='application/ld+json']")
    .map((_, el) => $(el).text())
    .get();
}

function flattenJsonLd(node, seen = new Set()) {
  const result = [];
  const stack = [node];
  while (stack.length) {
    const current = stack.pop();
    if (!current || typeof current !== "object") continue;
    if (seen.has(current)) continue;
    seen.add(current);
    if (!Array.isArray(current)) result.push(current);
    const values = Array.isArray(current) ? current : Object.values(current);
    for (const value of values)
      if (value && typeof value === "object") stack.push(value);
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
    } catch {}
  }
  return nodes;
}

function ensureArray(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  return [value];
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
  ];
  const urls = [];
  for (const field of imageFields) {
    for (const value of ensureArray(field)) {
      if (typeof value === "string" && value.trim()) {
        urls.push(value.trim());
      } else if (value && typeof value === "object") {
        if (typeof value.url === "string") urls.push(value.url.trim());
        if (typeof value.contentUrl === "string")
          urls.push(value.contentUrl.trim());
      }
    }
  }
  return urls;
}

function extractFromJsonLdNodes(nodes) {
  if (!nodes?.length) return {};
  let title = null;
  let description = null;
  let price = null;
  const images = [];

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

  return { title, description, price, images };
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
    console.warn("⚠️ Unable to parse __NEXT_DATA__ payload:", err.message);
    return null;
  }
}

function extractFromNextData(nextData) {
  if (!nextData) return {};
  let title = null;
  let description = null;
  let price = null;
  const images = [];

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
    const media = ensureArray(node.media || node.gallery || node.mediaItems);
    for (const mediaEntry of media) {
      if (mediaEntry && typeof mediaEntry === "object") {
        if (typeof mediaEntry.url === "string") images.push(mediaEntry.url);
        if (typeof mediaEntry.src === "string") images.push(mediaEntry.src);
        if (typeof mediaEntry.imageUrl === "string")
          images.push(mediaEntry.imageUrl);
      }
    }
  }

  return { title, description, price, images };
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

  console.log("🌐 Scraping with Puppeteer:", url);

  const browser = await launchBrowser();
  try {
    const page = await browser.newPage();
    const userAgent = pickUserAgent();
    await page.setUserAgent(userAgent);
    await page.setExtraHTTPHeaders({
      "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
      "Upgrade-Insecure-Requests": "1",
    });
    await page.setViewport({ width: 1280, height: 720, deviceScaleFactor: 1 });

    const { waitSelectors = [], waitAfterLoadMs = DEFAULT_WAIT_AFTER_LOAD } = options;

    await page.goto(url, {
      waitUntil: ["domcontentloaded", "networkidle2"],
      timeout: NAVIGATION_TIMEOUT,
    });

    try {
      await page.waitForFunction(
        () =>
          document.readyState === "complete" ||
          !!document.querySelector("meta[property='og:title']"),
        { timeout: Math.min(NAVIGATION_TIMEOUT, 30000) }
      );
    } catch (err) {
      console.warn("⚠️ Wait for readyState/meta timed out:", err.message);
    }

    if (waitSelectors.length) {
      await waitForSelectors(page, waitSelectors, Math.min(NAVIGATION_TIMEOUT, 30000));
    }

    if (waitAfterLoadMs) {
      await page.waitForTimeout(waitAfterLoadMs);
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
        return null;
      })
      .catch(() => null);

    console.log("✅ Page loaded successfully!");
    await browser.close();
    return { html, jsonLd, nextData: nextDataPayload, userAgent };
  } catch (err) {
    console.error("❌ Puppeteer scraping error:", err);
    try {
      await browser.close();
    } catch {}
    throw err;
  }
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

async function scrapeWithPuppeteerWithOptions(url, waitSelectors, waitAfterLoadMs) {
  return scrapeWithPuppeteer(url, {
    waitSelectors,
    waitAfterLoadMs,
  });
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
  for (const image of images) {
    if (!image) continue;
    const key = image.trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    result.push(key);
  }
  return result;
}

function extractFromHtml(html, url, jsonLdScripts = null, nextDataPayload = null) {
  const $ = cheerio.load(html);
  let title =
    $("meta[property='og:title']").attr("content") ||
    $("title").text() ||
    null;
  let description =
    $("meta[property='og:description']").attr("content") ||
    $("meta[name='description']").attr("content") ||
    null;
  let price =
    $("meta[property='product:price:amount']").attr("content") ||
    $('[itemprop="price"]').attr("content") ||
    $("meta[name='price']").attr("content") ||
    null;

  if (title)
    title = title.replace(/Prix Fnac/gi, "").replace(/\s{2,}/g, " ").trim();

  const jsonLdNodes = parseJsonLdScripts(jsonLdScripts);
  const jsonLdExtraction = extractFromJsonLdNodes(jsonLdNodes);

  const inlineNextData =
    nextDataPayload || $("#__NEXT_DATA__").html() || $("#__NUXT__").html();
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

  images.push(...jsonLdExtraction.images.map((img) => normalizeUrl(img, url)));
  images.push(...nextDataExtraction.images.map((img) => normalizeUrl(img, url)));

  const cleanedImages = dedupeImages(images.filter(Boolean)).slice(0, 100);

  return { title, description, price, images: cleanedImages };
}

app.get("/", (req, res) => {
  res.json({ ok: true, status: "feednly-scraper", uptime: process.uptime() });
});

app.get("/scrape", async (req, res) => {
  res.set("Cache-Control", "no-store"); // 🔥 évite le cache (304)
  const url = req.query.url;
  if (!url)
    return res
      .status(400)
      .json({ error: "Missing URL query param (?url=https://...)" });

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
    let html, jsonLdScripts;
    let nextDataPayload = null;
    try {
      const result = await pRetry(
        () => scrapeWithPuppeteerWithOptions(url, trimmedSelectors, waitAfterLoadMs),
        {
          retries: MAX_RETRIES,
        }
      );
      html = result.html;
      jsonLdScripts = result.jsonLd;
      nextDataPayload = result.nextData;
      if (result.userAgent) {
        console.log("🕵️ Puppeteer user agent:", result.userAgent);
      }
    } catch (err) {
      console.warn("⚠️ Puppeteer failed, fallback to Axios:", err.message);
      html = await fetchWithAxios(url);
      jsonLdScripts = extractJsonLdScriptsFromHtml(html);
    }

    const data = extractFromHtml(html, url, jsonLdScripts, nextDataPayload);
    console.log("✅ Extraction:", data);
    res.json({ ok: true, ...data });
  } catch (err) {
    console.error("Scrape error:", err.message);
    res.status(500).json({ ok: false, error: err.message });
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

setTimeout(async () => {
  if (DISABLE_PUPPETEER) {
    console.log("⏭️ Puppeteer preload skipped (disabled via env).");
    return;
  }
  try {
    console.log("⏳ Preloading Puppeteer...");
    const browser = await launchBrowser();
    await browser.close();
    console.log("✅ Puppeteer preloaded successfully!");
  } catch (err) {
    console.warn("⚠️ Puppeteer preload failed:", err.message);
  }
}, 1000);
