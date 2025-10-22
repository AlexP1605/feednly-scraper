import express from "express";
import * as cheerio from "cheerio";
import axios from "axios";
import pRetry from "p-retry";
import { access } from "node:fs/promises";
import { constants as fsConstants } from "node:fs";

const app = express();

process.on("uncaughtException", (err) => {
  console.error("Uncaught exception:", err);
});

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled rejection:", reason);
});

const portValue = process.env.PORT;
let PORT = Number.parseInt(`${portValue ?? ""}`.trim(), 10);
if (!Number.isFinite(PORT) || PORT <= 0) {
  console.warn(
    `Invalid PORT environment variable value "${portValue}". Falling back to 8080.`
  );
  PORT = 8080;
}
const PROXY = process.env.SCRAPER_PROXY || null;
let MAX_RETRIES = parseInt(process.env.MAX_RETRIES || "2", 10);
if (!Number.isFinite(MAX_RETRIES) || MAX_RETRIES < 0) {
  console.warn(
    `Invalid MAX_RETRIES value "${process.env.MAX_RETRIES}". Falling back to 2.`
  );
  MAX_RETRIES = 2;
}
const DISABLE_PUPPETEER = process.env.DISABLE_PUPPETEER === "true";
const CHROMIUM_OVERRIDE = process.env.PUPPETEER_EXECUTABLE_PATH || "/usr/bin/chromium";

let puppeteerModulePromise = null;
let chromiumProbePromise = null;

async function loadPuppeteer() {
  if (!puppeteerModulePromise) {
    puppeteerModulePromise = import("puppeteer")
      .then((mod) => mod?.default ?? mod)
      .catch((err) => {
        puppeteerModulePromise = null;
        console.error("Failed to load Puppeteer module:", err);
        throw err;
      });
  }
  return puppeteerModulePromise;
}

async function resolveChromiumExecutable() {
  if (!CHROMIUM_OVERRIDE) {
    return null;
  }

  if (!chromiumProbePromise) {
    chromiumProbePromise = access(CHROMIUM_OVERRIDE, fsConstants.X_OK)
      .then(() => CHROMIUM_OVERRIDE)
      .catch((err) => {
        console.error(
          `Chromium executable declared at ${CHROMIUM_OVERRIDE} is not accessible; falling back to Puppeteer's bundled binary if available.`,
          err
        );
        return null;
      });
  }

  return chromiumProbePromise;
}

const LAUNCH_ARGS = [
  "--no-sandbox",
  "--disable-setuid-sandbox",
  "--disable-dev-shm-usage",
  "--disable-accelerated-2d-canvas",
  "--disable-gpu",
  "--no-zygote",
  "--single-process"
];
if (PROXY) LAUNCH_ARGS.unshift(`--proxy-server=${PROXY}`);

async function launchBrowser() {
  const puppeteer = await loadPuppeteer();
  const launchOptions = {
    headless: "new",
    args: LAUNCH_ARGS
  };

  const executablePath = await resolveChromiumExecutable();
  if (executablePath) {
    launchOptions.executablePath = executablePath;
  }

  return puppeteer.launch(launchOptions);
}

// Normalisation des URLs d’images
function normalizeUrl(src, baseUrl) {
  if (!src) return null;
  src = src.trim();
  if (src.startsWith("//")) return "https:" + src;
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

// Fallback simple avec Axios
async function fetchWithAxios(url) {
  const res = await axios.get(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
      "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"
    },
    timeout: 20000
  });
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

    if (!Array.isArray(current)) {
      result.push(current);
    }

    const values = Array.isArray(current) ? current : Object.values(current);
    for (const value of values) {
      if (value && typeof value === "object") {
        stack.push(value);
      }
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

    const tryParse = (text) => {
      try {
        const parsed = JSON.parse(text);
        nodes.push(...flattenJsonLd(parsed));
        return true;
      } catch {
        return false;
      }
    };

    if (tryParse(trimmed)) continue;

    const withoutComments = trimmed.replace(/\/\*[\s\S]*?\*\//g, "");
    if (tryParse(withoutComments)) continue;

    const withoutHtmlComments = withoutComments.replace(/<!--.*?-->/gs, "");
    tryParse(withoutHtmlComments);
  }

  return nodes;
}

function nodeIsProduct(node) {
  if (!node || typeof node !== "object") return false;
  const type = node["@type"];
  if (!type) return false;
  if (Array.isArray(type)) {
    return type.some(
      (t) => typeof t === "string" && t.toLowerCase().includes("product")
    );
  }
  return typeof type === "string" && type.toLowerCase().includes("product");
}

function extractPriceFromOffers(offers) {
  if (!offers) return null;
  const offerList = Array.isArray(offers) ? offers : [offers];

  for (const offer of offerList) {
    if (!offer || typeof offer !== "object") continue;
    const directPrice = offer.price ?? offer.lowPrice ?? offer.highPrice;
    const spec = offer.priceSpecification || {};
    const specPrice =
      spec.price ?? spec.priceAmount ?? spec.minPrice ?? spec.maxPrice ?? null;
    const price = directPrice ?? specPrice;
    if (price == null) continue;

    const currency =
      offer.priceCurrency ||
      spec.priceCurrency ||
      spec.currency ||
      offer.currency;

    const priceString = String(price).trim();
    if (!priceString) continue;

    return currency ? `${priceString} ${currency}`.trim() : priceString;
  }

  return null;
}

function extractImagesFromNode(node, baseUrl) {
  const images = new Set();
  const addImage = (value) => {
    if (!value) return;
    if (typeof value === "string") {
      const normalized = normalizeUrl(value, baseUrl);
      if (normalized && !normalized.startsWith("data:")) images.add(normalized);
      return;
    }
    if (Array.isArray(value)) {
      value.forEach(addImage);
      return;
    }
    if (typeof value === "object") {
      if (value.url) addImage(value.url);
      if (value.contentUrl) addImage(value.contentUrl);
    }
  };

  addImage(node.image);
  addImage(node.images);
  addImage(node.photo);
  addImage(node.thumbnailUrl);

  return Array.from(images);
}

function extractFromJsonLd(jsonLdScripts, url) {
  const nodes = parseJsonLdScripts(jsonLdScripts);
  const productNode = nodes.find(nodeIsProduct);
  if (!productNode) {
    return { title: null, description: null, price: null, images: [] };
  }

  const title = productNode.name || productNode.headline || productNode.title || null;
  const description = productNode.description || null;
  const price =
    extractPriceFromOffers(productNode.offers) ||
    (productNode.price ? String(productNode.price) : null);
  const images = extractImagesFromNode(productNode, url);

  return { title, description, price, images };
}

function mergeProductData(primary, secondary) {
  return {
    title: primary.title || secondary.title || null,
    description: primary.description || secondary.description || null,
    price: primary.price || secondary.price || null,
    images: Array.from(
      new Set([...(primary.images || []), ...(secondary.images || [])])
    )
  };
}

// Scraping via Puppeteer
async function scrapeWithPuppeteer(url) {
  if (DISABLE_PUPPETEER) {
    throw new Error("Puppeteer usage disabled by DISABLE_PUPPETEER env var");
  }

  const browser = await launchBrowser();
  try {
    const page = await browser.newPage();
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"
    );
    await page.setExtraHTTPHeaders({
      "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"
    });

    await page.goto(url, { waitUntil: "networkidle2", timeout: 40000 });
    await page.waitForTimeout(500);

    const html = await page.content();
    const jsonLd = await page.$$eval(
      'script[type="application/ld+json"]',
      (scripts) => scripts.map((s) => s.innerText)
    );

    await page.close();
    await browser.close();

    return { html, jsonLd };
  } catch (err) {
    try {
      await browser.close();
    } catch {}
    throw err;
  }
}

// Extraction des données
function extractFromHtml(html, url, jsonLdScripts = null) {
  const $ = cheerio.load(html);
  const scripts =
    jsonLdScripts && Array.isArray(jsonLdScripts) && jsonLdScripts.length
      ? jsonLdScripts
      : $("script[type='application/ld+json']")
          .map((_, el) => $(el).text())
          .get();

  const jsonLdData = extractFromJsonLd(scripts, url);
  const title =
    jsonLdData.title ||
    $("meta[property='og:title']").attr("content") ||
    $("meta[name='twitter:title']").attr("content") ||
    $("title").text() ||
    null;
  const description =
    jsonLdData.description ||
    $("meta[property='og:description']").attr("content") ||
    $("meta[name='description']").attr("content") ||
    $("meta[name='twitter:description']").attr("content") ||
    null;

  let price =
    jsonLdData.price ||
    $("meta[property='product:price:amount']").attr("content") ||
    $("meta[name='price']").attr("content") ||
    $('[itemprop="price"]').attr("content") ||
    $('[class*=\"price\"]').first().text() ||
    $('[data-price]').attr("data-price") ||
    null;

  price = price ? String(price).trim() : null;

  const imgs = new Set();
  for (const img of jsonLdData.images || []) {
    if (img && !img.startsWith("data:")) imgs.add(img);
  }
  $("img").each((i, el) => {
    const src =
      $(el).attr("src") ||
      $(el).attr("data-src") ||
      $(el).attr("data-lazy") ||
      "";
    const normalized = normalizeUrl(src, url);
    if (normalized && !normalized.startsWith("data:")) imgs.add(normalized);
  });
  const ogImg = $("meta[property='og:image']").attr("content");
  if (ogImg) imgs.add(normalizeUrl(ogImg, url));

  const images = Array.from(imgs);
  return { title, description, price, images };
}

// Route principale /scrape
app.get("/", (req, res) => {
  res.json({ ok: true, status: "feednly-scraper", uptime: process.uptime() });
});

app.get("/scrape", async (req, res) => {
  const url = req.query.url;
  if (!url)
    return res
      .status(400)
      .json({ error: "Missing URL query param (example: /scrape?url=https://...)" });

  try {
    let html = null;
    let jsonLdScripts = null;
    let usedPuppeteer = false;

    if (!DISABLE_PUPPETEER) {
      try {
        const result = await pRetry(() => scrapeWithPuppeteer(url), {
          retries: MAX_RETRIES
        });
        html = result.html;
        jsonLdScripts = result.jsonLd;
        usedPuppeteer = true;
      } catch (err) {
        console.warn("Puppeteer scrape failed, falling back to Axios:", err.message);
      }
    }

    if (!html) {
      html = await fetchWithAxios(url);
      jsonLdScripts = extractJsonLdScriptsFromHtml(html);
    }

    let out = extractFromHtml(html, url, jsonLdScripts);

    if (usedPuppeteer && (!out.images?.length || !out.title || !out.description || !out.price)) {
      try {
        const fallbackHtml = await fetchWithAxios(url);
        const fallbackScripts = extractJsonLdScriptsFromHtml(fallbackHtml);
        const fallback = extractFromHtml(fallbackHtml, url, fallbackScripts);
        out = mergeProductData(out, fallback);
      } catch (fallbackErr) {
        console.warn("Axios fallback enrichment failed:", fallbackErr.message);
      }
    }

    res.json({ ok: true, ...out });
  } catch (err) {
    console.error("Scrape error:", err.message);
    res
      .status(500)
      .json({ ok: false, error: "Scraping failed", detail: err.message });
  }
});

const server = app.listen(PORT, "0.0.0.0", () =>
  console.log(`✅ Feednly Scraper running on port ${PORT}`)
);

server.on("error", (err) => {
  console.error("HTTP server failed to start:", err);
  process.exitCode = 1;
});
