import express from "express";
import * as cheerio from "cheerio";
import axios from "axios";
import pRetry from "p-retry";
import { access } from "node:fs/promises";
import { constants as fsConstants } from "node:fs";

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
      .catch(() => null);
  }
  return chromiumProbePromise;
}

async function launchBrowser() {
  const puppeteer = await loadPuppeteer();
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
    executablePath: "/usr/bin/chromium",
  };

  console.log("🚀 Launching Chromium...");
  const browser = await puppeteer.launch(launchOptions);
  console.log("✅ Chromium started successfully!");
  return browser;
}

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

async function fetchWithAxios(url) {
  const res = await axios.get(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
      "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    },
    timeout: 20000,
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

async function scrapeWithPuppeteer(url) {
  if (DISABLE_PUPPETEER)
    throw new Error("Puppeteer disabled by env var DISABLE_PUPPETEER");

  console.log("🌐 Scraping with Puppeteer:", url);

  const browser = await launchBrowser();
  try {
    const page = await browser.newPage();
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"
    );
    await page.setViewport({ width: 1280, height: 720, deviceScaleFactor: 1 });
    await page.setExtraHTTPHeaders({
      "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
      Accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
      "Sec-Ch-Ua": '"Chromium";v="123", "Not:A-Brand";v="8"',
      "Sec-Ch-Ua-Mobile": "?0",
      "Sec-Ch-Ua-Platform": '"Windows"',
      "Upgrade-Insecure-Requests": "1",
    });

    await page.goto(url, { waitUntil: "networkidle2", timeout: 60000 });
    await page.waitForTimeout(750);

    const consentSelectors = [
      "#didomi-notice-agree-button",
      "button[id*='didomi'][id*='agree']",
      "button[class*='didomi'][class*='agree']",
      "#onetrust-accept-btn-handler",
      "button[id*='onetrust'][id*='accept']",
      "button[data-testid='accept-all']",
    ];
    for (const selector of consentSelectors) {
      try {
        const handle = await page.$(selector);
        if (handle) {
          await handle.click().catch(() => {});
          await page.waitForTimeout(500);
          break;
        }
      } catch {}
    }

    await page.waitForTimeout(500);

    const html = await page.content();
    const jsonLd = await page.$$eval(
      'script[type="application/ld+json"]',
      (scripts) => scripts.map((s) => s.innerText)
    );

    console.log("✅ Page loaded successfully!");
    await browser.close();
    return { html, jsonLd };
  } catch (err) {
    console.error("❌ Puppeteer scraping error:", err);
    try {
      await browser.close();
    } catch {}
    throw err;
  }
}

function normalizePrice(raw) {
  if (!raw) return null;
  if (typeof raw !== "string") raw = `${raw}`;
  const cleaned = raw
    .replace(/[^0-9,\.]/g, "")
    .replace(/,(?=\d{3}(?:\D|$))/g, "")
    .replace(/\.(?=\d{3}(?:\D|$))/g, "")
    .replace(",", ".");
  if (!cleaned || !/[0-9]/.test(cleaned)) return null;
  return cleaned;
}

function extractFromHtml(html, url, jsonLdScripts = null) {
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
    normalizePrice($("meta[property='product:price:amount']").attr("content")) ||
    normalizePrice($("[itemprop='price']").attr("content")) ||
    normalizePrice($("[data-price-amount]").attr("data-price-amount")) ||
    null;

  if (title)
    title = title.replace(/Prix Fnac/gi, "").replace(/\s{2,}/g, " ").trim();

  // 🔍 récupération du prix via JSON-LD si manquant
  if (!price && jsonLdScripts?.length) {
    const nodes = parseJsonLdScripts(jsonLdScripts);
    const offer = nodes.find((n) => n["@type"] === "Offer" && n.price);
    if (offer?.price) price = normalizePrice(offer.price);
    if (!price) {
      const product = nodes.find((n) => n["@type"] === "Product" && n.offers?.price);
      if (product?.offers?.price) price = normalizePrice(product.offers.price);
    }
  }

  if (!price) {
    const fnacPrice = normalizePrice(
      $(".f-priceBox__price, [class*='price']").first().text()
    );
    if (fnacPrice) price = fnacPrice;
  }

  if (!price) {
    const priceFromJsonScript = $("script[type='application/json']")
      .map((_, el) => {
        try {
          return JSON.parse($(el).text());
        } catch {
          return null;
        }
      })
      .get()
      .map((obj) => {
        if (!obj || typeof obj !== "object") return null;
        if (obj.offers?.price) return normalizePrice(obj.offers.price);
        if (obj.product?.offers?.price)
          return normalizePrice(obj.product.offers.price);
        return null;
      })
      .find((value) => !!value);
    if (priceFromJsonScript) price = priceFromJsonScript;
  }

  const images = [];
  const seenImages = new Set();
  $("img").each((i, el) => {
    const src =
      $(el).attr("src") ||
      $(el).attr("data-src") ||
      ($(el).attr("data-srcset") || "").split(" ")[0] ||
      null;
    const normalized = normalizeUrl(src, url);
    if (
      normalized &&
      !normalized.includes("data:image/svg+xml") &&
      !seenImages.has(normalized)
    ) {
      seenImages.add(normalized);
      images.push(normalized);
    }
  });

  return { title, description, price, images };
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

  try {
    let html, jsonLdScripts;
    try {
      const result = await pRetry(() => scrapeWithPuppeteer(url), {
        retries: MAX_RETRIES,
      });
      html = result.html;
      jsonLdScripts = result.jsonLd;
    } catch (err) {
      console.warn("⚠️ Puppeteer failed, fallback to Axios:", err.message);
      html = await fetchWithAxios(url);
      jsonLdScripts = extractJsonLdScriptsFromHtml(html);
    }

    const data = extractFromHtml(html, url, jsonLdScripts);
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
