import express from "express";
import * as cheerio from "cheerio";
import axios from "axios";
import pRetry from "p-retry";
import { access } from "node:fs/promises";
import { constants as fsConstants } from "node:fs";

const app = express();

// --- Gestion globale des erreurs ---
process.on("uncaughtException", (err) => {
  console.error("❌ Uncaught exception:", err);
});
process.on("unhandledRejection", (reason) => {
  console.error("⚠️ Unhandled rejection:", reason);
});

// --- Vérification du PORT (Cloud Run) ---
const portValue = process.env.PORT;
let PORT = Number.parseInt(`${portValue ?? ""}`.trim(), 10);
if (!Number.isFinite(PORT) || PORT <= 0) {
  console.warn(`Invalid PORT "${portValue}", fallback to 8080.`);
  PORT = 8080;
}

// --- Variables globales ---
const PROXY = process.env.SCRAPER_PROXY || null;
let MAX_RETRIES = parseInt(process.env.MAX_RETRIES || "2", 10);
if (!Number.isFinite(MAX_RETRIES) || MAX_RETRIES < 0) MAX_RETRIES = 2;
const DISABLE_PUPPETEER = process.env.DISABLE_PUPPETEER === "true";
const CHROMIUM_OVERRIDE =
  process.env.PUPPETEER_EXECUTABLE_PATH || "/usr/bin/chromium";

let puppeteerModulePromise = null;
let chromiumProbePromise = null;

// --- Chargement lazy de Puppeteer ---
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

// --- Vérification de l'exécutable Chromium ---
async function resolveChromiumExecutable() {
  if (!CHROMIUM_OVERRIDE) return null;
  if (!chromiumProbePromise) {
    chromiumProbePromise = access(CHROMIUM_OVERRIDE, fsConstants.X_OK)
      .then(() => CHROMIUM_OVERRIDE)
      .catch((err) => {
        console.error(
          `⚠️ Chromium not accessible at ${CHROMIUM_OVERRIDE}, falling back.`,
          err
        );
        return null;
      });
  }
  return chromiumProbePromise;
}

// --- Arguments Puppeteer ---
const LAUNCH_ARGS = [
  "--no-sandbox",
  "--disable-setuid-sandbox",
  "--disable-dev-shm-usage",
  "--disable-accelerated-2d-canvas",
  "--disable-gpu",
  "--no-zygote",
  "--single-process",
];
if (PROXY) LAUNCH_ARGS.unshift(`--proxy-server=${PROXY}`);

// --- Lancement du navigateur ---
async function launchBrowser() {
  const puppeteer = await loadPuppeteer();
  const launchOptions = { headless: "new", args: LAUNCH_ARGS };
  const executablePath = await resolveChromiumExecutable();
  if (executablePath) launchOptions.executablePath = executablePath;
  return puppeteer.launch(launchOptions);
}

// --- Normalisation des URLs d’images ---
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

// --- Scraping via Axios fallback ---
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

// --- Extraction JSON-LD ---
function extractJsonLdScriptsFromHtml(html) {
  const $ = cheerio.load(html);
  return $("script[type='application/ld+json']")
    .map((_, el) => $(el).text())
    .get();
}

// --- Flatten JSON-LD ---
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

// --- Parse JSON-LD scripts ---
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

// --- Scraping avec Puppeteer ---
async function scrapeWithPuppeteer(url) {
  if (DISABLE_PUPPETEER)
    throw new Error("Puppeteer disabled by env var DISABLE_PUPPETEER");
  const browser = await launchBrowser();
  try {
    const page = await browser.newPage();
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"
    );
    await page.goto(url, { waitUntil: "networkidle2", timeout: 40000 });
    const html = await page.content();
    const jsonLd = await page.$$eval(
      'script[type="application/ld+json"]',
      (scripts) => scripts.map((s) => s.innerText)
    );
    await browser.close();
    return { html, jsonLd };
  } catch (err) {
    try {
      await browser.close();
    } catch {}
    throw err;
  }
}

// --- Extraction depuis HTML ---
function extractFromHtml(html, url, jsonLdScripts = null) {
  const $ = cheerio.load(html);
  const title =
    $("meta[property='og:title']").attr("content") ||
    $("title").text() ||
    null;
  const description =
    $("meta[property='og:description']").attr("content") ||
    $("meta[name='description']").attr("content") ||
    null;
  const price =
    $("meta[property='product:price:amount']").attr("content") ||
    $('[itemprop="price"]').attr("content") ||
    null;
  const images = [];
  $("img").each((i, el) => {
    const src = $(el).attr("src");
    const normalized = normalizeUrl(src, url);
    if (normalized) images.push(normalized);
  });
  return { title, description, price, images };
}

// --- Routes principales ---
app.get("/", (req, res) => {
  res.json({ ok: true, status: "feednly-scraper", uptime: process.uptime() });
});

app.get("/scrape", async (req, res) => {
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
    res.json({ ok: true, ...data });
  } catch (err) {
    console.error("Scrape error:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// --- Démarrage serveur ---
const server = app.listen(PORT, "0.0.0.0", () => {
  console.log(`✅ Feednly Scraper running on port ${PORT}`);
  console.log("ℹ️ Waiting for incoming requests...");
});

server.on("error", (err) => {
  console.error("❌ HTTP server failed to start:", err);
  process.exit(1);
});

// --- Préchargement asynchrone de Puppeteer ---
setTimeout(async () => {
  try {
    console.log("⏳ Preloading Puppeteer...");
    const puppeteer = await import("puppeteer");
    const browser = await puppeteer.launch({ args: ["--no-sandbox"] });
    await browser.close();
    console.log("✅ Puppeteer preloaded successfully!");
  } catch (err) {
    console.warn("⚠️ Puppeteer preload failed:", err.message);
  }
}, 1000);

