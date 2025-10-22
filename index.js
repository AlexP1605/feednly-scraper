import express from "express";
import puppeteer from "puppeteer";
import * as cheerio from "cheerio";
import axios from "axios";
import pRetry from "p-retry";

const app = express();

// Configuration
const PORT = process.env.PORT || 3000;
const PROXY = process.env.SCRAPER_PROXY || null;
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || "2", 10);

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

// ✅ Lance Chromium intégré (compatible Render)
async function launchBrowser() {
  const browserFetcher = puppeteer.createBrowserFetcher();
  const revisionInfo = await browserFetcher.download('1095492'); // Version stable de Chromium
  return await puppeteer.launch({
    headless: true,
    args: LAUNCH_ARGS,
    executablePath: revisionInfo.executablePath
  });
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

// Scraping via Puppeteer (chargement complet de la page)
async function scrapeWithPuppeteer(url) {
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
      (scripts) => scripts.map((s) => s.innerText).join("\n")
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

// Extraction des données depuis le HTML
function extractFromHtml(html, url, jsonLdText) {
  const $ = cheerio.load(html);

  const title =
    $("meta[property='og:title']").attr("content") ||
    $("meta[name='twitter:title']").attr("content") ||
    $("title").text() ||
    null;

  const description =
    $("meta[property='og:description']").attr("content") ||
    $("meta[name='description']").attr("content") ||
    $("meta[name='twitter:description']").attr("content") ||
    null;

  let price =
    $("meta[property='product:price:amount']").attr("content") ||
    $("meta[name='price']").attr("content") ||
    $('[itemprop="price"]').attr("content") ||
    $('[class*=\"price\"]').first().text() ||
    $('[data-price]').attr("data-price") ||
    null;

  const imgs = new Set();
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
app.get("/scrape", async (req, res) => {
  const url = req.query.url;
  if (!url)
    return res
      .status(400)
      .json({ error: "Missing URL query param (example: /scrape?url=https://...)" });

  try {
    const { html, jsonLd } = await pRetry(() => scrapeWithPuppeteer(url), {
      retries: MAX_RETRIES
    });

    const out = extractFromHtml(html, url, jsonLd);

    // Fallback si aucune image
    if (!out.images || out.images.length === 0) {
      try {
        const fallbackHtml = await fetchWithAxios(url);
        const fallback = extractFromHtml(fallbackHtml, url, null);
        out.images = Array.from(new Set([...(out.images || []), ...(fallback.images || [])]));
        out.title = out.title || fallback.title;
        out.description = out.description || fallback.description;
        out.price = out.price || fallback.price;
      } catch {}
    }

    res.json({ ok: true, ...out });
  } catch (err) {
    console.error("Scrape error:", err.message);
    res
      .status(500)
      .json({ ok: false, error: "Scraping failed", detail: err.message });
  }
});

app.listen(PORT, () => console.log(`✅ Feednly Scraper running on port ${PORT}`));
