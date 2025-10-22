import express from "express";
import puppeteer from "puppeteer";
import * as cheerio from "cheerio";
import axios from "axios";
import pRetry from "p-retry";

const app = express();

// Config via env vars
const PORT = process.env.PORT || 3000;
const PROXY = process.env.SCRAPER_PROXY || null; // ex: "http://username:password@proxy.host:port"
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || "2", 10);
const LAUNCH_ARGS = [
  "--no-sandbox",
  "--disable-setuid-sandbox",
  "--disable-dev-shm-usage",
  "--disable-accelerated-2d-canvas",
  "--disable-gpu"
];

if (PROXY) LAUNCH_ARGS.unshift(`--proxy-server=${PROXY}`);

function normalizeUrl(src, baseUrl) {
  if (!src) return null;
  src = src.trim();
  if (src.startsWith("//")) return "https:" + src;
  if (src.startsWith("/")) {
    try {
      const base = new URL(baseUrl);
      return base.origin + src;
    } catch (e) {
      return src;
    }
  }
  if (!src.startsWith("http")) {
    try {
      const base = new URL(baseUrl);
      return base.origin + "/" + src;
    } catch (e) {
      return src;
    }
  }
  return src;
}

async function fetchWithAxios(url) {
  // fallback simple fetch with headers
  const res = await axios.get(url, {
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
      "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"
    },
    timeout: 20000
  });
  return res.data;
}

async function scrapeWithPuppeteer(url) {
  const browser = await puppeteer.launch({
    headless: true,
    args: LAUNCH_ARGS
  });

  try {
    const page = await browser.newPage();

    // headers and viewport
    await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36");
    await page.setExtraHTTPHeaders({
      "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"
    });
    await page.setViewport({ width: 1366, height: 768 });

    // goto
    await page.goto(url, { waitUntil: "networkidle2", timeout: 30000 });

    // wait for images or body
    await page.waitForTimeout(500); // small wait for dynamic imgs to settle

    // grab HTML
    const html = await page.content();
    // also capture JSON-LD if present
    const jsonLd = await page.$$eval('script[type="application/ld+json"]', scripts =>
      scripts.map(s => s.innerText).join("\n")
    );

    await page.close();
    await browser.close();

    return { html, jsonLd };
  } catch (err) {
    try { await browser.close(); } catch(e) {}
    throw err;
  }
}

function extractFromHtml(html, url, jsonLdText) {
  const $ = cheerio.load(html);

  // title
  let title =
    $("meta[property='og:title']").attr("content") ||
    $("meta[name='twitter:title']").attr("content") ||
    $("title").text() ||
    null;
  title = title ? title.trim() : null;

  // description
  let description =
    $("meta[property='og:description']").attr("content") ||
    $("meta[name='description']").attr("content") ||
    $("meta[name='twitter:description']").attr("content") ||
    null;
  description = description ? description.trim() : null;

  // price: many fallbacks
  let price = null;
  // meta price
  price = price || $("meta[property='product:price:amount']").attr("content");
  price = price || $("meta[name='price']").attr("content");
  price = price || $('[itemprop="price"]').attr("content");
  price = price || $('[class*="price"]').first().text();
  price = price || $('[data-price]').attr("data-price");

  if (price) {
    price = price.toString().replace(/\s+/g, ' ').trim();
  }

  // try JSON-LD parsing for price
  if ((!price || price.length < 1) && jsonLdText) {
    try {
      const j = JSON.parse(jsonLdText);
      // json-ld may be array or object - search recursively
      const findPrice = obj => {
        if (!obj || typeof obj !== 'object') return null;
        if (obj.price) return obj.price;
        if (obj.offers && obj.offers.price) return obj.offers.price;
        for (const k of Object.keys(obj)) {
          const val = obj[k];
          const found = findPrice(val);
          if (found) return found;
        }
        return null;
      };
      const candidate = findPrice(j);
      if (candidate) price = candidate;
    } catch (e) {
      // ignore JSON parse errors
    }
  }

  // images: collect unique, normalized
  const imgs = new Set();
  $("img").each((i, el) => {
    const src = $(el).attr("src") || $(el).attr("data-src") || $(el).attr("data-lazy");
    const urlNorm = normalizeUrl(src, url);
    if (urlNorm && !urlNorm.startsWith("data:")) imgs.add(urlNorm);
  });

  // also check og:image(s)
  const ogImg = $("meta[property='og:image']").attr("content");
  if (ogImg) imgs.add(normalizeUrl(ogImg, url));
  $("meta[property='og:image:secure_url']").each((i, el) => {
    const v = $(el).attr("content"); if (v) imgs.add(normalizeUrl(v, url));
  });

  // fallback: find image urls inside inline CSS background-image
  $('[style]').each((i, el) => {
    const style = $(el).attr('style') || '';
    const m = style.match(/url\(([^)]+)\)/);
    if (m && m[1]) {
      const val = m[1].replace(/['"]/g, '');
      imgs.add(normalizeUrl(val, url));
    }
  });

  // to array and basic filter (width suggestions will be handled client-side)
  const images = Array.from(imgs).filter(Boolean);

  return { title, description, price, images };
}

app.get("/scrape", async (req, res) => {
  const url = req.query.url;
  if (!url) return res.status(400).json({ error: "Missing URL query param (example: /scrape?url=https://...)" });

  try {
    // Use p-retry to attempt a couple times (network, cloudflare transient)
    const { html, jsonLd } = await pRetry(() => scrapeWithPuppeteer(url), {
      retries: MAX_RETRIES,
      onFailedAttempt: err => {
        console.warn(`Attempt ${err.attemptNumber} failed. ${err.retriesLeft} retries left. ${err.message}`);
      }
    });

    const out = extractFromHtml(html, url, jsonLd);
    // If no images found, fallback to simple axios fetch of raw HTML (some sites block puppeteer)
    if ((!out.images || out.images.length === 0)) {
      try {
        const fallbackHtml = await fetchWithAxios(url);
        const fallback = extractFromHtml(fallbackHtml, url, null);
        // merge images keeping unique
        out.images = Array.from(new Set([...(out.images||[]), ...(fallback.images||[])]));
        out.title = out.title || fallback.title;
        out.description = out.description || fallback.description;
        out.price = out.price || fallback.price;
      } catch (e) {
        // ignore fallback error
      }
    }

    // Basic score or meta info
    res.json({
      ok: true,
      url,
      title: out.title,
      description: out.description,
      price: out.price,
      images: out.images
    });
  } catch (err) {
    console.error("Scrape error:", err && err.message ? err.message : err);
    res.status(500).json({ ok: false, error: "Scraping failed", detail: err.message });
  }
});

app.listen(PORT, () => console.log(`Feednly Scraper running on port ${PORT}`));
