import express from "express";
import * as cheerio from "cheerio";
import puppeteer from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import axios from "axios";
import fs from "node:fs/promises";
import path from "node:path";
import { performance } from "node:perf_hooks";

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

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms || 0)));
}

function randomItem(list) {
  return list[Math.floor(Math.random() * list.length)];
}

function randomBetween(min, max) {
  return min + Math.random() * (max - min);
}

const { TimeoutError } = puppeteer.errors ?? {};

async function enableRequestOptimizations(page, targetUrl) {
  let targetHostname = null;
  try {
    targetHostname = new URL(targetUrl).hostname;
  } catch {
    targetHostname = null;
  }

  try {
    await page.setRequestInterception(true);
  } catch (err) {
    console.warn("Failed to enable request interception", err.message);
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
      console.warn(`Navigation attempt (${strategy.label}) failed`, err.message);
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

function isLikelyProductImage(url) {
  if (!url) return false;
  const lower = url.toLowerCase();
  if (!/\.(jpe?g|png|webp)(?:$|\?)/.test(lower)) return false;
  if (PLACEHOLDER_KEYWORDS.some((keyword) => lower.includes(keyword))) return false;
  return PRODUCT_IMAGE_KEYWORDS.some((keyword) => lower.includes(keyword));
}

function createHtmlPreview(html, maxLength = 320) {
  if (!html) return "";
  const normalized = `${html}`.replace(/\s+/g, " ").trim();
  if (normalized.length <= maxLength) {
    return normalized;
  }
  return `${normalized.slice(0, maxLength)}…`;
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

  const priceMeta =
    $("meta[property='product:price:amount']").attr("content") ||
    $("meta[name='product:price:amount']").attr("content") ||
    $("meta[itemprop='price']").attr("content") ||
    null;
  let price = priceMeta;
  if (!price) {
    const priceRegex = /(\$|€|£|¥|₹|₩|₽)\s?\d+[\d.,]*/;
    const priceCandidate =
      $("[class*='price'], [id*='price'], span[itemprop='price'], meta[itemprop='price']")
        .toArray()
        .map((el) => {
          const element = $(el);
          return element.attr("content") || element.text();
        })
        .find((text) => text && priceRegex.test(text));
    if (priceCandidate) {
      price = priceCandidate.trim();
    }
  }

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
        const candidate = $(element).attr("content") || $(element).attr("href");
        const normalized = normalizeUrl(candidate, url);
        if (normalized && isLikelyProductImage(normalized)) {
          images.push(normalized);
        }
      });
  }

  $("img")
    .toArray()
    .forEach((element) => {
      const candidate = $(element).attr("src") || $(element).attr("data-src");
      const normalized = normalizeUrl(candidate, url);
      if (normalized && isLikelyProductImage(normalized)) {
        images.push(normalized);
      }
    });

  return { title, description, price: price || null, images: dedupe(images) };
}

function isValidResult(result) {
  return Boolean(result && result.title && Array.isArray(result.images) && result.images.length > 0);
}

async function launchBrowser(proxyUrl) {
  const args = ["--no-sandbox", "--disable-setuid-sandbox"];
  if (proxyUrl) {
    args.push(`--proxy-server=${proxyUrl}`);
  }
  return puppeteer.launch({ headless: "new", args });
}

async function loadCookiesForDomain(domain) {
  if (!domain) return [];
  const cookiePath = path.resolve("cookies", `${domain}_cookies.json`);
  try {
    const raw = await fs.readFile(cookiePath, "utf8");
    const cookies = JSON.parse(raw);
    if (Array.isArray(cookies)) {
      return cookies.filter((cookie) => cookie && typeof cookie === "object");
    }
  } catch (err) {
    if (err.code !== "ENOENT") {
      console.warn("Failed to load cookies", err.message);
    }
  }
  return [];
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
  try {
    browser = await launchBrowser();
    page = await browser.newPage();
    pageSetup = await configurePage(page, url);
    const { userAgent, viewport } = pageSetup;
    console.log("🟢 Stage1 start", { userAgent, viewport });
    const navigationStart = performance.now();
    let navigationMeta = null;
    let navigationError = null;
    try {
      navigationMeta = await navigatePage(page, url);
    } catch (err) {
      navigationError = err;
      lastError = err;
      lastErrorMessage = err.message;
      console.warn("Stage1 navigation error", err.message);
    }
    const navigationDurationSeconds =
      navigationMeta?.durationSeconds ??
      navigationError?.navigationDurationSeconds ??
      roundDuration((performance.now() - navigationStart) / 1000);
    const navigationWaitUntil = navigationMeta?.waitUntil ?? navigationError?.navigationWaitUntil ?? null;
    const navigationTimedOut =
      (navigationMeta && navigationMeta.navigationTimedOut) || Boolean(navigationError?.navigationTimedOut);
    console.log("🟢 Stage1 navigation", {
      waitUntil: navigationWaitUntil,
      durationSeconds: navigationDurationSeconds,
      navigationTimedOut,
    });
    await delay(randomBetween(...HUMAN_DELAY_RANGE));
    const html = await page.content();
    const extracted = extractFromHtmlContent(html, url);
    if (isValidResult(extracted)) {
      const durationSeconds = roundDuration((performance.now() - stageStart) / 1000);
      console.log("🟢 Stage1 success", {
        title: extracted.title || null,
        images: extracted.images?.length || 0,
        durationSeconds,
      });
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
    lastErrorMessage = err.message || lastErrorMessage;
    console.error("🔴 Stage1 error", { message: err.message, stack: err.stack });
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
  return { ok: false, stage: "stage1", error: lastErrorMessage || lastError?.message || "Stage1 failed" };
}

async function runStage2(url) {
  const proxyRaw = process.env.MOBILE_PROXY || "";
  if (!proxyRaw) {
    console.warn("Stage2 skipped: MOBILE_PROXY missing");
    return { ok: false, stage: "stage2", error: "MOBILE_PROXY missing" };
  }
  let proxy;
  try {
    proxy = new URL(proxyRaw);
  } catch (err) {
    console.warn("Stage2 invalid proxy", err.message);
    return { ok: false, stage: "stage2", error: "Invalid MOBILE_PROXY URL" };
  }
  const proxyServer = `${proxy.protocol}//${proxy.hostname}${proxy.port ? `:${proxy.port}` : ""}`;
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
  console.log("🟡 Stage2 using proxy:", proxyRaw);
  for (let attempt = 1; attempt <= 2; attempt += 1) {
    attempts = attempt;
    let browser;
    let page;
    let pageSetup = null;
    try {
      browser = await launchBrowser(proxyServer);
      page = await browser.newPage();
      pageSetup = await configurePage(page, url);
      const { userAgent, viewport } = pageSetup;
      console.log("🟢 Stage2 start", { attempt, userAgent, viewport });
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
          console.warn("Failed to apply cookies", err.message);
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
        lastErrorMessage = err.message;
        console.warn(`Stage2 attempt ${attempt} navigation error`, err.message);
      }
      const navigationDurationSeconds =
        navigationMeta?.durationSeconds ??
        navigationError?.navigationDurationSeconds ??
        roundDuration((performance.now() - navigationStart) / 1000);
      const navigationWaitUntil = navigationMeta?.waitUntil ?? navigationError?.navigationWaitUntil ?? null;
      const navigationTimedOut =
        (navigationMeta && navigationMeta.navigationTimedOut) || Boolean(navigationError?.navigationTimedOut);
      console.log("🟢 Stage2 navigation", {
        attempt,
        waitUntil: navigationWaitUntil,
        durationSeconds: navigationDurationSeconds,
        navigationTimedOut,
      });
      await delay(randomBetween(...HUMAN_DELAY_RANGE));
      const html = await page.content();
      const extracted = extractFromHtmlContent(html, url);
      if (isValidResult(extracted)) {
        const durationSeconds = roundDuration((performance.now() - stageStart) / 1000);
        console.log("🟢 Stage2 success", {
          attempt,
          title: extracted.title || null,
          images: extracted.images?.length || 0,
          durationSeconds,
        });
        return buildSuccessPayload(extracted, {
          stage: "stage2",
          blocked: false,
          fallbackUsed: false,
          durationSeconds,
          network: { durationSeconds },
          attempts: attempt,
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
      lastErrorMessage = err.message || lastErrorMessage;
      console.error("🔴 Stage2 failed", {
        attempt,
        message: err.message,
        stack: err.stack,
      });
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
    console.warn("Stage3 skipped: BRIGHTDATA_API_KEY missing");
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
    console.log("🟡 BrightData request start", { zone: payload.zone, url });
    console.log("🟡 BrightData payload:", JSON.stringify(payload));

    const response = await axios.post(
      "https://api.brightdata.com/request",
      payload,
      { headers, timeout: NAVIGATION_TIMEOUT }
    );

    const responseData = response.data;
    const responseType = Array.isArray(responseData)
      ? "array"
      : responseData === null
      ? "null"
      : typeof responseData;
    console.log("🟢 BrightData response status:", response.status);
    console.log("🟢 BrightData response type:", responseType);
    if (responseData && typeof responseData === "object") {
      console.log("🟢 BrightData response keys:", Object.keys(responseData));
    }
    console.log("🟢 BrightData response preview:", JSON.stringify(responseData).slice(0, 500));

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

    const htmlContent = htmlCandidates.find((candidate) =>
      typeof candidate === "string" && candidate.trim().length > 0
    );

    if (!htmlContent) {
      console.warn("Stage3 empty response body", {
        responseType,
        hasContent: htmlCandidates.some((candidate) => candidate && candidate.length),
      });
      return { ok: false, stage: "stage3", attempts, error: "Empty response body" };
    }

    const htmlPreview = createHtmlPreview(htmlContent);
    console.log("🟡 BrightData HTML preview", {
      length: htmlContent.length,
      preview: htmlPreview,
    });

    const extracted = extractFromHtmlContent(htmlContent, url);

    if (!isValidResult(extracted)) {
      console.warn("Stage3 failed to extract valid product data", {
        htmlLength: htmlContent.length,
        preview: htmlPreview,
      });
      return { ok: false, stage: "stage3", attempts, error: "Invalid BrightData extraction" };
    }

    const durationSeconds = roundDuration((performance.now() - stageStart) / 1000);
    console.log("🟢 BrightData success", {
      title: extracted.title || null,
      images: extracted.images?.length || 0,
      durationSeconds,
    });
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
    console.error("🔴 BrightData error:", err.message);
    console.error("🔴 BrightData response status:", err.response?.status);
    console.error("🔴 BrightData response data:", err.response?.data);
    return {
      ok: false,
      stage: "stage3",
      attempts,
      error: err.message || "BrightData request failed",
    };
  }
}

async function scrapeWithStages(url) {
  if (!url) {
    throw new Error("URL is required");
  }
  console.log("🧩 Starting scrapeWithStages", { url, time: new Date().toISOString() });
  const attemptsSummary = [];
  console.log("➡️ Starting Stage1");
  const stage1 = await runStage1(url);
  attemptsSummary.push({ stage: "stage1", ok: Boolean(stage1?.ok), error: stage1?.error || null });
  if (stage1?.ok) {
    return stage1;
  }
  console.log("➡️ Starting Stage2");
  const stage2 = await runStage2(url);
  attemptsSummary.push({ stage: "stage2", ok: Boolean(stage2?.ok), error: stage2?.error || null });
  if (stage2?.ok) {
    return stage2;
  }
  console.log("➡️ Starting Stage3");
  const stage3 = await runStage3(url);
  attemptsSummary.push({ stage: "stage3", ok: Boolean(stage3?.ok), error: stage3?.error || null });
  if (stage3?.ok) {
    return stage3;
  }
  console.error("❌ All stages failed", { attempts: attemptsSummary });
  return { ok: false, status: "blocked" };
}

app.get("/", (_req, res) => {
  res.json({ ok: true, status: "feednly-scraper", uptime: process.uptime() });
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    uptime: process.uptime(),
    mobileProxyConfigured: Boolean(process.env.MOBILE_PROXY),
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

console.log("🩺 Health configuration", {
  brightDataZone: process.env.BRIGHTDATA_ZONE || null,
  mobileProxyConfigured: Boolean(process.env.MOBILE_PROXY),
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Feednly Scraper listening on port ${PORT}`);
});
