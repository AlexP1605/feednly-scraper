1 file changed
+40
-22
lines changed
Search within code
 
‎server.js‎
+40
-22
Lines changed: 40 additions & 22 deletions
Original file line number	Diff line number	Diff line change
@@ -1,4 +1,4 @@
import express from "express";
import express from "express";
import * as cheerio from "cheerio";
import axios from "axios";
import pRetry from "p-retry";
@@ -65,25 +65,33 @@ async function resolveChromiumExecutable() {
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

// --- Normalisation des URLs d’images ---
@@ -167,21 +175,29 @@ function parseJsonLdScripts(jsonLdScripts) {
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
    await page.goto(url, { waitUntil: "networkidle2", timeout: 40000 });
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
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
@@ -257,16 +273,18 @@ server.on("error", (err) => {
  process.exit(1);
});

// --- Préchargement asynchrone de Puppeteer ---
// --- Préchargement asynchrone de Puppeteer (version Codex optimisée) ---
setTimeout(async () => {
  if (DISABLE_PUPPETEER) {
    console.log("⏭️ Puppeteer preload skipped (disabled via env).");
    return;
  }
  try {
    console.log("⏳ Preloading Puppeteer...");
    const puppeteer = await import("puppeteer");
    const browser = await puppeteer.launch({ args: ["--no-sandbox"] });
    const browser = await launchBrowser();
    await browser.close();
    console.log("✅ Puppeteer preloaded successfully!");
  } catch (err) {
    console.warn("⚠️ Puppeteer preload failed:", err.message);
  }
}, 1000);
