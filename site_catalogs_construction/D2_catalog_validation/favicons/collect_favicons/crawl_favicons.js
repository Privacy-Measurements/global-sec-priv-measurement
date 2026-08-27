#!/usr/bin/env node

const { chromium, request } = require('playwright');

const STATUS_SUCCESS = 2;
const STATUS_FAILED = -1;

function parseArgs(argv) {
  const out = {
    site: '',
    timeoutSec: 120,
    settleMs: 10000,
  };
  for (let i = 2; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--site') out.site = argv[++i] || '';
    else if (a === '--timeout-sec') out.timeoutSec = Number(argv[++i] || out.timeoutSec);
    else if (a === '--settle-ms') out.settleMs = Number(argv[++i] || out.settleMs);
  }
  return out;
}

async function detectFaviconCandidates(page) {
  return page.evaluate(() => {
    const links = [...document.querySelectorAll('link[rel]')];
    const c = [];
    for (const l of links) {
      const rel = (l.getAttribute('rel') || '').toLowerCase();
      if (!rel.includes('icon')) continue;
      const href = l.getAttribute('href');
      if (!href) continue;
      let score = 0;
      const sizes = (l.getAttribute('sizes') || '').toLowerCase();
      const m = sizes.match(/(\d+)x(\d+)/);
      if (m) score += parseInt(m[1], 10);
      if (rel.includes('shortcut')) score += 5;
      if (rel.includes('apple-touch-icon')) score += 10;
      try {
        c.push({ u: new URL(href, document.baseURI).toString(), s: score });
      } catch (_) {}
    }
    c.sort((a, b) => b.s - a.s);

    const out = [];
    const seen = new Set();
    for (const it of c) {
      if (!seen.has(it.u)) {
        out.push(it.u);
        seen.add(it.u);
      }
    }

    try {
      const ico = new URL('/favicon.ico', location.origin).toString();
      if (!seen.has(ico)) out.push(ico);
    } catch (_) {}

    try {
      const apple = new URL('/apple-touch-icon.png', location.origin).toString();
      if (!seen.has(apple)) out.push(apple);
    } catch (_) {}

    return out;
  });
}

async function fetchFaviconBytes(requestContext, favUrl, timeoutMs) {
  if (favUrl.startsWith('data:')) {
    const m = favUrl.match(/^data:([^;,]+)?(;base64)?,(.*)$/is);
    if (!m) return { ok: false, err: 'invalid_data_url', mime: '' };
    const mime = (m[1] || 'application/octet-stream').toLowerCase();
    try {
      const raw = m[2] ? Buffer.from(m[3], 'base64') : Buffer.from(m[3], 'utf-8');
      return { ok: true, raw, mime };
    } catch (_) {
      return { ok: false, err: 'data_url_decode_failed', mime };
    }
  }

  try {
    const resp = await requestContext.get(favUrl, { timeout: timeoutMs });
    if (resp.status() >= 400) {
      return { ok: false, err: `favicon_http_status:${resp.status()}`, mime: '' };
    }
    const raw = await resp.body();
    const mime = (resp.headers()['content-type'] || '');
    return { ok: true, raw, mime };
  } catch (e) {
    return { ok: false, err: `download_error:${e}`, mime: '' };
  }
}

async function crawlSite(site, timeoutSec, settleMs) {
  const result = {
    status: STATUS_FAILED,
    attempted_url: '',
    homepage_url: '',
    favicon_url: '',
    content_type: '',
    favicon_base64: '',
    error: '',
    homepage_status: null,
    head_html: '',
    fav_candidates: [],
  };

  const timeoutMs = Math.max(1, timeoutSec) * 1000;
  const browser = await chromium.launch({
    headless: true,
    args: [
      '--ignore-certificate-errors',
      '--ignore-ssl-errors',
      '--allow-running-insecure-content',
    ],
  });

  let context;
  let page;
  let requestContext;

  try {
    context = await browser.newContext({ ignoreHTTPSErrors: true });
    requestContext = await request.newContext({ ignoreHTTPSErrors: true });
    page = await context.newPage();

    let ok = false;
    for (const scheme of ['https', 'http']) {
      const u = `${scheme}://${site}`;
      result.attempted_url = u;
      try {
        const resp = await page.goto(u, { waitUntil: 'domcontentloaded', timeout: timeoutMs });
        if (resp) result.homepage_status = resp.status();
        ok = true;
        break;
      } catch (e) {
        if (String(e).includes('Timeout')) result.error = `timeout:${u}`;
        else result.error = `nav_error:${u}:${e}`;
      }
    }

    if (!ok) return result;

    if (settleMs > 0) {
      try {
        await page.waitForTimeout(settleMs);
      } catch (_) {}
      try {
        await page.waitForLoadState('domcontentloaded', { timeout: Math.min(timeoutMs, 5000) });
      } catch (_) {}
    }

    result.homepage_url = page.url();

    try {
      result.head_html = await page.evaluate(() => (document.head ? document.head.outerHTML : ''));
    } catch (_) {}

    try {
      result.fav_candidates = await detectFaviconCandidates(page);
    } catch (_) {
      try {
        await page.waitForLoadState('load', { timeout: Math.min(timeoutMs, 5000) });
        result.fav_candidates = await detectFaviconCandidates(page);
      } catch (e) {
        result.error = `favicon_detect_error:${e}`;
        return result;
      }
    }

    if (!result.fav_candidates.length) {
      result.error = 'no_favicon_found';
      if (result.homepage_status !== null && result.homepage_status >= 400) {
        result.error += `; homepage_http_status:${result.homepage_status}`;
      }
      return result;
    }

    let lastErr = '';
    for (const fav of result.fav_candidates) {
      result.favicon_url = fav;
      const fetched = await fetchFaviconBytes(requestContext, fav, timeoutMs);
      if (!fetched.ok) {
        lastErr = fetched.err || 'favicon_fetch_failed';
        continue;
      }
      if (!fetched.raw || !fetched.raw.length) {
        lastErr = 'empty_body';
        continue;
      }

      result.status = STATUS_SUCCESS;
      result.error = '';
      result.content_type = fetched.mime || '';
      result.favicon_base64 = Buffer.from(fetched.raw).toString('base64');
      return result;
    }

    result.error = lastErr || 'favicon_fetch_failed';
    if (result.homepage_status !== null && result.homepage_status >= 400) {
      result.error += `; homepage_http_status:${result.homepage_status}`;
    }
    return result;
  } finally {
    try { if (requestContext) await requestContext.dispose(); } catch (_) {}
    try { if (page) await page.close(); } catch (_) {}
    try { if (context) await context.close(); } catch (_) {}
    try { await browser.close(); } catch (_) {}
  }
}

async function main() {
  const args = parseArgs(process.argv);
  if (!args.site) {
    console.error('Missing required --site');
    process.exit(2);
  }

  const out = await crawlSite(args.site, args.timeoutSec, args.settleMs);
  process.stdout.write(JSON.stringify(out));
}

main().catch((e) => {
  console.error(`fatal:${e}`);
  process.exit(1);
});
