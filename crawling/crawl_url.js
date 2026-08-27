import path from 'path';
import { doCrawl } from './pagegraph_crawler_src/crawl.js';
import { measurementDelay, browserExeFilePath, saveScreenshots, storeHar } from './pagegraph_crawler_src/settings.js';
import { setupProxy, importMitmCert } from './pagegraph_crawler_src/mim_cookie_consent.js';
import { getAcceptLanguagesValue } from './utils/utils.js';

const parseArgs = (argv) => {
    const args = {};
    for (let i = 2; i < argv.length; i++) {
        const key = argv[i];
        const val = argv[i + 1];
        if (key.startsWith('--') && val !== undefined && !val.startsWith('--')) {
            args[key.slice(2)] = val;
            i += 1;
        }
    }
    return args;
};

const closeProxySafely = async (proxy) => {
    if (!proxy || typeof proxy.close !== 'function') {
        return;
    }

    await new Promise((resolve) => {
        let settled = false;
        const finish = () => {
            if (!settled) {
                settled = true;
                resolve();
            }
        };

        const timeout = setTimeout(finish, 3000);

        try {
            if (proxy.close.length >= 1) {
                proxy.close(() => {
                    clearTimeout(timeout);
                    finish();
                });
                return;
            }

            const maybePromise = proxy.close();
            if (maybePromise && typeof maybePromise.then === 'function') {
                maybePromise.then(() => {
                    clearTimeout(timeout);
                    finish();
                }).catch(() => {
                    clearTimeout(timeout);
                    finish();
                });
                return;
            }

            clearTimeout(timeout);
            finish();
        } catch (_err) {
            clearTimeout(timeout);
            finish();
        }
    });
};

const crawlUrl = async ({ outputPath, browserProfilePath, url, country, maxAttempts, proxyPort }) => {
    if (!browserExeFilePath) {
        throw new Error('BROWSER_PATH is not set. Please configure it in .env.');
    }
    const outputDirPath = path.resolve(outputPath);
    const profileDirPath = path.resolve(browserProfilePath);
    const site = path.basename(path.dirname(outputDirPath));

    const acceptLanguage = getAcceptLanguagesValue(country);

    const crawlingArgs = {
        executablePath: browserExeFilePath,
        seconds: measurementDelay,
        userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.224 Safari/537.36',
        crawlDuplicates: false,
        screenshot: saveScreenshots,
        storeHar: storeHar,
        storeHarBody: false,
        acceptLanguage: acceptLanguage,
        compress: true,
        proxyServer: `http://127.0.0.1:${proxyPort}`,
        url: url,
        outputPath: outputDirPath,
        existingUserDataDirPath: profileDirPath,
    };

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        console.log(`[crawl_url] Attempt ${attempt}/${maxAttempts} for ${site}: ${url}`);
        try {
            const domContentLoadedTs = await doCrawl(crawlingArgs, [], console);
            console.log(`[crawl_url] Success for ${site}/: ${url}`);
            return { success: true, domContentLoadedTs };
        } catch (err) {
            console.error(`[crawl_url] Attempt ${attempt} failed: ${err.message}`);
        }
    }

    return { success: false, domContentLoadedTs: null };
};

await (async () => {
    let exitCode = 1;
    try {
        // Validate required environment variables
        const requiredEnv = ['BROWSER_PATH', 'MEASUREMENT_DELAY'];
        const missing = requiredEnv.filter(v => !process.env[v]);
        if (missing.length > 0) {
            throw new Error(`Missing required env variables: ${missing.join(', ')}`);
        }

        const args = parseArgs(process.argv);

        // Validate required CLI arguments
        const requiredArgs = ['output_path', 'browser_profile_path', 'url', 'country', 'maxAttempts', 'proxy_port'];
        const missingArgs = requiredArgs.filter(a => !args[a]);
        if (missingArgs.length > 0) {
            throw new Error(`Missing required CLI arguments: ${missingArgs.join(', ')}`);
        }

        const outputPath = args.output_path;
        const browserProfilePath = args.browser_profile_path;
        const url = args.url.trim();
        const country = args.country.trim().toUpperCase();
        const maxAttempts = Number.parseInt(args.maxAttempts, 10);
        const proxyPort = Number.parseInt(args.proxy_port, 10);

        await importMitmCert();
        const proxy = await setupProxy(proxyPort);

        try {
            const result = await crawlUrl({ outputPath, browserProfilePath, url, country, maxAttempts, proxyPort });
            console.log(`[crawl_url_result]${JSON.stringify(result)}`);
            exitCode = result.success ? 0 : 2;
        } finally {
            // Cleanup proxy server
            try {
                await closeProxySafely(proxy);
            } catch (err) {
                console.error('[crawl_url] Error closing proxy:', err.message);
            }
        }
    } catch (err) {
        console.error('[crawl_url] Fatal error:', err.message);
        exitCode = 1;
    } finally {
        process.exitCode = exitCode;
    }
})();


