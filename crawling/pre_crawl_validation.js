import { asHTTPUrl, isTimeoutError, getRandomSubset, getUniqueUrlsByHref, getAcceptLanguagesValue } from './utils/utils.js';
import { setupEnv } from './utils/setup_env.js';
import { puppeteerConfigForArgs, launchWithRetry } from './pagegraph_crawler_src/puppeteer.js';
import { preCrawledUrlsPath, shieldsDownChromiumProfileTemplate } from './pagegraph_crawler_src/paths.js';
import { getExecFile, maxCrawls, minCrawls, targetURLs, maxCores} from './pre_crawler_src/settings.js';
import { getCountry } from './utils/crawling_country.js';
import { selectRandomChildUrl } from './utils/page.js';
import { readFile, writeFile, cp, rm, readdir } from 'fs/promises';
import fs from 'fs';
import path from 'path';
import os from 'os';
import sleep from 'await-sleep';


// Base Puppeteer args
const baseCrawlingArgs = {
    executablePath: getExecFile(),
    userAgent: undefined,
    existingUserDataDirPath: undefined,
    'Accept-Language': 'to_update',
};

const getTime = () => {
    return new Date().toLocaleTimeString('en-GB', { hour12: false }); // HH:MM:SS
};


// Initialize a separate browser profile for a pre-crawl worker
const initializePreCrawlProfile = async (coreIndex) => {
    const profileDirPath = path.join(process.cwd(), `precrawling_browser_profile_${coreIndex}`);

    // Remove any old profile 
    await rm(profileDirPath, { recursive: true, force: true });

    // Copy template profile to new location
    await cp(shieldsDownChromiumProfileTemplate, profileDirPath, { recursive: true });
    return profileDirPath;
};




export const filterAlreadyProcessedSites = async (sitesData, inputBaseName) => {

    let alreadyProcessed = new Set();

    const partialFile = `${inputBaseName}_validation_partial.json`;
    const partialFilePath = path.join(preCrawledUrlsPath, partialFile)

    if (fs.existsSync(partialFilePath)) {
        const partialData = JSON.parse(await readFile(partialFilePath, 'utf-8'));
        Object.keys(partialData).forEach(fqdn => alreadyProcessed.add(fqdn));
        console.log(`Skipping ${alreadyProcessed.size} already processed sites from partial file.`);
    } 
    else {
        console.log('No partial file found, processing all sites.');
    }

    const filteredSitesData = Object.fromEntries(
        Object.entries(sitesData)
            .filter(([fqdn, urls]) => Array.isArray(urls) && urls.length <= 1 && !alreadyProcessed.has(fqdn))
    );

    console.log(`Found ${Object.keys(filteredSitesData).length} sites to re-process.`);
    return filteredSitesData;
};


// Function to pre-crawl one site
// - fqdn: full domain name 
// - url: top-level url
// - coreIndex: index of the core
const PreCrawlSite = async (fqdn, url, coreIndex, counter) => {
    const crawlingArgs = structuredClone(baseCrawlingArgs);
    crawlingArgs['existingUserDataDirPath'] = path.join(process.cwd(), `precrawling_browser_profile_${coreIndex}`)

    // Get Puppeteer configuration for launching browser
    const puppeteerConfig = await puppeteerConfigForArgs(crawlingArgs);

    // Remove PageGraph argument
    puppeteerConfig.args = puppeteerConfig.args.filter(
        arg => arg !== '--enable-features=PageGraph'
    );

    try {
        let attempt = 0;
        let success = false;
        let finalResult = [];

        // Retry loop: try up to 5 times if only one URL is found
        while (attempt < 5 && !success) {

            attempt++;
            console.log(`[Worker ${coreIndex}] Attempt ${attempt} for ${fqdn}, count ${counter}`);

            try {

                const siteToCrawl = asHTTPUrl(url);
                let allChilds = [siteToCrawl];
                let nbSuccessfullCrawls = 0;
                let crawlingIndex = 0;

                while (true) {

                    if ((crawlingIndex === maxCrawls) || (nbSuccessfullCrawls >= minCrawls && allChilds.length > targetURLs)) {
                        break;
                    }
                    if (crawlingIndex === allChilds.length) {
                        break;
                    }


                    let envHandle;
                    let browser;
                    let page;

                    const urlToCrawl = allChilds[crawlingIndex];
                    try {

                        envHandle = setupEnv(crawlingArgs);
                        browser = await launchWithRetry(puppeteerConfig);


                        page = await browser.newPage();

                        await page.setExtraHTTPHeaders({
                            'Accept-Language': crawlingArgs['Accept-Language'],
                        });


                        await page.goto(urlToCrawl, { waitUntil: 'domcontentloaded', timeout: 60000 });

                        await sleep(3000);

                        try {
                            await page.addScriptTag({ path: './resources/consent_banner/consent.js' });
                        } catch (err) {
                            console.log(err)
                        }  


                        await sleep(10000);


                        let childsOfPage = await selectRandomChildUrl(page);

                        if (childsOfPage !== undefined) {
                            allChilds.push(...childsOfPage);
                        }

                        allChilds = getUniqueUrlsByHref(allChilds);

                        nbSuccessfullCrawls += 1;

                    } catch (e) {
                        if (isTimeoutError(e)) {
                            console.log(`[Worker ${coreIndex}] Navigation timeout for ${urlToCrawl}`);
                        } else {
                            console.log(`[Worker ${coreIndex}] Error loading ${urlToCrawl}:`, e.message);
                        }
                    } finally {
                        try {
                            if (page && !page.isClosed()) await page.close();
                        } catch (e) {
                            console.log(`[Worker ${coreIndex}] Error closing page:`, e.message);
                        }
                        try {
                            if (browser) await browser.close();
                        } catch (e) {
                            console.log(`[Worker ${coreIndex}] Error closing browser:`, e.message);
                        }
                        try {
                            if (envHandle) await envHandle.close();
                        } catch (e) {
                            console.log(`[Worker ${coreIndex}] Error closing envHandle:`, e.message);
                        }
                    }
                    crawlingIndex += 1;
                }


                // Keep the first URL plus up to 49 randomly selected child URLs (the first one is the home page, so we keep it)
                const [first, ...rest] = allChilds;
                finalResult = [first, ...getRandomSubset(rest, 49)];

                if (finalResult.length > 1) {
                    success = true;
                } else {
                    console.log(`[Worker ${coreIndex}] Only 1 URL found for ${fqdn}, retrying...`);
                    await sleep(2000); // small delay before retry
                }

            } catch (error) {
                console.log(`[Worker ${coreIndex}] Error crawling ${fqdn} on attempt ${attempt}:`, error);
                await sleep(2000); // wait before retry
            }
        }

        if (!success && finalResult.length <= 1) {
            console.log(`[Worker ${coreIndex}] Failed to get more than 1 URL for ${fqdn} after 5 attempts`);
        }

        return finalResult.length > 0 ? finalResult : { error: 'No URLs found after retries' };

    } catch (e) {
        console.log(e)
    }
};


// Main
await (async () => {
    const args = process.argv.slice(2);
    if (args.length < 1) {
        console.error("Usage: node re_precrawl.js <input_file.json>");
        process.exit(1);
    }

    // Always resolve relative to preCrawledUrlsPath
    const inputFileName = args[0];
    const filePath = path.join(preCrawledUrlsPath, inputFileName);
    const inputBaseName = path.basename(inputFileName, path.extname(inputFileName)); 

    // Confirm file exists
    if (!fs.existsSync(filePath)) {
        console.error(`Input file not found at: ${filePath}`);
        process.exit(1);
    }

    let country = process.env.COUNTRY_FOR_PRECRAWL;
    if (!country) { 
        country = await getCountry();
    }
    if (!country) {
        console.error("Could not get current country. Aborting.");
        process.exit(1);
    }

    console.log('Country:', country)

    const accept_languages = getAcceptLanguagesValue(country);
    baseCrawlingArgs['Accept-Language'] = accept_languages;
    console.log('Using Accept-Language', accept_languages);

    // Load A.json
    const rawData = await readFile(filePath, 'utf-8');
    let sitesData = JSON.parse(rawData);

    sitesData = await filterAlreadyProcessedSites(sitesData, inputBaseName)

    let numCores = os.cpus().length / 2; 
    numCores = Math.min(maxCores, numCores);
    console.log(`Running with ${numCores} workers`);

    // Queue: only sites with 0–1 URL
    const taskQueue = Object.entries(sitesData)
        .filter(([fqdn, urls]) => Array.isArray(urls) && urls.length <= 1);

    console.log(`Found ${taskQueue.length} sites to re-process (only 1 URL found in pre-crawl).`);

    const acquireNextTask = () => taskQueue.length === 0 ? null : taskQueue.shift();

    const worker = async (coreIndex) => {
        let counter = 0;
        const workerResults = {};

        await initializePreCrawlProfile(coreIndex);

        let task;
        while ((task = acquireNextTask())) {
            counter++;
            const [fqdn, urls] = task;
            workerResults[fqdn] = await PreCrawlSite(fqdn, urls[0], coreIndex, counter);


            if (counter % 25 === 0) {
                const intermediateFile = path.join(
                    preCrawledUrlsPath,
                    `${inputBaseName}_validation_core${coreIndex}_partial_${counter}.json`
                );
                await writeFile(intermediateFile, JSON.stringify(workerResults, null, 2), 'utf-8');
                console.log(`[Worker ${coreIndex}] Saved intermediate results at ${counter} sites`);
            }

        }
        return workerResults;
    };

    // Run workers
    const workers = Array.from({ length: numCores }, (_, i) => worker(i));
    const workerResultsArray = await Promise.all(workers);
    const fixedSites = Object.assign({}, ...workerResultsArray);

    // Merge fixed sites back into the original data
    const mergedResults = { ...sitesData, ...fixedSites };

    // Save validation file in same folder
    const outFile = path.join(
        preCrawledUrlsPath,
        `${inputBaseName}_validation.json`
    );
    await writeFile(outFile, JSON.stringify(mergedResults, null, 2), 'utf-8');
    console.log(`Wrote validated file to ${outFile}`);

    // Cleanup profiles
    const files = await readdir(process.cwd());
    for (const file of files) {
        if (file.startsWith('reprecrawl_profile_')) {
            await rm(path.join(process.cwd(), file), { recursive: true, force: true });
        }
    }

    process.exit(0);
})();
