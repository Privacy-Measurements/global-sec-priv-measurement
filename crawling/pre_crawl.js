import { asHTTPUrl, isTimeoutError, getRandomSubset, getUniqueUrlsByHref, getAcceptLanguagesValue } from './utils/utils.js';
import { setupEnv } from './utils/setup_env.js';
import { puppeteerConfigForArgs, launchWithRetry } from './pagegraph_crawler_src/puppeteer.js';
import {bucketsPath, shieldsDownChromiumProfileTemplate, preCrawledUrlsPath} from './pagegraph_crawler_src/paths.js';
import { getExecFile, maxCrawls, minCrawls, targetURLs, maxCores } from './pre_crawler_src/settings.js';
import { getCountry } from './utils/crawling_country.js';
import { selectRandomChildUrl } from './utils/page.js';
import {readFile, writeFile, cp, rm, readdir} from 'fs/promises';
import fs from 'fs';
import path from 'path';
import os from 'os';
import sleep from 'await-sleep';


// Base arguments for Puppeteer browser launches
const baseCrawlingArgs = {
    executablePath: getExecFile(),
    userAgent: undefined,
    existingUserDataDirPath: undefined,
    'Accept-Language': 'to_update',
};


const removeAlreadyPrecrawledSites = async (sites, inputBaseName, country) => {

    const existingMergedFile = path.join(preCrawledUrlsPath, `PRECRAWL_${inputBaseName}_${country}_partial.json`);
    const existingResultFile = path.join(preCrawledUrlsPath, `PRECRAWL_${inputBaseName}_${country}.json`);

    let alreadyCrawledFQDNs = new Set();



    if (fs.existsSync(existingMergedFile)) {
        console.log(`Found existing pre-crawl file: ${existingMergedFile}, will skip already crawled sites`);
        const rawData = await readFile(existingMergedFile, 'utf-8');
        const existingData = JSON.parse(rawData);
        for (const fqdn of Object.keys(existingData)) {
            alreadyCrawledFQDNs.add(fqdn);
        } 
    }

    if (fs.existsSync(existingResultFile)) {
        console.log(`Found existing pre-crawl file: ${existingResultFile}, will skip already crawled sites`);
        const rawData = await readFile(existingResultFile, 'utf-8');
        const existingData = JSON.parse(rawData);
        for (const fqdn of Object.keys(existingData)) {
            alreadyCrawledFQDNs.add(fqdn); 
        }
    }

    const filteredSites = sites.filter(site => !alreadyCrawledFQDNs.has(site.fqdn));

    return filteredSites

}

// Initialize a separate browser profile for a pre-crawl worker
const initializePreCrawlProfile = async (coreIndex) => {
    const profileDirPath = path.join(process.cwd(), `precrawling_browser_profile_${coreIndex}`);

    // Remove any old profile 
    await rm(profileDirPath, { recursive: true, force: true });

    // Copy template profile to new location
    await cp(shieldsDownChromiumProfileTemplate, profileDirPath, { recursive: true });
    return profileDirPath;
};

 // Load list of websites to pre-crawl from a JSON file located at ../crux_urls/buckets
async function loadSites(fileName, country=undefined) {
    fileName = path.join(bucketsPath, fileName)
    const rawData = await readFile(fileName, 'utf8');
    let sites = JSON.parse(rawData);

    // Check if the filename contains 'country_coded' or 'country_specific'
    if (fileName.includes('country_coded') || fileName.includes('country_specific')) {
        if (!country) {
            throw new Error("Country must be provided for country-specific site files.");
        }
        const countryKey = country.toLowerCase();
        sites = sites[countryKey] || [];
    }

    return sites
}


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
                        browser = await launchWithRetry(
                                puppeteerConfig,
                        );

                        page = await browser.newPage();
                        await page.setExtraHTTPHeaders({
                            'Accept-Language': crawlingArgs['Accept-Language'],
                        });

                        await page.goto(urlToCrawl, { waitUntil: 'domcontentloaded', timeout: 20000 });

                        await sleep(1000);

                        try {
                            await page.addScriptTag({ path: './resources/consent_banner/consent.js' });
                        } catch (err) {
                            console.log('Could not add script:', err)
                        }

                        
                        await sleep(2000);

                        let childsOfPage = await selectRandomChildUrl(page);
                        if (childsOfPage !== undefined) {
                            allChilds.push(...childsOfPage);
                        }

                        // Remove duplicates
                        allChilds = getUniqueUrlsByHref(allChilds);
                        nbSuccessfullCrawls += 1;

                    } catch (e) {
                        if (isTimeoutError(e)) {
                            console.log(`[Worker ${coreIndex}] Navigation timeout for ${urlToCrawl}`);
                        } else {
                            console.log(`[Worker ${coreIndex}] Error loading ${urlToCrawl}:`, e.message);
                        }
                    }
                    finally {
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

    } 
    catch (e) {
        console.log(e)
    }
};



await (async () => {

    const args = process.argv.slice(2);
    if (args.length < 1) {
        console.error("Usage: node pre_crawl.js <input_file.json>");
        process.exit(1);
    }
    const fileName = args[0];
    const inputBaseName = path.basename(fileName, path.extname(fileName)); 

    
    let country = process.env.COUNTRY_FOR_PRECRAWL;
    if (!country) { 
        country = await getCountry();
    }
    if (!country) {
        console.error("Could not get current country. Aborting.");
        process.exit(1);
    }

    console.log('Country:', country)

    
    const accept_languages = getAcceptLanguagesValue(country)
    baseCrawlingArgs['Accept-Language'] = accept_languages
    console.log('Using Accept-Language', accept_languages)


    let numCores = os.cpus().length / 2; 
    numCores = Math.min(maxCores, numCores);
    console.log(`Detected ${numCores} CPU cores`);


    try {

        // Load list of sites to pre-crawl
        let sites = await loadSites(fileName, country);
        
        sites = await removeAlreadyPrecrawledSites(sites, inputBaseName, country) 
    
        const totalWebsite = sites.length;
        console.log(`########## Pre-Crawling ${fileName}. Number of websites: ${totalWebsite} ##########`);
    
        if (totalWebsite == 0) {
            return;
        }

        // Convert sites into a queue of [fqdn, url] tasks
        const taskQueue = Object.entries(
            Object.fromEntries(sites.map(site => [site.fqdn, site.origin]))
        );

        // Function to fetch next task from queue
        const acquireNextTask = () => {
            if (taskQueue.length === 0) return null;
            return taskQueue.shift();
        };

        // Worker function: each worker/core runs this
        const dynamicWorker = async (coreIndex) => {

            let task;
            let counter = 0;

            const workerResults = {}; 

            // Initialize a dedicated profile for this worker
            await initializePreCrawlProfile(coreIndex)

            // Process tasks until queue is empty
            while ((task = acquireNextTask())) {
                counter++;
                const [fqdn, url] = task;
                try {
                    workerResults[fqdn] = await PreCrawlSite(fqdn, url, coreIndex, counter)
                }
                catch (err) {
                    console.error(`[Worker ${coreIndex}] Fatal error crawling ${fqdn}:`, err);
                    workerResults[fqdn] = 'Error'
                }

                if (counter % 10 === 0) {
                    const intermediateFile = path.join(
                        preCrawledUrlsPath,
                        `PRECRAWL_${inputBaseName}_${country}_core${coreIndex}_partial_${counter}.json`
                    );
                    await writeFile(intermediateFile, JSON.stringify(workerResults, null, 2), 'utf-8');
                    console.log(`[Worker ${coreIndex}] Saved intermediate results at ${counter} sites`);
                }
            }

            return workerResults; 
        }

        const workers = Array.from({ length: numCores }, (_, i) => dynamicWorker(i));
        const workerResultsArray = await Promise.all(workers);

        // Merge all worker results into a single object
        const results = Object.assign({}, ...workerResultsArray);
        
        // Write pre-crawled URLs to a JSON file
        await writeFile(
            path.join(preCrawledUrlsPath, `PRECRAWL_${inputBaseName}_${country}.json`),
            JSON.stringify(results, null, 2),
            'utf-8'
        );
    }
    finally {

        // Cleanup all profiles after crawling is complete
        const cwd = process.cwd();
        const files = await readdir(cwd);

        for (const file of files) {
            if (file.startsWith('precrawling_browser_profile_')) {
                const profileDirPath = path.join(cwd, file);
                await rm(profileDirPath, { recursive: true, force: true });    
            }
        }
    }

})().then(() => {
    process.exit(0);
}).catch((err) => {
    console.error('Fatal error:', err);
    process.exit(1);
});
