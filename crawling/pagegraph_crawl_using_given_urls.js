import os from 'os';
//import lighthouse from 'lighthouse';
import {cp} from 'node:fs/promises';
import path from 'path';
import fs from 'fs-extra';
import {doCrawl} from './pagegraph_crawler_src/crawl.js';
import {getCountry} from './utils/crawling_country.js'
import {getSnapShotDirPath, shieldsDownChromiumProfileTemplate, preCrawledUrlsPath} from './pagegraph_crawler_src/paths.js';
import {measurementDelay, proxyPort, browserExeFilePath, maxCores, crawlingDepth, saveScreenshots, storeHar} from './pagegraph_crawler_src/settings.js';
import Logger from './pagegraph_crawler_src/logging.js';
import {setupProxy, importMitmCert} from './pagegraph_crawler_src/mim_cookie_consent.js';
import {getAcceptLanguagesValue} from './utils/utils.js';





// Base crawling arguments to reuse for all crawls
const baseCrawlingArgs = {
    'executablePath': browserExeFilePath,
    'seconds': measurementDelay,
    'userAgent': undefined,
    'storeHar': true,             
    'storeHarBody': false,        
    'crawlDuplicates': false,
    'screenshot': saveScreenshots,
    'storeHar': storeHar,
    'Accept-Language': 'to_update',
    'compress': true,
    'proxyServer': undefined,
    'url': 'to_update',
    'outputPath': 'to_update',
    'existingUserDataDirPath': 'to_update'
}



const getCategoryBasedOnFileName = (fileName) => {
    const lowerName = fileName.toLowerCase();

    if (lowerName.includes('global')) {
        return 'global';
    } 
    else if (lowerName.includes('country_specific')) {
        return 'country_specific';
    } 
    else if (lowerName.includes('country_coded')) {
        return 'country_coded';
    } 
    else {
        return null;
    }
};


/**
 * Initializes a snapshot directory for a website.
 * - Copies a fresh browser profile template.
 * - Returns profile path and log file path.
 */
const initializeSnapshotDirectoryAndReturnProfileAndLogFilePath = async (snapshotDirPath) => {

    const profileDirPath = path.join(snapshotDirPath, 'browser_profile');     
    const logFilePath = path.join(snapshotDirPath, 'crawl.log');

    await fs.mkdir(snapshotDirPath, { recursive: true });       
    await cp(shieldsDownChromiumProfileTemplate, profileDirPath, {recursive: true});



    return {profileDirPath, logFilePath}
    
}


/**
 * Filters out websites that have already been crawled.
 * Returns only the remaining sites to process.
 */
const filter_treated_websites = async(inputwebsites, country, category) => {

    const result = {}

    for (let [fqdn, site] of Object.entries(inputwebsites)) {

        const snapshotDirPath = getSnapShotDirPath(country, category, fqdn) 

        if(await fs.pathExists(snapshotDirPath)) {
           continue 
        }
        result[fqdn] = site
    }

    return result
    
}

function normalizeUrl(url) {
  try {
    const u = new URL(url);

    // Lowercase host
    const host = u.host.toLowerCase();

    // Always enforce trailing slash for path
    const path = u.pathname.replace(/\/+$/, "/");

    // Drop query + hash (tracking parameters, etc.)
    return `${u.protocol}//${host}${path}`;
  } catch {
    return url; // fallback if invalid
  }
}


function uniqueNormalizedUrls(urls) {
  const seen = new Set();
  const result = [];

  for (const rawUrl of urls) {
    const normalized = normalizeUrl(rawUrl);
    if (!seen.has(normalized)) {
      seen.add(normalized);
      result.push(normalized);
    }
  }

  return result;
}

/**
 * Crawl a single site.
 * - Tries each URL up to 'crawlingDepth'.
 * - Retries failed URLs up to 2 times.
 * - Cleans up browser profile after crawling.
 * - Returns list of successfully crawled URLs.
 */
const crawlSite = async (site, urls, snapshotDirPath, logFilePath, profileDirPath, coreIndex, justOne = false) => {
    
    const logger = new Logger(logFilePath, coreIndex);

    const crawlingArgs = structuredClone(baseCrawlingArgs);
    crawlingArgs['proxyServer'] = `http://127.0.0.1:${proxyPort + coreIndex}`;
    crawlingArgs['outputPath'] = snapshotDirPath;
    crawlingArgs['existingUserDataDirPath'] = profileDirPath;

    const harDir = path.join(snapshotDirPath, 'har');
    await fs.mkdir(harDir, { recursive: true });
    crawlingArgs.harDir = harDir; 

    let successFullCrawls = [];

    for (const url of urls) {
        if (successFullCrawls.length === crawlingDepth) {
            logger.info(`Successfuly completing site: ${site}`)
            break;
        }

        try {
            logger.info(`Crawling URL: ${url}`)
            crawlingArgs.url = url;
            await doCrawl(crawlingArgs, [], logger); 
            successFullCrawls.push(url);
            
            if(justOne) {
                break
            }
        }    
        catch (error) {
            logger.info(`Error crawling ${url}: ${error}`);
            await new Promise(res => setTimeout(res, 1000));
        }     
    }

    // Clean up the browser profile after crawling
    try {
        await new Promise((res) => setTimeout(res, 1000));
        await fs.remove(profileDirPath);
    } 
    catch (err) {
        logger.info(`Failed to clean profile for ${site}: ${err}`);
    }

    return successFullCrawls;
};



export function getExcludedSites(fileName) {
    const filePath = path.join(preCrawledUrlsPath, fileName);

    if (!fs.existsSync(filePath)) {
        console.warn(`Exclude file not found: ${filePath}`);
        return new Set();
    }

    const lines = fs.readFileSync(filePath, 'utf-8').split('\n').map(l => l.trim());
    // Skip header "etld" and empty lines
    const domains = lines.slice(1).filter(l => l.length > 0);

    return new Set(domains);
}

await (async () => {

    let urlsFile;
    let excludeFile;

    process.argv.slice(2).forEach((arg, index, arr) => {
        if (arg === "--urlsFile") {
            urlsFile = arr[index + 1];
        } else if (arg === "--excludeFile") {
            excludeFile = arr[index + 1];
        }
    });

    if (!urlsFile) {
        console.error('You must provide --urlsFile <filename>');
         process.exit(1);
    }

    const category = getCategoryBasedOnFileName(urlsFile)

    if (!category) {
        console.error('Could not extract the category/bucket from the file_name.');
        process.exit(1);
    }

     // Get the machine's country -- using the IP address
     let country = process.env.COUNTRY_FOR_PRECRAWL;
     if (!country) { 
         country = await getCountry();
     }
     if (!country) {
         console.error("Could not get current country. Aborting.");
         process.exit(1);
     }
 

    // Read crawling input from JSON file. Its a json_file containing the results of a precrawl. Keys are domains (fqdn), and values are a list of URLs within the FQDN. Grouped by websites' category (bucket)
    urlsFile = path.join(preCrawledUrlsPath, urlsFile);
    let crawlingInput = JSON.parse(fs.readFileSync(urlsFile, 'utf-8'));

    crawlingInput = await filter_treated_websites(crawlingInput, country, category)


    if (excludeFile) {
        const excludedSites = getExcludedSites(excludeFile);
    
        crawlingInput = Object.fromEntries(
            Object.entries(crawlingInput)
                  .filter(([fqdn]) => !excludedSites.has(fqdn))
        );
        console.log(`After excluding, ${Object.keys(crawlingInput).length} sites remain for crawling.`);
    }


    const accept_languages = getAcceptLanguagesValue(country)
    baseCrawlingArgs['Accept-Language'] = accept_languages
    console.log('Using Accept-Language', accept_languages)


    
    // Number of parallel crawling instances / browser sessions
    let numCores = os.cpus().length;
    numCores = Math.min(maxCores, numCores);
    console.log(`Detected ${numCores} CPU cores`);

    // Import MITM certificate for proxy interception
    importMitmCert()



    const totalWebsites = Object.keys(crawlingInput).length;
    console.log(`########## Crawling websites of category: ${category} for country: ${country} ##########. Number of websites: ${totalWebsites}$##########`);

    // Convert websites object to a task queue array
    const taskQueue = Object.entries(crawlingInput);


    // Get the next site from the queue
    const acquireNextTask = () => {
        if (taskQueue.length === 0) return null;
        return taskQueue.shift();
    };


    /**
     * Worker function to crawl sites.
     * - Sets up a proxy for the worker
     * - Pulls next site from the task queue
     * - Runs two crawls for each site (for validation purposes)
     */
    const dynamicWorker = async (coreIndex) => {

        await setupProxy(proxyPort + coreIndex)
        
        let task;
        let counter = 0;

        while ((task = acquireNextTask())) {

            counter++;
            let [fqdn, urls] = task;

            const snapshotDirPath = getSnapShotDirPath(country, category, fqdn);

            if (await fs.pathExists(snapshotDirPath)) {
                continue;
            }

            urls = uniqueNormalizedUrls(urls)
            // ********************************* First crawl ********************************* 
            const { profileDirPath, logFilePath } = await initializeSnapshotDirectoryAndReturnProfileAndLogFilePath(snapshotDirPath);
            const successful_urls = await crawlSite(fqdn, urls, snapshotDirPath, logFilePath, profileDirPath, coreIndex)


            if (successful_urls.length === 0) {
                continue;
            }

            // ********************************* validation crawl ********************************* 
            const validationDirPath = path.join(snapshotDirPath,  'validation'); 
            const { profileDirPath: validationProfileDirPath, logFilePath: validationLogFilePath } = await initializeSnapshotDirectoryAndReturnProfileAndLogFilePath(validationDirPath);
            await crawlSite(fqdn, successful_urls, validationDirPath, validationLogFilePath, validationProfileDirPath, coreIndex)
        }

    }
    const workers = Array.from({ length: numCores }, (_, i) => dynamicWorker(i));
    await Promise.all(workers);

    
})().then(() => {
  process.exit(0); 
}).catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});;


