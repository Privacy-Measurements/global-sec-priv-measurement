import fs from 'fs-extra';
import path from 'path';
import {fileURLToPath } from 'url';
import {doCrawl } from './pagegraph_crawler_src/crawl.js';
import {shieldsDownChromiumProfileTemplate} from './pagegraph_crawler_src/paths.js';
import {maxCores, browserExeFilePath, measurementDelay, saveScreenshots, storeHar, proxyPort} from './pagegraph_crawler_src/settings.js';
import os from 'os';
import {setupProxy, importMitmCert} from './pagegraph_crawler_src/mim_cookie_consent.js';


const baseCrawlingArgs = {
    'executablePath': browserExeFilePath,
    'seconds': measurementDelay,
    'userAgent': undefined,
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

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Base path where all sites are stored
const snapshotsDir = path.resolve(__dirname, '../data/snapshots');

//list all site directories
function getSiteDirs(baseDir) {
    return fs.readdirSync(baseDir).filter(f => fs.lstatSync(path.join(baseDir, f)).isDirectory());
}

//list all .graphml files in a directory
function getGraphmlFiles(dir) {
    if (!fs.existsSync(dir)) return [];
    return fs.readdirSync(dir).filter(f => f.endsWith('.graphml')).map(f => path.join(dir, f));
}

//extract URL from a graphml file (stream line-by-line)
async function extractUrlFromGraphml(filePath) {
    return new Promise((resolve, reject) => {
        const readStream = fs.createReadStream(filePath, { encoding: 'utf-8' });
        let urlFound = null;

        readStream.on('data', chunk => {
            const match = chunk.match(/<url>(.*?)<\/url>/);
            if (match) {
                urlFound = match[1];
                readStream.destroy(); // stop reading the rest
            }
        });

        readStream.on('close', () => resolve(urlFound));
        readStream.on('error', err => reject(err));
    });
}

// Initialize snapshot for validation
async function initializeValidationDir(snapshotDirPath) {
    const validationDirPath = path.join(snapshotDirPath, 'validation');
    const profileDirPath = path.join(validationDirPath, 'browser_profile');

    await fs.ensureDir(validationDirPath);
    await fs.cp(shieldsDownChromiumProfileTemplate, profileDirPath, { recursive: true });

    return { validationDirPath, profileDirPath};
}

// Run validation crawl for a set of URLs


async function runValidationCrawl(siteName, urls, snapshotDirPath, coreIndex) {
    if (urls.length === 0) return;

    console.log(`Running validation for ${siteName}: ${urls.length} URLs`);

    const {validationDirPath, profileDirPath} = await initializeValidationDir(snapshotDirPath);

    const crawlingArgs = structuredClone(baseCrawlingArgs);
    crawlingArgs['proxyServer'] = `http://127.0.0.1:${proxyPort + coreIndex}`;
    crawlingArgs['outputPath'] = validationDirPath;
    crawlingArgs['existingUserDataDirPath'] = profileDirPath;


    for (const url of urls) {

        try {

            crawlingArgs.url = url;
            await doCrawl(crawlingArgs, [], console);
        } catch (err) {
            console.error(`Error validating ${url}: ${err}`);
        }
    }

    // Cleanup profile
    await fs.remove(profileDirPath);
}

(async () => {
    const countryDir = path.join(snapshotsDir, 'DE', 'country_specific'); // adjust path
    const sites = getSiteDirs(countryDir);

    const tasks = [];
    for (const siteName of sites) {
        const sitePath = path.join(countryDir, siteName);
        const validationPath = path.join(sitePath, 'validation');
    
        const mainGraphmlFiles = getGraphmlFiles(sitePath);
        const validationGraphmlFiles = getGraphmlFiles(validationPath);
    
        const mainUrls = await Promise.all(mainGraphmlFiles.map(f => extractUrlFromGraphml(f)));
        const validationUrls = new Set(await Promise.all(validationGraphmlFiles.map(f => extractUrlFromGraphml(f))));
    
        const missingUrls = mainUrls.filter(u => u && !validationUrls.has(u));
    

        if ((missingUrls.length > 0) && (validationUrls.size < mainUrls.length)) {

          tasks.push({ siteName, sitePath, missingUrls });
        } else {
        }
    }

    console.log(`Total sites needing validation: ${tasks.length}`);

      // Parallel worker pool
    let numCores = os.cpus().length; 
    numCores = Math.min(maxCores, numCores);
    console.log(`Detected ${numCores} CPU cores`);

    const queue = tasks.slice(); 


    importMitmCert()


    const acquireTask = () => {
        if (queue.length === 0) return null;
        return queue.shift();
    };

    const worker = async (workerId) => {
        let task;
        await setupProxy(proxyPort + workerId)
        while ((task = acquireTask())) {
        const { siteName, sitePath, missingUrls } = task;
        try {
            console.log(`[Worker ${workerId}] Validating site: ${siteName} (${missingUrls.length} urls)`);
            await runValidationCrawl(siteName, missingUrls, sitePath, workerId);
            console.log(`[Worker ${workerId}] Done with ${siteName}`);
        } catch (err) {
            console.error(`[Worker ${workerId}] Error validating ${siteName}:`, err);
        }
        }
    }



    const workers = Array.from({ length: numCores }, (_, i) => worker(i));
    await Promise.all(workers);

    console.log('All validation tasks completed.');


})();
