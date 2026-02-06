import fs from 'fs-extra';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const country = process.argv[2];
const category = process.argv[3];
const outputFileName = process.argv[4];

if (!country || !category || !outputFileName) {
    console.error('Usage: node script.js <country> <category> <output.json>');
    process.exit(1);
}

const outputFile = path.resolve(__dirname, '../../crux_urls/urls_to_crawl/', outputFileName);
const basePath = path.resolve(__dirname, '../../data/snapshots/');

// --- Utility functions ---

// list all .graphml files in a directory
function getGraphmlFiles(dir) {
    if (!fs.existsSync(dir)) return [];
    return fs.readdirSync(dir)
        .filter(f => f.endsWith('.graphml'))
        .map(f => path.join(dir, f));
}

// extract URL from a graphml file (stop at first <url> tag found)
async function extractUrlFromGraphml(filePath) {
    return new Promise((resolve, reject) => {
        const readStream = fs.createReadStream(filePath, { encoding: 'utf-8' });
        let urlFound = null;

        readStream.on('data', chunk => {
            const match = chunk.match(/<url>(.*?)<\/url>/);
            if (match) {
                urlFound = cleanUrl(match[1]);
                readStream.destroy(); // stop reading further
            }
        });

        readStream.on('close', () => resolve(urlFound));
        readStream.on('error', err => reject(err));
    });
}

// remove query parameters (and fragment if present)
function cleanUrl(url) {
    try {
        const u = new URL(url);
        u.search = '';  // remove query params
        u.hash = '';    // remove fragments (#...)
        return u.toString();
    } catch {
        return url; // fallback if malformed
    }
}

// --- Build crawl map ---
async function buildCrawlMap(basePath, country, category) {
    const fullPath = path.join(basePath, country, category);

    const etldDirs = fs.readdirSync(fullPath).filter(name => {
        const fullEtldPath = path.join(fullPath, name);
        return fs.lstatSync(fullEtldPath).isDirectory();
    });

    const crawlMap = {};

    for (const etld of etldDirs) {
        const etldPath = path.join(fullPath, etld);


        const files = getGraphmlFiles(etldPath);


        const urls = new Set();
        for (const file of files) {
            try {
                const url = await extractUrlFromGraphml(file);

                if (url) urls.add(url);
            } catch (err) {
                console.error(`Error reading ${file}: ${err.message}`);
            }
        }


        if (urls.size > 0) {
            crawlMap[etld] = Array.from(urls);
        }
    }

    return crawlMap;
}

// --- Run ---
(async () => {
    const crawlIndex = await buildCrawlMap(basePath, country, category);

    await fs.ensureDir(path.dirname(outputFile));
    fs.writeFileSync(outputFile, JSON.stringify(crawlIndex, null, 2));

    console.log(`Crawl index written to ${outputFile}`);
})();
