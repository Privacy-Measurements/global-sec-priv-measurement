// This file is just going to be simple checks ot make the crawl.ts code
// easier to read and maintain.

import { existsSync, lstatSync} from 'node:fs';
import { join, sep } from 'node:path';


const countryAcceptLanguages = {
    'US': 'en-US,en;q=0.9',
    'AE': 'ar-AE,ar;q=0.9,en-US;q=0.8,en;q=0.7',
    'IN': 'en-IN,en;q=0.9,hi;q=0.8',
    'DE': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'DZ': 'fr-DZ,fr;q=0.9,ar-DZ,ar;q=0.8,en-US;q=0.7,en;q=0.6'
};


export const asHTTPUrl = (possibleUrl, baseUrl) => {
    try {
        const url = (typeof possibleUrl === 'string')
            ? new URL(possibleUrl, baseUrl)
            : possibleUrl;
        if (!url.protocol.startsWith('http')) {
            return undefined;
        }
        if (url.pathname === '/' && String(url).endsWith('/') === false) {
            url.pathname += '/';
        }
        return url;
    }
    catch (ignore) {
        return undefined;
    }
}

export const isDir = (path) => {
    if (!existsSync(path)) {
        return false;
    }
    const pathStats = lstatSync(path);
    if (pathStats.isDirectory()) {
        return true;
    }
    if (pathStats.isSymbolicLink()) {
        return isDir(join(path, sep));
    }
    return false;
}

export const chunkArray = (array, size) => {
    const chunks = [];
    const entries = Object.entries(array);

    for (let i = 0; i < entries.length; i += size) {
        const chunk = entries.slice(i, i + size);
        chunks.push(Object.fromEntries(chunk));
    }
    return chunks;
}

export const getRandomSubset = (list, size) => {
    const shuffled = [...list].sort(() => 0.5 - Math.random());
    return shuffled.slice(0, size);
}

export const getUniqueUrlsByHref = (urlList) => {

    const seen = new Set();
    const uniqueUrls = [];

    for (const urlObj of urlList) {
        if (!seen.has(urlObj.href)) {
            seen.add(urlObj.href);
            uniqueUrls.push(urlObj);
        }
    }

    return uniqueUrls;
}

export const isTimeoutError = (error) => {
    if (typeof error.name !== 'string') {
        return false;
    }
    return error.name === 'TimeoutError';
}

export const getAcceptLanguagesValue = (country) => {
    return countryAcceptLanguages[country]
}
