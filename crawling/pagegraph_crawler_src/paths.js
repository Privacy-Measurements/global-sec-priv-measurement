'use strict';

import path from 'path';
import {existsSync, lstatSync} from 'node:fs';

const __filename = new URL(import.meta.url).pathname;
const __dirname = path.dirname(__filename);

export const crawlerDirPath = path.dirname(__dirname);
export const shieldsDownChromiumProfileTemplate = path.join(crawlerDirPath, 'resources/shields-down-profile/')
export const consentExtensionPath = path.join(crawlerDirPath, 'resources/consent_banner/consent.js'); 
export const logFilePath = path.join(crawlerDirPath, 'crawling.log')
export const rootDirPath = path.dirname(crawlerDirPath);
export const dataDirPath = path.join(rootDirPath, 'data');
export const snapshotsDirPath = path.join(dataDirPath, 'snapshots');
export const compressendWebsitesListsFileName = "compressed_urls.zip"
export const compressedWebsitesListsPath = path.join(rootDirPath, 'crux_urls');
export const preCrawledUrlsPath = path.join(compressedWebsitesListsPath, 'urls_to_crawl');
export const bucketsPath = path.join(compressedWebsitesListsPath, 'buckets');




export const getSnapShotDirPath = (country, category, etld) => {

    let res = path.join(snapshotsDirPath,  country);
    res = path.join(res,  category);
    res = path.join(res, etld)

    return res
} 


export const createFilenameFromUrl = (url) => {
    let fileSafeUrl = String(url).replace(/[^\w]/g, '_');
    fileSafeUrl = fileSafeUrl.slice(0, 100);
    return fileSafeUrl;
};


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
};
