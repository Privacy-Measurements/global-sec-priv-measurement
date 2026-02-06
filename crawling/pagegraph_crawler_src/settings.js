'use strict';
import dotenv from 'dotenv';
import path from 'path';
import fs from 'fs';


function findEnvFile(startDir = process.cwd()) {
  let dir = startDir;
  while (dir !== path.parse(dir).root) {
    const envPath = path.join(dir, '.env');
    if (fs.existsSync(envPath)) {
      return envPath;
    }
    dir = path.dirname(dir); // go up one level
  }
  return null; // not found
}

const envFilePath = findEnvFile();
if (envFilePath) {
  dotenv.config({ path: path.resolve(envFilePath) });
} else {
  console.warn('.env file not found in any parent directory');
}



export const measurementDelay = parseInt(process.env.MEASUREMENT_DELAY, 10)// in s -- We have 5 seconds delay by default (in addition to the value mentioned here)
export const pageGraphTimeout = 1000 * parseInt(process.env.PAGEGRAPH_TIMEOUT, 10)
export const navigationTimeout = 1000 * parseInt(process.env.NAVIGATION_TIMEOUT, 10)
export const maximumRank = 10000
export const crawlingDepth = parseInt(process.env.CRAWLING_DEPTH, 10)
export const proxyPort = parseInt(process.env.PROXY_PORT, 10);
export const browserExeFilePath = process.env.BROWSER_PATH;
export const maxCores = parseInt(process.env.MAX_CORES, 10);
export const saveScreenshots = process.env.SAVE_SCREENSHOTS == 'true'
export const storeHar = process.env.STORE_HAR == 'true'

