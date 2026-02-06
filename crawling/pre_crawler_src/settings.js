import 'dotenv/config'; // ES module way


export const maxCrawls = 10
export const minCrawls = 3
export const targetURLs = 50
export const maxCores = process.env.MAX_CORES;

export const getExecFile = () => {
    return process.env.BROWSER_FOR_PRECRAWL_PATH;
}

export const headless = false