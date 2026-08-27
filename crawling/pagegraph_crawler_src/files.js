import { writeFile } from 'node:fs/promises';
import { join, parse } from 'node:path';
import { gzip } from 'node-gzip';
import { isDir } from '../utils/utils.js';


const dateTimeStamp = Math.floor(Date.now() / 1000);


const createFilename = (url) => {

    const MAX_URL_LENGTH = 100; // maximum length for URL portion of filename

    let fileSafeUrl = String(url).replace(/[^\w]/g, '_');

    if (fileSafeUrl.length > MAX_URL_LENGTH) {
        fileSafeUrl = fileSafeUrl.slice(0, MAX_URL_LENGTH);
    }

    return ['page_graph_', fileSafeUrl, '_', dateTimeStamp].join('');
};



const createOutputPath = (args, url) => {
    if (isDir(args.outputPath) === true) {
        return join(args.outputPath, createFilename(url));
    }
    else {
        const pathParts = parse(args.outputPath);
        return pathParts.dir + '/' + pathParts.name;
    }
};




const createGraphMLPath = (args, url) => {
    
    let outputPath = join(createOutputPath(args, url) + '.graphml');

    if (args.compress === true) {
        outputPath = outputPath + '.gz';
    }

    return outputPath;
};



export const writeGraphML = async (args, url, response, logger) => {
    try {

        const outputFilename = createGraphMLPath(args, url);
        logger.info('Writing PageGraph file to: ', outputFilename);
        const data = args.compress
            ? await gzip(response.data)
            : response.data;
        await writeFile(outputFilename, data);
    }
    catch (err) {
        console.log('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
        console.log(err)
        throw new Error(`Pagegraph file writing error: ${err}`);
    }
};




export const createScreenshotPath = (args, url) => {
    const outputPath = join(createOutputPath(args, url) + '.png');
    return outputPath;
};




const createHARPath = (args, url) => {
    const outputPath = join(createOutputPath(args, url) + '.har');
    return outputPath;
};


export const writeHAR = async (args, url, har, logger) => {
    try {
        const outputFilename = createHARPath(args, url);
        await writeFile(outputFilename, JSON.stringify(har, null, 4));
    }
    catch (err) {
        logger.error('saving HAR file: ', String(err));
    }
};

