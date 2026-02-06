import fs from 'fs-extra';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const country = process.argv[2];
const category = process.argv[3];

if (!country || !category) {
    console.error('Usage: requires a country name, and a category of sites <global|country-coded|country>');
    process.exit(1);
}

const fullBasePath = path.resolve(__dirname, '../../data/snapshots/', country, category);

if (!fs.existsSync(fullBasePath) || !fs.lstatSync(fullBasePath).isDirectory()) {
    console.error(`Provided path "${fullBasePath}" is not a valid directory.`);
    process.exit(1);
}

const subDirs = fs.readdirSync(fullBasePath);
const fileCountMap = new Map();


subDirs.forEach(subDir => {
    const subDirPath = path.join(fullBasePath, subDir);

    if (fs.lstatSync(subDirPath).isDirectory()) {
        const contents = fs.readdirSync(subDirPath);
        const graphmlFiles = contents.filter(file => file.endsWith('.graphml') || file.endsWith('.graphml.gz'));

        const count = graphmlFiles.length;

        if (!fileCountMap.has(count)) {
            fileCountMap.set(count, 0);
        }

        fileCountMap.set(count, fileCountMap.get(count) + 1);
    }
});

const sortedKeys = Array.from(fileCountMap.keys()).sort((a, b) => a - b);

console.log('\nGraphML file count per eTLD directory:');
sortedKeys.forEach(count => {
    const suffix = count === 1 ? 'file' : 'files';
    console.log(`Number of eTLDs with ${count} graphml ${suffix} = ${fileCountMap.get(count)}`);
});




let totalSuccessful = 0;
const thresholdSuccess = 5

sortedKeys.forEach(count => {
    if (count > thresholdSuccess) {
        totalSuccessful += fileCountMap.get(count);
    }
});

console.log(`\nTotal number of eTLDs with more than ${thresholdSuccess} graphml files = ${totalSuccessful}`);



