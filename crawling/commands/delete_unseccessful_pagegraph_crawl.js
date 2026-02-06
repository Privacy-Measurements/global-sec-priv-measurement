import fs from 'fs-extra';
import path from 'path';
import {fileURLToPath} from 'url';



const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const snapshotsDir = path.resolve(__dirname, '../../data/snapshots');

const args = process.argv.slice(2);
const shouldDelete = args.includes('--delete'); // default = false


if (!fs.existsSync(snapshotsDir)) {
    console.error(`Snapshots dir not found: ${snapshotsDir}`);
    process.exit(1);
}

const contents = fs.readdirSync(snapshotsDir).filter(f => fs.lstatSync(path.join(snapshotsDir, f)).isDirectory());

if (contents.length !== 1) {
    console.error(`Expected exactly one directory inside snapshots/, found: ${contents.length}`);
    process.exit(1);
}

const countryDirectory = path.join(snapshotsDir, contents[0]);

console.log(`Working inside: ${countryDirectory}`);


let deletedCount = 0;
let keptCount = 0;

const categoryDirectories = fs.readdirSync(countryDirectory).filter(f => fs.lstatSync(path.join(countryDirectory, f)).isDirectory());


for (const categoryDir of categoryDirectories) {
    const categoryPath = path.join(countryDirectory, categoryDir);
    const siteDirectories = fs.readdirSync(categoryPath).filter(f => fs.lstatSync(path.join(categoryPath, f)).isDirectory());

    for (const siteDirectory of siteDirectories) {
        const sitePath = path.join(categoryPath, siteDirectory);


        if (fs.lstatSync(sitePath).isDirectory()) {
            const contents = fs.readdirSync(sitePath);
            const graphmlCompressedFiles = contents.filter(file => file.endsWith('.graphml.gz'));
            const graphmFiles = contents.filter(file => file.endsWith('.graphml'));

            if ((graphmFiles.length + graphmlCompressedFiles.length < 1 )) {
                deletedCount += 1

                if(shouldDelete) {
                    fs.rmSync(sitePath, { recursive: true, force: true });
                }
                console.log(`${shouldDelete ? 'Deleted' : 'Would delete'} siteDirectory (no graphml file): ${sitePath}`);                
            }
            else {
                keptCount +=1
            }
        }
    }
};

console.log('Total dirs deleted -- unsuccessful pagegraph crawls :', deletedCount)
console.log('Dirs remaining -- successful pagegraph crawls', keptCount)

