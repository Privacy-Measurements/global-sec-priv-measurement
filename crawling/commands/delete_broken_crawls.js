import fs from 'fs-extra';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const snapshotsDir = path.resolve(__dirname, '../../data/snapshots');

// 🔹 Parse command-line arguments
const args = process.argv.slice(2);
const shouldDelete = args.includes('--delete'); // default false
const shouldFix = args.includes('--fix');       // default false

if (shouldDelete && shouldFix) {
    console.error("Error: --delete and --fix cannot be used together.");
    process.exit(1);
}

function countMatching(dir, matchFn) {
    let count = 0;
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
            if (matchFn(entry.name)) {
                count++;
            }
            count += countMatching(fullPath, matchFn);
        }
    }
    return count;
}

function collectFiles(dir, exts) {
    let collected = [];
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
            collected = collected.concat(collectFiles(fullPath, exts));
        } else {
            if (exts.some(ext => entry.name.endsWith(ext))) {
                collected.push(fullPath);
            }
        }
    }
    return collected;
}

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
let fixedCount = 0;

const categoryDirectories = fs.readdirSync(countryDirectory).filter(f => fs.lstatSync(path.join(countryDirectory, f)).isDirectory());

for (const categoryDir of categoryDirectories) {
    const categoryPath = path.join(countryDirectory, categoryDir);
    const siteDirectories = fs.readdirSync(categoryPath).filter(f => fs.lstatSync(path.join(categoryPath, f)).isDirectory());

    for (const siteDirectory of siteDirectories) {
        const sitePath = path.join(categoryPath, siteDirectory);

        // (1) If contains any "browser_profile" dir
        const browserProfileCount = countMatching(sitePath, name => name === 'browser_profile');
        if (browserProfileCount > 0) {
            if (shouldDelete) {
                fs.rmSync(sitePath, { recursive: true, force: true });
                console.log(`Deleted siteDirectory (browser_profile found): ${sitePath}`);
                deletedCount++;
                continue;
            } else if (shouldFix) {
                // Remove only the browser_profile directories
                const entries = fs.readdirSync(sitePath, { withFileTypes: true });
                for (const entry of entries) {
                    if (entry.isDirectory() && entry.name === 'browser_profile') {
                        fs.rmSync(path.join(sitePath, entry.name), { recursive: true, force: true });
                        console.log(`Fixed siteDirectory: removed browser_profile from ${sitePath}`);
                        fixedCount++;
                    }
                }
                keptCount++;
                continue;
            } else {
                console.log(`Would delete siteDirectory (browser_profile found): ${sitePath}`);
                deletedCount++;
                continue;
            }
        }

        // (2) If siteDirectory is empty -> delete
        if (fs.readdirSync(sitePath).length === 0) {
            if (shouldDelete) {
                fs.rmSync(sitePath, { recursive: true, force: true });
            }
            console.log(`${shouldDelete ? 'Deleted' : 'Would delete'} empty siteDirectory: ${sitePath}`);
            deletedCount++;
            continue;
        }

        // (3) Collect .graphml/.graphml.gz files
        const graphmlFiles = collectFiles(sitePath, ['.graphml', '.graphml.gz']);

        if (graphmlFiles.length > 0) {

            // Rule (1): graphml present but no "validation" dir → delete or fix
            
            const validationCount = countMatching(sitePath, name => name === 'validation');
            if (validationCount === 0) {
                if (shouldDelete) {
                    fs.rmSync(sitePath, { recursive: true, force: true });
                    console.log(`Deleted siteDirectory (graphml > 0 but no validation dir): ${sitePath}`);
                } else if (shouldFix) {
                    const validationPath = path.join(sitePath, 'validation');
                    fs.ensureDirSync(validationPath);
                    fs.writeFileSync(path.join(validationPath, 'crawl.log'), '');
                    console.log(`Fixed siteDirectory: created validation/ with crawl.log in ${sitePath}`);
                    fixedCount++;
                } else {
                    console.log(`Would delete siteDirectory (graphml > 0 but no validation dir): ${sitePath}`);
                }
                deletedCount++;
                continue;
            }
 
            const crawlLogs = collectFiles(sitePath, ['crawl.log']);

            // Rule (2): graphml present but crawl.log count != 2 → delete
            if (crawlLogs.length !== 2) {
                if (shouldDelete) {
                    fs.rmSync(sitePath, { recursive: true, force: true });
                }
                console.log(`${shouldDelete ? 'Deleted' : 'Would delete'} siteDirectory (graphml > 0 but crawl.log != 2): ${sitePath}`);
                deletedCount++;
                continue;
            }


        }

        keptCount++;
    }
}

console.log(`\nSummary:`);
console.log(`${shouldDelete ? 'Deleted' : 'Would delete'}: ${deletedCount} siteDirectories`);
console.log(`Kept: ${keptCount} siteDirectories`);
if (shouldFix) {
    console.log(`Fixed: ${fixedCount} siteDirectories`);
}
