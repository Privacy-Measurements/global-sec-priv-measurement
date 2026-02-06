import fs from 'fs-extra';
import path from 'path';
import { fileURLToPath } from 'url';
import archiver from 'archiver';
import os from 'os';
import {maxCores} from '../pagegraph_crawler_src/settings.js';


const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Adjust snapshotsDir
const snapshotsDir = path.resolve(__dirname, '../../data/snapshots');

// Read country + category from CLI
const country = process.argv[2];
const category = process.argv[3];

if (!country || !category) {
  console.error('Usage: node script.js <country> <category>');
  process.exit(1);
}

function countGraphmlFiles(dir) {
  if (!fs.existsSync(dir)) return 0;
  return fs.readdirSync(dir)
    .filter(f => f.endsWith('.graphml') || f.endsWith('.graphml.gz'))
    .length;
}

async function zipDirectory(sourceDir, outPath) {
  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(outPath);
    const archive = archiver('zip', { zlib: { level: 9 } });

    output.on('close', () => resolve());
    archive.on('error', err => reject(err));

    archive.pipe(output);
    archive.directory(sourceDir, false);
    archive.finalize();
  });
}

(async () => {
  const countryDir = path.join(snapshotsDir, country, category);
  if (!fs.existsSync(countryDir)) {
    console.error(`Directory not found: ${countryDir}`);
    process.exit(1);
  }

  const sites = fs.readdirSync(countryDir).filter(f =>
    fs.lstatSync(path.join(countryDir, f)).isDirectory()
  );

  // Build task queue
  const taskQueue = sites.map(siteName => ({ siteName }));

  const acquireNextTask = () => {
    if (taskQueue.length === 0) return null;
    return taskQueue.shift();
  };

  const worker = async (workerId) => {
    let task;
    while ((task = acquireNextTask())) {
      const { siteName } = task;
      const sitePath = path.join(countryDir, siteName);
      const validationPath = path.join(sitePath, 'validation');

      if (!fs.existsSync(validationPath)) {
        console.log(`[W${workerId}] Skipping ${siteName} (no validation folder)`);
        continue;
      }

      const mainCount = countGraphmlFiles(sitePath);
      const validationCount = countGraphmlFiles(validationPath);

//      if (mainCount > 0 && mainCount <= validationCount) {
//      if (mainCount > 0 && validationCount > 0 ) {
        const zipPath = sitePath + '.zip';
        console.log(`[W${workerId}] Zipping ${siteName} -> ${zipPath}`);
        await zipDirectory(sitePath, zipPath);

        // delete folder after zipping
        await fs.remove(sitePath);
//      } else {
        console.log(`[W${workerId}] Skipping ${siteName} (main=${mainCount}, validation=${validationCount})`);
//      }
    }
  };

  // Number of parallel workers

  let numCores = os.cpus().length;
  numCores = Math.min(maxCores, numCores);
  console.log(`Detected ${numCores} CPU cores`);


  const workers = Array.from({ length: numCores }, (_, i) => worker(i));
  await Promise.all(workers);

  console.log('Done.');
})();
