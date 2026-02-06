import { readFile } from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// Emulate __dirname in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Get command-line argument
const fileArg = process.argv[2];

if (!fileArg) {
  console.error('Provide the filename as a command-line argument.');
  process.exit(1);
}

const fullPath = path.resolve(__dirname, '../../crux_urls/urls_to_crawl', fileArg);



function countDistinctUrls(data) {
  const countMap = {};

  for (const [etld, urls] of Object.entries(data)) {
    if (!Array.isArray(urls)) {
      console.warn(`Problem: eTLD '${etld}' has a non-array value:`, urls);
      continue;
    }
    const uniqueUrls = new Set(urls);
    countMap[etld] = uniqueUrls.size;
  }

  return countMap;
}

// Aggregates how many eTLDs have the same URL count
function summarizeCounts(countMap) {
  const summary = {};

  for (const count of Object.values(countMap)) {
    summary[count] = (summary[count] || 0) + 1;
  }

  return summary;
}

(async () => {

  const rawJson = await readFile(fullPath, 'utf-8');
  const data = JSON.parse(rawJson);


    const counts = countDistinctUrls(data);
    const summary = summarizeCounts(counts);

    for (const [urlCount, etldCount] of Object.entries(summary).sort((a, b) => a[0] - b[0])) {
      console.log(`  ${etldCount} eTLDs have ${urlCount} distinct URLs`);
    }
    
    const totalEtlds = Object.keys(counts).length;
    const etldsWithAtLeast5 = Object.values(counts).filter(count => count >= 5).length;
    const etldsWithAtLeast2 = Object.values(counts).filter(count => count >= 2).length;
  
    console.log(`Total eTLDs: ${totalEtlds}`);
    console.log(`eTLDs with ≥2 URLs: ${etldsWithAtLeast2}`);

    console.log(`eTLDs with ≥5 URLs: ${etldsWithAtLeast5}`);
  
})();
