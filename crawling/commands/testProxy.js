import puppeteerLib from 'puppeteer-core';
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const proxyPort = 8081;

(async () => {
  console.log(`🌐 Launching browser with proxy http://127.0.0.1:${proxyPort}...`);

  const browser = await puppeteerLib.launch({
    headless: false, // Show browser so you can see errors
    'executablePath' : './resources/pagegraph_brave_build/Static/brave',
    args: [
      `--proxy-server=http://127.0.0.1:${proxyPort}`,
      '--ignore-certificate-errors'
    ]
  });

  const page = await browser.newPage();
  page.setDefaultTimeout(60000); // 60 seconds


  try {
    console.log('🧭 Navigating to Google...');
    await page.goto('https://www.3bmeteo.com/', { waitUntil: 'domcontentloaded' });
//    await page.goto('https://www.google.com/', { waitUntil: 'domcontentloaded' });
    console.log('Successfully loaded Google');
  } catch (err) {
    console.error('Navigation failed:', err);
  }

  await sleep(50000);


//  await page.waitForTimeout(5000); // Keep browser open for 5s
  await browser.close();
})();
