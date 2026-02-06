import puppeteerLib from 'puppeteer-core';

// Features in Brave to disable when launching the browser
const disabledBraveFeatures = [
    'Speedreader',
    'Playlist',
    'BraveVPN',
    'AIRewriter',
    'AIChat',
    'BravePlayer',
    'BraveDebounce',
    'BraveRewards',
    'BraveSearchOmniboxBanner',
    'BraveGoogleSignInPermission',
    'BraveNTPBrandedWallpaper',
    'AdEvent',
    'NewTabPageAds',
    'CustomNotificationAds',
    'InlineContentAds',
    'PromotedContentAds',
    'TextClassification',
    'SiteVisit',
];

// Chrome features to disable
const disabledChromeFeatures = [
    'IPH_SidePanelGenericMenuFeature',
];

// Combine both Brave and Chrome disabled features into one list
const disabledFeatures = disabledBraveFeatures.concat(disabledChromeFeatures);

// sleep for a given number of milliseconds
const asyncSleep = async (millis) => {
    return await new Promise(resolve => setTimeout(resolve, millis));
};

// Wait time for retries for retries (exponential: 1s, 2s, 4s, ...)
const defaultComputeTimeout = (tryIndex) => {
    return Math.pow(2, tryIndex - 1) * 1000;
};

// Build Puppeteer launch configuration based on arguments
export const puppeteerConfigForArgs = async (args) => {
    
    // Set output directory in environment for PageGraph
    process.env.PAGEGRAPH_OUT_DIR = args.outputPath;


    // browser launch flags
    const chromeArgs = [
        '--ash-no-nudges', //Disables pop-up suggestions for chrome features
        '--disable-brave-update', //Stops Brave from automatically checking for or installing updates.
        '--disable-breakpad', //Disables the crash reporting system that sends crash data to Brave.
        '--disable-component-extensions-with-background-pages', //Stops extensions that run in the background from starting.
        '--disable-component-update', //Prevents automatic updates for components like codecs or extensions.
        '--disable-features=' + disabledFeatures.join(','),
        '--disable-first-run-ui', //Skips Brave’s initial setup or welcome screens.
        '--disable-infobars', //Hides small info bars at the top of the browser window (like “Chrome is being controlled by automated test software”).
        '--disable-ipc-flooding-protection', //Turns off protections against excessive inter-process communication (IPC) between browser processes.
        '--disable-renderer-backgrounding', //Prevents background tabs from being “paused.”
        '--disable-site-isolation-trials', //Disables experimental “site isolation” features that separate sites into different processes. 
        '--disable-sync', //Turns off syncing of bookmarks, passwords, history, etc.
        '--enable-features=PageGraph',
        '--ignore-certificate-errors', //Ignores SSL/TLS certificate warnings.
        '--no-first-run',
        '--no-sandbox', 
        '--disable-setuid-sandbox',
        '-disable-cache',
        '--disable-blink-features=AutomationControlled',
        '--user-data-dir=' + args.existingUserDataDirPath,
    ];

    // Configure proxy if provided
    if (args.proxyServer != null) {
        chromeArgs.push(`--proxy-server=${args.proxyServer.toString()}`);
        if (args.proxyServer.protocol === 'socks5') {
            const socksProxyRule = '--host-resolver-rules=MAP * ~NOTFOUND , EXCLUDE ' + args.proxyServer.hostname;
            chromeArgs.push(socksProxyRule);
        }
    }

    // Append any extra arguments passed in
    if (args.extraArgs != null) {
        chromeArgs.push(...args.extraArgs);
    }

    // Puppeteer launch options
    const puppeteerArgs = {
        defaultViewport: null,
        args: chromeArgs,
        executablePath: args.executablePath,
        headless: false,
    };

    return puppeteerArgs;
};

export const launchWithRetry = async (launchOptions, logger, retryOptions) => {
    // default to 3 retries with a base-2 exponential-backoff delay
    // between each retry (1s, 2s, 4s, ...)

    const retries = retryOptions === undefined ? 3 : +retryOptions.retries;
    const computeTimeout = retryOptions !== undefined ? retryOptions.computeTimeout : defaultComputeTimeout;

    try {
        return puppeteerLib.launch(launchOptions);
    }
    catch (err) {
        logger.info('Failed to launch: ', err, '. ', retries, ' left…');
    }
    for (let i = 1; i <= retries; ++i) {
        await asyncSleep(computeTimeout(i));
        try {
            return puppeteerLib.launch(launchOptions);
        }
        catch (err) {
            logger.info('Failed to launch: ', err, '. ', (retries - i), ' left…');
        }
    }
    throw new Error(`Unable to launch after ${retries} retries!`);
};
