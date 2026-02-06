
import Timeout from 'await-timeout';
import {asHTTPUrl, isTimeoutError} from '../utils/utils.js';
import {setupEnv} from '../utils/setup_env.js'
import {createScreenshotPath, writeGraphML, writeHAR} from './files.js';
import {puppeteerConfigForArgs, launchWithRetry} from './puppeteer.js';
import {harFromMessages} from 'chrome-har';
import {pageGraphTimeout, navigationTimeout} from './settings.js';
import sleep from 'await-sleep';


function withTimeout(promise, ms, errorMessage = "Timeout exceeded") {
    return Promise.race([
        promise,
        new Promise((_, reject) =>
            setTimeout(() => reject(new Error(errorMessage)), ms)
        )
    ]);
}


// Repeatedly checks a condition until either:
// - the given time (secs) has passed, OR
// - the `unlessFunc` returns true.
// Returns true if it stopped because of timeout, false if because `unlessFunc`.
const waitUntilUnless = (secs, unlessFunc, intervalMs = 500) => {
    const totalMs = secs * 1000;
    const endTime = Date.now() + totalMs;
    return new Promise((resolve) => {
        const timerId = setInterval(() => {
            const hasTimePassed = Date.now() > endTime;
            const unlessFuncRs = unlessFunc();
            const shouldEnd = hasTimePassed === true || unlessFuncRs === true;
            if (shouldEnd === true) {
                clearTimeout(timerId);
                const returnedBcTimeout = hasTimePassed === true;
                resolve(returnedBcTimeout);
            }
        }, intervalMs);
    });
};



// Prepares Chrome DevTools Protocol (CDP) listeners for HAR generation.
// It captures network and page events, and optionally stores response bodies.
const prepareHARGenerator = async (client, networkEvents, pageEvents, storeHarBody, responseBodies, logger) => {
    await client.send('Page.enable');
    await client.send('Network.enable');

    // Listen to network-related events
    const networkMethods = [
        'Network.requestWillBeSent',
        'Network.requestServedFromCache',
        'Network.dataReceived',
        'Network.responseReceived',
        'Network.resourceChangedPriority',
        'Network.loadingFinished',
        'Network.loadingFailed',
    ];

    // Listen to page-related events
    const pageMethods = [
        'Page.loadEventFired',
        'Page.domContentEventFired',
        'Page.frameStartedLoading',
        'Page.frameAttached',
        'Page.frameScheduledNavigation',
    ];

    // Capture network events and response bodies
    networkMethods.forEach((method) => {
        client.on(method, (params) => {
            networkEvents.push({ method, params });
            if (storeHarBody && method == 'Network.loadingFinished') {
                const responseParams = params;
                const requestId = responseParams.requestId;
                client.send('Network.getResponseBody', { requestId: requestId })
                    .then((responseBody) => {
                    responseBodies.set(requestId.toString(), responseBody);
                }, (reason) => {                    
                    logger.info('LoadingFinishedError: ' + reason);
                });
            }
        });
    });

    // Capture page events
    pageMethods.forEach((method) => {
        client.on(method, (params) => {
            pageEvents.push({ method, params });
        });
    });
};


// Generates a page graph using Chrome DevTools protocol.
// Waits for seconds, then requests `Page.generatePageGraph`.
// Times out if the page graph capture takes too long.
const generatePageGraph = async (seconds, page, client, waitFunc, logger) => {

    await waitUntilUnless(seconds, waitFunc);
    const pageGraphTimer = new Timeout();

    try {
        const response = await Promise.race([
            client.send('Page.generatePageGraph'),
            pageGraphTimer.set(pageGraphTimeout, 'Page graph capture timed out')
        ]);

        pageGraphTimer.clear();

        const responseLen = response.data.length;
        return response
      } 
      catch (error) {
        pageGraphTimer.clear();
        throw new Error(`Page graph timeout`);
    } 

      
};


// Converts collected network/page events into a HAR file and writes it to disk.
const exportHAR = async(networkEvents, pageEvents, responseBodies, args, urlToCrawl, logger) => {

    await Promise.all(responseBodies);
    for (const event of networkEvents) {
        if (!args.storeHarBody) {
            break;
        }

        if (event.method !== 'Network.responseReceived') {
            continue;
        }


        const requestId = event.params.requestId;
        const responseBody = responseBodies.get(requestId.toString());
        const responseParams = event.params;
        if (!responseBody) {
            responseParams.response.body = undefined;
            continue;
        }

        const responseBodyEncoding = responseBody.base64Encoded
            ? 'base64'
            : undefined;
        const responseBodyBuffer = Buffer.from(responseBody.body, responseBodyEncoding);
        responseParams.response.body = responseBodyBuffer.toString();
    }

    const allEvents = pageEvents.concat(networkEvents);

    const har = harFromMessages(allEvents, {
        includeTextFromResponseBody: args.storeHarBody,
    });

    await writeHAR(args, urlToCrawl, har, logger);

}

// Wraps `exportHAR` with a timeout 
const exportHARTimeOut = async(networkEvents, pageEvents, responseBodies, args, urlToCrawl, logger) => {

    const harTimeout = new Timeout();

    try {
        await Promise.race([
            exportHAR(networkEvents, pageEvents, responseBodies, args, urlToCrawl, logger),
            harTimeout.set(30000, 'export HAR timed out')
        ]);

        harTimeout.clear();
        return;
      } 
      catch (error) {
        harTimeout.clear();
      //  console.log(error)
        throw new Error(`HAR export timeout`);
    } 


}



function normalizeUrl(url) {
  try {
    const u = new URL(url);
    // Keep only origin + pathname
    return u.origin + u.pathname.replace(/\/+$/, '/'); // ensure trailing slash
  } catch (e) {
    return url; // fallback to raw if invalid
  }
}



async function safePageClose(page, logger, timeoutMs = 5000) {
    try {
        await Promise.race([
            page.close(),
            new Promise((_, reject) => setTimeout(() => reject(new Error('page.close() timeout')), timeoutMs))
        ]);
    } catch (e) {
        logger.info(`Force disposing page: ${e.message}`);
        try {
            const target = page.target();
            const client = await target.createCDPSession();
            await client.send('Target.closeTarget', { targetId: target._targetId });
        } catch (killErr) {
            logger.info(`Failed to force close page: ${killErr.message}`);
        }
    }
}


// Main function to crawl a given URL.
// 1. Launches a browser instance.
// 2. Prepares environment, CDP session, and listeners.
// 3. Navigates to the target URL.
// 4. Captures page graph, HAR, and screenshot.
// 5. Handles redirections, errors, and cleanup.
export const doCrawl = async (args, redirectChain, logger) => {

    const url = args.url
    const normalizedUrl = normalizeUrl(url);


    if (normalizedUrl && !redirectChain.includes(normalizedUrl)) {
        redirectChain = [...redirectChain, normalizedUrl];
    }


    let shouldRedirectToUrl; 
    let shouldReturnError; 

    const urlToCrawl = asHTTPUrl(args.url);
    const launchOptions = await puppeteerConfigForArgs(args); 
    const envHandle = setupEnv();

    let shouldStopWaitingFlag = false;
    const shouldStopWaitingFunc = () => {
        return shouldStopWaitingFlag;
    };

    try {

        const browser = await launchWithRetry(launchOptions, logger); 
        await sleep(1000)

        // Close any default pages opened by Puppeteer
        try {

            const pages = await browser.pages();
            if (pages.length > 0) {
                for (const aPage of pages) {
                    await aPage.close();
                }
            }
        } catch (pageListErr) {
            logger.info(`Could not list/close default pages: ${pageListErr.message}`);
        }
        try {
            const page = await browser.newPage();
            await sleep(1000)
            page.setDefaultTimeout(60000);

            await page.setCacheEnabled(false);
            await page.setExtraHTTPHeaders({
                'Accept-Language': args['Accept-Language'],
            });

            const client = await page.target().createCDPSession();
            const networkEvents = [];
            const pageEvents = [];
            const responseBodies = new Map();
            
            // Prepare HAR collection if requested
            await prepareHARGenerator(client, networkEvents, pageEvents, true, responseBodies, logger); 
            

            // Crash handling
            client.on('Target.targetCrashed', (event) => {
                const logMsg = {
                    targetId: event.targetId,
                    status: event.status,
                    errorCode: event.errorCode,
                };
                logger.error(`Target.targetCrashed ${JSON.stringify(logMsg)}`);
                throw new Error(event.status);
            });

            
            // Apply user agent override
            if (args.userAgent !== undefined) {
                await page.setUserAgent(args.userAgent); //discuss this next meeting
            }

            // Enable request interception to detect redirects
            await page.setRequestInterception(true);

            let firstLoad = true

            page.on('request', async (request) => {

                //This is a redirection request, must stop it because it brakes pagegraph
                if (!firstLoad && request.isNavigationRequest() && request.frame() !== null && request.frame().parentFrame() === null) {

                    const requestUrl = request.url()
                    const normalizedUrl = normalizeUrl(requestUrl)

                    if (!redirectChain.includes(normalizedUrl)) {
                        shouldRedirectToUrl = requestUrl
                        redirectChain.push(normalizedUrl);
                        shouldStopWaitingFlag = true
                    } 

                    
                    try {
                        await client.send('Page.stopLoading');
                    } catch (err) {
                        logger.info(`StopLoading failed: ${err.message}`);
                    }
                }

                firstLoad = false //Only allow first navigation request
                request.continue()
            })      


            // Navigate to page

            const navigationCheckTimer = new Timeout()

            try {

                await Promise.race([
                    page.goto(urlToCrawl, { 
                        waitUntil: 'domcontentloaded',
                        timeout: navigationTimeout  
                    }),
                    navigationCheckTimer.set(navigationTimeout + 10000, 'Goto timeout')
                ]);

                navigationCheckTimer.clear()
            }
            
            catch (e) {

                navigationCheckTimer.clear()
                if (isTimeoutError(e) === true) {

                    logger.info('Navigation timeout exceeded.');
                }
                throw e;
            }

            await sleep(5000);

            // Capture page graph and write to file
            const response = await generatePageGraph(args.seconds, page, client, shouldStopWaitingFunc, logger);
            await writeGraphML(args, urlToCrawl, response, logger);
            
            // Store HAR
            if (args.storeHar) {
                await exportHARTimeOut(networkEvents, pageEvents, responseBodies, args, urlToCrawl, logger) 
            }
            if (args.screenshot) {
                const screenshotPath = createScreenshotPath(args, urlToCrawl);
                try {

                    await Promise.race([
                        page.screenshot({ type: 'png', path: screenshotPath }),
                        new Promise((_, reject) => setTimeout(() => reject(new Error('browser.close() timeout')), 5000))
                    ]);
                }
                catch {
                    logger.info('could not take screenshot')

                }
            }
            await safePageClose(page, logger)
        }
        catch (err) {
            logger.info('Error:', err)
            shouldReturnError = true; 
        }
        finally {
            try {
                await Promise.race([
                    browser.close(),
                    new Promise((_, reject) => setTimeout(() => reject(new Error('browser.close() timeout')), 3000))
                ]);
            } catch (e) {
                logger.info(`Force killing browser: ${e.message}`);
                browser.process().kill('SIGKILL');
            }

        }
    }
    catch (err) {


        const errorMessage = err instanceof Error 
            ? err.stack || err.message 
            : JSON.stringify(err, Object.getOwnPropertyNames(err)) || String(err);

        logger.info('Error occurred: ' + errorMessage);
        shouldReturnError = true;
    }
    finally {
        envHandle.close();
    }
    
    // Handle redirection case (crawl new URL)
    if (shouldRedirectToUrl !== undefined) {
        const newArgs = { ...args };
        newArgs.url = shouldRedirectToUrl;
        await doCrawl(newArgs, redirectChain, logger);
        return;
    }

    // If crawl failed, bubble up error to parent
    if (shouldReturnError) {
        throw new Error("Go to next child!");
    }
};
