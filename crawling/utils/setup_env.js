import * as osLib from 'os';
import Xvbf from 'xvfb';

let xvfbPlatforms = new Set(['linux', 'openbsd']);




export const setupEnv = () => {
    const platformName = osLib.platform();
    let xvfbHandle;

    const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

    // Make the close function async
    const closeFunc = async () => {
        if (!xvfbHandle) return;

        let attempt = 0;
        while (attempt < 5) {
            try {
                xvfbHandle.stopSync();
                break; 
            } catch (error) {
                attempt++;
                if (attempt >= 5) {
                    throw new Error('Failed to stop Xvfb after maximum retries');
                } else {
                    console.warn(`Xvfb stop failed, retrying in 1s... (attempt ${attempt})`);
                    await wait(1000); 
                }
            }
        }
    };

    if (xvfbPlatforms.has(platformName)) {
        xvfbHandle = new Xvbf({
            xvfb_args: ['-screen', '0', '1024x768x24'],
        });
        xvfbHandle.startSync();
    }

    return {
        close: closeFunc, 
    };
};