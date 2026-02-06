export const makeNavigationTracker = (navUrl, history) => {
    const currentUrl = navUrl;
    const historySet = new Set(history);
    const historyStringSet = new Set(history.map(x => x.toString()));
    const isCurrentUrl = (aURL) => {
        return aURL.toString() === currentUrl.toString();
    };
    const isInHistory = (aUrl) => {
        return historyStringSet.has(aUrl.toString());
    };
    const toHistory = () => {
        const historyArray = Array.from(historySet);
        historyArray.push(currentUrl);
        return historyArray;
    };
    return { isCurrentUrl, isInHistory, toHistory };
};


export const isTopLevelPageNavigation = (request) => {
    if (request.isNavigationRequest() === false) {
        return false;
    }
    // Check to see if this is a navigation to an error page.
    if (request.frame() === null) {
        return false;
    }
    // Check to make sure this is the top level frame
    if (request.frame().parentFrame() !== null) {
        return false;
    }
    return true;
};