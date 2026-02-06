
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

export const isTimeoutError = (error) => {
    if (typeof error.name !== 'string') {
        return false;
    }
    return error.name === 'TimeoutError';
};
