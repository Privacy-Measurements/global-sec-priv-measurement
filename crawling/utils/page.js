import { asHTTPUrl } from './utils.js';
import { parseDomain, fromUrl } from 'parse-domain';


function getRandomLinks(links, count) {
    const shuffled = [...links].sort(() => 0.5 - Math.random());
    return shuffled.slice(0, count);
  }

const filterSameETLDPlusOne = (links, mainUrl) => {

    const mainDomain = parseDomain(fromUrl(mainUrl.href));
    if (!mainDomain || !mainDomain.domain || !mainDomain.topLevelDomains) return [];
    const mainEtldPlusOne 
    = `${mainDomain.domain}.${mainDomain.topLevelDomains.join('.')}`;

    return links.filter(link => {
        const linkDomain = parseDomain(fromUrl(link.href));
        if (linkDomain && linkDomain.domain && linkDomain.topLevelDomains) {
            const linkEtldPlusOne = `${linkDomain.domain}.${linkDomain.topLevelDomains.join('.')}`;
            return linkEtldPlusOne === mainEtldPlusOne;
        }
        return false;
    });

};


const filterSameFQDN = (links, mainUrl) => {
    const mainFqdn = new URL(mainUrl.href).hostname;

    return links.filter(link => {
        try {
            const linkFqdn = new URL(link.href).hostname;
            return linkFqdn === mainFqdn;
        } catch (e) {
            return false;
        }
    });
};



export const selectRandomChildUrl = async (page) => {

    const mainFrameUrl = asHTTPUrl(page.url());
    let rawLinks;
    try {
        rawLinks = await page.$$('a[href]');
    }
    catch (e) {
        return undefined;
    }
    let links = [];
    for (const link of rawLinks) {
        const hrefHandle = await link.getProperty('href');
        const hrefValue = await hrefHandle.jsonValue();
        try {
            const hrefUrl = asHTTPUrl(hrefValue.trim(), mainFrameUrl);
            if (hrefUrl === undefined) {
                continue;
            }
            links.push(hrefUrl);
        }
        catch (ignore) {
            continue;
        }
    }


    if (mainFrameUrl !== undefined) {
        links = filterSameFQDN(links, mainFrameUrl);
    }
    links = [...new Set(links)];

    const randomLinks = getRandomLinks(links);
    return randomLinks;
 
};




