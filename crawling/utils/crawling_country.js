'use strict';

import axios from "axios";

const COUNTRY_NAMES_DICT = {
    'United Arab Emirates' : 'AE',
    'United States': 'US',
    'India': 'IN',
    'Germany': 'DE',
    'Algeria': 'DZ'

}

export async function getCountry() {
    try {
        // Get Public IP
        const ipResponse = await axios.get("https://api64.ipify.org?format=json");
        const ip = ipResponse.data.ip;

        // Get Location Info
        const locationResponse = await axios.get(`http://ip-api.com/json/${ip}`);
        const locationData = locationResponse.data;
        let country = locationData.country
        
        if (COUNTRY_NAMES_DICT[country]) {
            country = COUNTRY_NAMES_DICT[country];
        }

        return country.toUpperCase()
        
    } catch (error) {
        console.error("Error fetching IP information:", error.message);
        return null;
    }
}


