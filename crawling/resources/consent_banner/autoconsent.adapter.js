import AutoConsent from '@duckduckgo/autoconsent';
import { autoconsent } from '@duckduckgo/autoconsent/rules/rules.json';
import { consentomatic } from '@duckduckgo/autoconsent/rules/consentomatic.json';

const autoconsentConfig = {
    enabled: true,
    autoAction: 'optIn',
    disabledCmps: [],
    enablePrehide: false,
    enableCosmeticRules: false,
    enableGeneratedRules: true,
    detectRetries: 20,
    isMainWorld: true,
    prehideTimeout: 2000,
    enableFilterList: false,
    logs: {
        lifecycle: false,
        rulesteps: false,
        detectionsteps: false,
        evals: false,
        errors: true,
        messages: false,
        waits: false,
    },
};

const rules = {
    autoconsent,
    consentomatic,
};

if (!window.__globalPrivacyAutoconsentInjected) {
    window.__globalPrivacyAutoconsentInjected = true;

    const sendContentMessage = async (message) => {
        if (message?.type === 'autoconsentError') {
            console.warn('[autoconsent]', message.details);
        }
    };

    new AutoConsent(sendContentMessage, autoconsentConfig, rules);
}