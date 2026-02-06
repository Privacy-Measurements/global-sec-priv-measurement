// Cookie button detection regexes

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const PHRASES_EN = ["accept","accept & close","accept & continue","accept & proceed","accept all","accept all cookies","accept all settings","accept and close","accept and continue","accept and continue to site","accept cookies","accept our policy","accept recommended cookies","agree","agree & close","agree & continue","agree all","agree and close","agree and continue","allow","allow all","allow all cookies","allow cookies","close","close this message and continue","confirm","consent","continue","continue (accept cookies)","continue and accept","continue to site","continue with recommended cookies","got it","i accept","i accept all","i accept all cookies","i accept cookies","i agree","i consent","i understand","ok","okay","yes","fine by me","happy to accept cookies","no problem","proceed","save and close","select all","that's ok","understood","yes, continue","yes, i accept","yes, i agree"];
const PHRASES_DE = ["akzeptieren","akzeptieren & schließen","akzeptieren & fortfahren","akzeptieren & weiter","alle akzeptieren","alle cookies akzeptieren","alle einstellungen akzeptieren","akzeptieren und schließen","akzeptieren und fortfahren","akzeptieren und zur website weiter","cookies akzeptieren","unsere richtlinie akzeptieren","empfohlene cookies akzeptieren","zustimmen","zustimmen & schließen","zustimmen & weiter","zustimmen alle","zustimmen und schließen","zustimmen und weiter","zulassen","alle zulassen","alle cookies zulassen","cookies zulassen","schließen","diese nachricht schließen und fortfahren","bestätigen","zustimmung","weiter","weiter (cookies akzeptieren)","weiter und akzeptieren","weiter zur website","weiter mit empfohlenen cookies","verstanden","ich akzeptiere","ich akzeptiere alle","ich akzeptiere alle cookies","ich akzeptiere cookies","ich stimme zu","ich verstehe","ok","okay","ja","fortfahren","speichern und schließen","alles auswählen","alles klar","einverstanden","ja, weiter","ja, ich akzeptiere","ja, ich stimme zu"];
const PHRASES_IT = ["accetta","accetta e chiudi","accetta e continua","accetta e procedi","accetta tutto","accetta tutti i cookie","accetta tutte le impostazioni","accetta e chiudi","accetta e continua","accetta e continua la navigazione","accetta i cookie","accetta la nostra politica","accetta i cookie consigliati","acconsento","acconsento e continuo","acconsento tutto","acconsento e chiudo","acconsento e continuo","consenti","consenti tutto","consenti tutti i cookie","consenti i cookie","chiudi","chiudi questo messaggio e continua","conferma","consenso","continua","continua (accetta cookie)","continua e accetta","continua al sito","continua con i cookie consigliati","ho capito","accetto","accetto tutto","accetto tutti i cookie","accetto i cookie","sono d'accordo","do il consenso","capisco","ok","okay","sì","va bene","prosegui","salva e chiudi","seleziona tutto","tutto ok","comprendo","sì, continua","sì, accetto","sì, sono d'accordo"];
const PHRASES_FR = ["accepter","accepter et fermer","accepter et continuer","accepter et poursuivre","accepter tout","accepter tous les cookies","accepter tous les paramètres","accepter et fermer","accepter et continuer","accepter et continuer vers le site","accepter les cookies","accepter notre politique","accepter les cookies recommandés","consentir","consentir et fermer","consentir et continuer","consentir tout","consentir et fermer","consentir et continuer","autoriser","autoriser tout","autoriser tous les cookies","autoriser les cookies","fermer","fermer ce message et continuer","confirmer","consentement","continuer","continuer (accepter les cookies)","continuer et accepter","continuer vers le site","continuer avec les cookies recommandés","compris","j'accepte","j'accepte tout","j'accepte tous les cookies","j'accepte les cookies","je consens","je comprends","ok","okay","oui","tout va bien","procéder","enregistrer et fermer","tout sélectionner","c'est bon","compris","oui, continuer","oui, j'accepte","oui, je consens"];
const PHRASES_ES = ["aceptar","aceptar y cerrar","aceptar y continuar","aceptar y proceder","aceptar todo","aceptar todas las cookies","aceptar toda la configuración","aceptar y cerrar","aceptar y continuar","aceptar y continuar en el sitio","aceptar cookies","aceptar nuestra política","aceptar cookies recomendadas","acepto","acepto y continúo","acepto todo","acepto y cierro","acepto y continúo","permitir","permitir todo","permitir todas las cookies","permitir cookies","cerrar","cerrar este mensaje y continuar","confirmar","consentir","continuar","continuar (aceptar cookies)","continuar y aceptar","continuar al sitio","continuar con cookies recomendadas","entendido","acepto","acepto todo","acepto todas las cookies","acepto cookies","consiento","entiendo","ok","vale","sí","proseguir","guardar y cerrar","seleccionar todo","está bien","comprendido","sí, continuar","sí, acepto","sí, consiento"];
const PHRASES_PT = ["aceitar","aceitar e fechar","aceitar e continuar","aceitar e prosseguir","aceitar tudo","aceitar todos os cookies","aceitar todas as configurações","aceitar e fechar","aceitar e continuar","aceitar e continuar no site","aceitar cookies","aceitar nossa política","aceitar cookies recomendados","concordo","concordo e continuo","concordo tudo","concordo e fecho","concordo e continuo","permitir","permitir tudo","permitir todos os cookies","permitir cookies","fechar","fechar esta mensagem e continuar","confirmar","consentir","continuar","continuar (aceitar cookies)","continuar e aceitar","continuar no site","continuar com cookies recomendados","entendi","aceito","aceito tudo","aceito todos os cookies","aceito cookies","consinto","entendo","ok","okay","sim","prosseguir","salvar e fechar","selecionar tudo","está bem","compreendido","sim, continuar","sim, aceito","sim, consinto"];
const PHRASES_NL = ["accepteren","accepteren en sluiten","accepteren en doorgaan","accepteren en verdergaan","alles accepteren","alle cookies accepteren","alle instellingen accepteren","accepteren en sluiten","accepteren en doorgaan","accepteren en doorgaan naar site","cookies accepteren","ons beleid accepteren","aanbevolen cookies accepteren","akkoord gaan","akkoord gaan alles","akkoord gaan en sluiten","akkoord gaan en doorgaan","toestaan","alles toestaan","alle cookies toestaan","cookies toestaan","sluiten","dit bericht sluiten en doorgaan","bevestigen","toestemming geven","doorgaan","doorgaan (cookies accepteren)","doorgaan en accepteren","doorgaan naar site","doorgaan met aanbevolen cookies","begrepen","ik accepteer","ik accepteer alles","ik accepteer alle cookies","ik accepteer cookies","ik ga akkoord","ik begrijp het","oké","okay","ja","doorgaan","opslaan en sluiten","alles selecteren","alles duidelijk","akkoord","ja, doorgaan","ja, ik accepteer","ja, ik ga akkoord"];
const PHRASES_RUSSIAN = ["принять","принять и закрыть","принять и продолжить","принять и перейти","принять все","принять все куки","принять все настройки","принять и закрыть","принять и продолжить","принять и перейти на сайт","принять куки","принять нашу политику","принять рекомендуемые куки","согласиться","согласиться и продолжить","согласиться на всё","согласиться и закрыть","согласиться и продолжить","разрешить","разрешить всё","разрешить все куки","разрешить куки","закрыть","закрыть это сообщение и продолжить","подтвердить","согласие","продолжить","продолжить (принять куки)","продолжить и принять","перейти на сайт","продолжить с рекомендуемыми куками","понял","я принимаю","я принимаю всё","я принимаю все куки","я принимаю куки","я согласен","я понимаю","ок","окей","да","продолжать","сохранить и закрыть","выбрать всё","понятно","согласен","да, продолжить","да, я принимаю","да, я согласен"];
const PHRASES_ARABIC = ["موافقة","موافقة على الكل","قبول","قبول الكل","أوافق","أوافق على الكل","أوافق على سياسة الخصوصية","استمرار","الموافقة","نعم","نعم، أوافق","أفهم","موافق","موافقتي","استمر","قبول الكوكيز","قبول سياسة الكوكيز","قبول الكل والاغلاق","قبول الكل والمتابعة","موافقة ومتابعة","موافقة وإغلاق","موافقة ومتابعة الموقع","أوافق على استخدام الكوكيز","أوافق على استخدام ملفات تعريف الارتباط","إدراك","نعم موافق", "السماح للكلّ"];
const PHRASES_HINDI = ["स्वीकार","स्वीकृत","हाँ","ठीक है","सहमत","जारी रखें","मंजूर","सहमति","अनुमति","स्वीकार करें","स्वीकार करना","स्वीकार और जारी रखें","स्वीकार और बंद करें","सभी स्वीकार करें","सभी कुकीज़ स्वीकार करें","सभी सेटिंग्स स्वीकार करें","सहमति दें","सहमति जारी रखें","समझ गया","ठीक है, स्वीकार करें","मैं सहमत हूँ"];
const PHRASES_CHINEESE = ["接受","同意","继续","确定","允许","明白","好的","是","接受所有","同意所有","接受并关闭","同意并继续","接受全部","我同意","我接受","我同意使用","我接受使用","允许所有","允许所有Cookies"];
const PHRASES_PERSIAN = ["قبول","بله","ادامه","موافقت","متوجه شدم","اجازه دادن","پذیرش","پذیرفتن","قبول کردن","موافقت کردن","تایید","تأیید","ادامه دادن","قبول و ادامه","قبول و بستن","تمامی کوکی‌ها را قبول کنید","من قبول دارم"];



const allPhrases = new Set([
  ...PHRASES_EN,
  ...PHRASES_DE,
  ...PHRASES_IT,
  ...PHRASES_FR,
  ...PHRASES_ES,
  ...PHRASES_PT,
  ...PHRASES_NL,
  ...PHRASES_RUSSIAN,
  ...PHRASES_ARABIC,
  ...PHRASES_HINDI,
  ...PHRASES_CHINEESE,
  ...PHRASES_PERSIAN
]);


function normalizeText(text) {
  return text.trim().toLowerCase();
}


const clickButton = (button) => {
    try {
        button.click();
        console.log('Cookie button clicked');
    } catch (err) {
        console.error('Failed to click the button:', err.message);
    }
};

const acceptCookiesOnPage = (current_document) => {
    const buttons = current_document.querySelectorAll('button, a, input[type="submit"]');

    for (const button of buttons) {
        
        const text = (button.textContent || button.value || "").trim().toLowerCase();

        // Also check nested text if textContent is empty
        let combinedText = text;
        if (!text && button.querySelector('span')) {
            combinedText = Array.from(button.querySelectorAll('span'))
                .map(s => s.textContent.trim().toLowerCase())
                .join(' ');
        }

        if (allPhrases.has(combinedText) || allPhrases.has(text)) {
            console.log(`Cookie button found: "${combinedText}", "${text}"`);
            clickButton(button);
        }
    }

    console.log('Failed to find a cookie accept button');
};

const acceptCookiesRecursively = (window) => {
    acceptCookiesOnPage(window.document);
    
    for (const frame of Array.from(window.frames)) {
        try {
            acceptCookiesRecursively(frame);
        } catch (err) {
            console.log('Error accessing frame:', err.message);
            continue
        }
    }
};


console.log('Waiting 2 seconds before checking cookies...');
await sleep(2000); 
acceptCookiesRecursively(window);
await sleep(5000); 
acceptCookiesRecursively(window);