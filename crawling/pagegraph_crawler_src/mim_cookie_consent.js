import { consentExtensionPath } from './paths.js';
import { promisify } from 'util';
import { exec as execCallback } from 'child_process';
import { Proxy } from "http-mitm-proxy";
import { build } from 'esbuild';
import Promise from 'bluebird';
import fs from 'fs';
import path from 'path';


const password = ''

const buildInjectedConsentScript = async () => {
  const result = await build({
    entryPoints: [path.resolve(consentExtensionPath)],
    bundle: true,
    write: false,
    format: 'iife',
    platform: 'browser',
    target: ['es2021'],
    logLevel: 'silent',
    minify: true,
    legalComments: 'none',
  });

  return result.outputFiles[0].text.replace(/<\/script/gi, '<\\/script');
};

const consentScript = await buildInjectedConsentScript();
const jsToInject = `<script>${consentScript}</script>`;



const execPromise = promisify(execCallback);


/**
 * Run certutil CA import once in master process
 */
export const importMitmCert = async () => {
  try {
    // Make sure NSS DB exists
    await execPromise("mkdir -p $HOME/.pki/nssdb");
    // Attempt to import cert
    await execPromise(
      'certutil -d sql:$HOME/.pki/nssdb -A -t "C,," -n mitm-ca -i ./.http-mitm-proxy/certs/ca.pem'
    );
    console.log("MITM CA certificate imported successfully.");
  } catch (err) {
    // If cert already exists, just log and continue
    if (err.message && err.message.includes("already exists")) {
      console.log("MITM CA certificate already exists. Skipping import.");
    } else {
      throw err; // rethrow for unexpected errors
    }
  }
};


export const setupProxy = async (port) => {
    let proxy = new Proxy();
    proxy.use(Proxy.wildcard);
    proxy.use(Proxy.gunzip);

    proxy = await new Promise((resolve, reject) => {
        console.log('listening:', port)
        proxy.listen({ host: '127.0.0.1', port }, (err) => {
            if (err) return reject(err);
            resolve(proxy)
        });
    });


    proxy.onRequest((ctx, callback) => {
      const chunks = [];
      
      ctx.onResponseData((ctx, chunk, callback) => {
          chunks.push(chunk);
          return callback(null, undefined); // don't write chunks to client response
      });

      ctx.onResponseEnd((ctx, callback) => {
          let body = Buffer.concat(chunks);
          if (
              ctx.serverToProxyResponse !== undefined &&
              ctx.serverToProxyResponse.headers["content-type"] &&
              ctx.serverToProxyResponse.headers["content-type"].indexOf("text/html") === 0
          ) {

              const html = body.toString();
              if (html.includes('</body>')) {
                  body = html.replace('</body>', jsToInject + '</body>');
              }

          }

          ctx.proxyToClientResponse.write(body);
          return callback();
    });

    callback();
    });
    return proxy;
};



