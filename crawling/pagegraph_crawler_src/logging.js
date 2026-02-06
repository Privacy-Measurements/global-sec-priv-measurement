'use strict';

import fs from 'fs';


class Logger {
    

    constructor(iterationLogFile, core_index) {
        this.iterationLogFile = iterationLogFile;
        this.core_index = core_index
    }

    baseLogFunction(prefix, ...msg) {
        const timestamp = new Date().toISOString(); 

        const messageParts = [
            `[${timestamp}] `,
            `${prefix} FROM core ${this.core_index}: `
        ];


        for (const part of msg) {
            if (Array.isArray(part)) {
                for (const item of part) {
                    messageParts.push(String(item));
                }
            } else {
                messageParts.push(String(part));
            }
        }
        const finalMessage = messageParts.join('');
        console.log(finalMessage);

        if (this.iterationLogFile !== undefined) {
            fs.appendFile(this.iterationLogFile, finalMessage + '\n', (err) => {
                if (err) console.error('Failed to write to iteration log file:', err);
            });
        }
    }

    info(...msg) {
        this.baseLogFunction('INFO', ...msg);
    }

}

export default Logger;
