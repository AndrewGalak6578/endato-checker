import { parentPort, workerData } from 'worker_threads';
import fs from 'fs';
import pLimit from 'p-limit';
import pino from 'pino';
import Sorter from './sorter.js';
import { createApiInstance } from './api.js';

// Destructure workerData
const {
    logPath,
    errorLogPathJson,
    rawLogPath,
    MAX_CONCURRENT_REQUESTS,
    apiCredentials,
    outputFilePath,
    workerId
} = workerData;

// Set up logging with pino
const logger = pino(pino.destination({ dest: logPath, sync: true }));
const errorLogger = pino(pino.destination({ dest: errorLogPathJson, sync: true }));
const rawLogger = pino(pino.destination({ dest: rawLogPath, sync: true }));


let limit = pLimit(MAX_CONCURRENT_REQUESTS);
let maxConcurrentRequests = MAX_CONCURRENT_REQUESTS;
let requestErrors = 0;
let isProcessing = false;

// Initialize API instance with the first set of credentials
let currentCredentialIndex = 0;
let api = createApiInstance(
    apiCredentials[currentCredentialIndex].apiKey,
    apiCredentials[currentCredentialIndex].apiPassword
);


// Validation functions
function isValidDate(dateStr) {
    const dateRegex = /^(0[1-9]|1[0-2])\/(0[1-9]|[12]\d|3[01])\/(19|20)\d\d$/;
    return dateRegex.test(dateStr);
}

function isValidPhoneNumber(phone) {
    const phoneRegex = /^(\d{3})-(\d{3})-(\d{4})$/;
    return phoneRegex.test(phone);
}

function isValidEmail(email) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
}
function validatePersonData(person) {
    const errors = [];
    // if (!person.firstName) errors.push('FirstName is required.');
    // if (!person.lastName) errors.push('LastName is required.');
    if (person.dob && !isValidDate(person.dob)) errors.push('Dob must be in mm/dd/yyyy format.');
    if (person.age !== null && (isNaN(person.age) || person.age <= 0 || person.age > 120)) errors.push('Age must be a valid number between 1 and 120.');
    // if (person.phone && !isValidPhoneNumber(person.phone)) errors.push('Phone must be in format ###-###-####');
    // if (person.email && !isValidEmail(person.email)) errors.push('Email must be a valid email address.');
    // Removed the check for at least one of phone or email
    return errors;
}

// Message handler from the main thread
parentPort.on('message', async (line) => {
    if (line === null) {
        // Wait for processing to finish
        if (isProcessing) {
            await waitForProcessing();
        }
        // Close loggers and exit
        await logger.flush();
        await errorLogger.flush();
        await rawLogger.flush();
        parentPort.close();
        process.exit(0);
    } else {
        console.log(`Worker ${workerId} received a line for processing.`);
        isProcessing = true;
        await processLine(line);
        isProcessing = false;
        // Notify main thread that task is completed
        parentPort.postMessage({ type: 'taskCompleted', workerId });
    }
});

// Function to wait until processing is done
function waitForProcessing() {
    return new Promise((resolve) => {
        const interval = setInterval(() => {
            if (!isProcessing) {
                clearInterval(interval);
                resolve();
            }
        }, 100);
    });
}

// Function to adjust concurrency based on errors
function adjustConcurrency(error = false) {
    if (error) {
        requestErrors++;
        if (requestErrors > 5 && maxConcurrentRequests > 1) {
            maxConcurrentRequests--;
            limit = pLimit(maxConcurrentRequests);
            requestErrors = 0;
            console.log(`Reduced MAX_CONCURRENT_REQUESTS to ${maxConcurrentRequests}`);
        }
    } else {
        if (requestErrors === 0 && maxConcurrentRequests < MAX_CONCURRENT_REQUESTS) {
            maxConcurrentRequests++;
            limit = pLimit(maxConcurrentRequests);
            console.log(`Increased MAX_CONCURRENT_REQUESTS to ${maxConcurrentRequests}`);
        }
    }
}

// Function to process a single line
async function processLine(line) {
    const sorter = new Sorter(line);
    const parsedData = sorter.parsedData;

    // Check if parsedData is empty
    if (parsedData.length === 0) {
        console.error(`No valid data parsed from line: ${line}`);
        return;
    }

    // Process each person individually
    for (const person of parsedData) {
        console.log(`Processing person: ${person.FirstName} ${person.LastName}`);
        await sendPerson(person);
    }
}

// Function to format phone numbers
function formatPhoneNumber(phoneNumber) {
    if (!phoneNumber) return null;

    const cleaned = ('' + phoneNumber).replace(/\D/g, '');

    if (cleaned.length === 10) {
        const match = cleaned.match(/^(\d{3})(\d{3})(\d{4})$/);
        if (match) {
            return `${match[1]}-${match[2]}-${match[3]}`;
        }
    }

    return null;
}

// Function to map person properties to API expectations
function mapPerson(person) {
    return {
        firstName: person.FirstName || null,
        middleName: person.MiddleName || null,
        lastName: person.LastName || null,
        dob: person.Dob || null,
        age: person.Age || null,
        address: {
            addressLine1: person.Address?.AddressLine1 || null,
            addressLine2: person.Address?.AddressLine2 || null,
        },
        phone: person.Phone && person.Phone.length > 0 ? formatPhoneNumber(person.Phone[0]) : null,
        email: person.Email && person.Email.length > 0 ? person.Email[0] : null,
    };
}

// Function to send individual person data to the API
async function sendPerson(person) {
    const mappedPerson = mapPerson(person);
    const data = mappedPerson;
    const validationErrors = validatePersonData(mappedPerson);
    if (validationErrors.length > 0) {
        console.warn(`Validation errors for ${person.FirstName} ${person.LastName}:`, validationErrors);
        return; // Пропустить, если ошибки
    }

    console.log(`Sending data for ${person.FirstName} ${person.LastName}...`); // Лог отправки

    let retries = 3;
    while (retries > 0) {
        try {
            const apiResponse = await limit(() =>
                api.post('https://devapi.endato.com/Contact/Enrich', mappedPerson, {
                    headers: {
                        'galaxy-search-type': 'DevAPIContactEnrich',
                    },
                })
            );

            // Лог успешной отправки
            console.log(`Success for ${person.FirstName} ${person.LastName}: ${JSON.stringify(apiResponse.data)}`);

            rawLogger.info(apiResponse.data);
            const emails = apiResponse.data.person.emails.map((email) => email.email);
            parentPort.postMessage({ type: 'emails', person, emails });
            writePersonToOutputFile(apiResponse.data.person, person);
            adjustConcurrency(false); // Успешный запрос
            break; // Выйти из цикла при успехе
        } catch (apiError) {
            adjustConcurrency(true); // Ошибка
            retries--; // Уменьшить количество попыток
            console.error(`Error processing person ${person.FirstName} ${person.LastName}. Retries left: ${retries}`);
            console.error(`API error: ${apiError.message}`);

            if (retries === 0) {
                console.error(`Failed to process ${person.FirstName} ${person.LastName} after 3 attempts.`);
            }
        }
    }
}

// Function to switch API credentials
function switchApiCredentials() {
    currentCredentialIndex = (currentCredentialIndex + 1) % apiCredentials.length;
    const newApiKey = apiCredentials[currentCredentialIndex].apiKey;
    const newApiPassword = apiCredentials[currentCredentialIndex].apiPassword;

    // Create a new API instance with the new credentials
    api = createApiInstance(newApiKey, newApiPassword);
    console.log(`Switched to API key: ${newApiKey}`);
}
function writePersonToOutputFile(responsePerson, originalPerson) {
    // Reconstruct the line in the specified format
    const line = formatPersonAsLine(responsePerson, originalPerson);

    // Write to the output file synchronously to prevent data corruption
    fs.appendFileSync(outputFilePath, line + '\n', 'utf8');
}

// Function to format person data as a line
function formatPersonAsLine(responsePerson, originalPerson) {
    // Extract fields from originalPerson
    const ID = originalPerson.ID || '';
    const DOB = originalPerson.Dob || '';
    const fullName = `${originalPerson.FirstName || ''} ${originalPerson.MiddleName || ''} ${originalPerson.LastName || ''}`.trim();

    // Use address from response or original
    const addresses = responsePerson.addresses || [];
    const mostRecentAddress = addresses[0] || {
        street: originalPerson.Address?.AddressLine1 || '',
        city: originalPerson.Address?.AddressLine2?.split(',')[0]?.trim() || '',
        state: originalPerson.Address?.AddressLine2?.split(',')[1]?.trim().split(' ')[0] || '',
        zip: originalPerson.Address?.AddressLine2?.split(',')[1]?.trim().split(' ')[1] || ''
    };

    const street = mostRecentAddress.street || '';
    const city = mostRecentAddress.city || '';
    const state = mostRecentAddress.state || '';
    const zip = mostRecentAddress.zip || '';

    // Phones and Emails from response or original
    const phonesFromResponse = (responsePerson.phones || []).map(phone => phone.number);
    const phonesFromOriginal = originalPerson.Phones || [];
    const allPhones = [...phonesFromResponse, ...phonesFromOriginal];
    const phones = allPhones.join('|');

    const emailsFromResponse = (responsePerson.emails || []).map(email => email.email);
    const emailsFromOriginal = originalPerson.Emails || [];
    const allEmails = [...emailsFromResponse, ...emailsFromOriginal];
    const emails = allEmails.join('|');

    // Construct the line in the same format as the input
    const parts = [
        ID,
        DOB,
        fullName,
        street,
        city,
        state,
        zip,
        phones,
        emails
    ];

    return parts.join(';');
}