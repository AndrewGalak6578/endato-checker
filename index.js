// index.js
import fs from 'fs';
import readline from 'readline';
import { Worker } from 'worker_threads';
import path from 'path';
import pino from 'pino';
import os from 'os';
import osUtils from 'os-utils';
import { fileURLToPath } from 'url';

const workers = []; // Массив для хранения всех воркеров


// Определяем __filename и __dirname в ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Пути к файлам для логов и ошибок
const errorLogPathJson = path.join(__dirname, 'error_log.json');
const logPath = path.join(__dirname, 'log.json');
const rawLogPath = path.join(__dirname, 'raw_log.json');
const outputFilePath = path.join(__dirname, 'output.txt');

const outputStream = fs.createWriteStream(outputFilePath, { flags: 'a' });

const apiCredentials = [
    { apiKey: '4b6592bfe70d4754bf0819bced8ca751', apiPassword: '46da070751574ad591b3254415dc3d69' },
];

// Настройка логирования с использованием pino
const logger = pino(pino.destination({ dest: logPath, sync: true }));
const errorLogger = pino(pino.destination({ dest: errorLogPathJson, sync: true }));
const rawLogger = pino(pino.destination({ dest: rawLogPath, sync: true }));

// Функция для проверки доступности email
function checkEmailAvailability(emails) {
    const domainsToCheck = ['aol.com', 'gmail.com', 'outlook.com', 'hotmail.com', 'yahoo.com'];

    const availableEmails = emails.filter(email => {
        const domain = email.split('@')[1];
        return domainsToCheck.includes(domain);
    });

    if (availableEmails.length > 0) {
        console.log(`Available Emails: ${availableEmails.join(', ')}`);
    } else {
        console.log('All emails are taken.');
    }
}

// Функция для динамического определения количества воркеров на основе загрузки системы
function getOptimalWorkerCount() {
    const cpuCount = os.cpus().length;
    const load = os.loadavg()[0]; // Средняя загрузка за 1 минуту
    const targetLoad = 0.8;

    let workerCount = Math.round((cpuCount * targetLoad) / (load || 1)); // Избегаем деления на ноль
    workerCount = Math.max(1, Math.min(cpuCount, workerCount)); // Ограничиваем между 1 и количеством ядер

    return workerCount;
}

// Функция для создания воркера
function createWorker(workerData, workerId) {
    workerData.outputFilePath = outputFilePath;
    workerData.workerId = workerId; // Добавляем workerId в workerData
    const worker = new Worker(new URL('./worker.js', import.meta.url), {
        workerData,
        type: 'module',
    });
    worker.id = workerId;

    worker.on('message', (msg) => {
        if (msg.type === 'emails') {
            const { person, emails } = msg;
            console.log(`Extracted Emails for ${person.FirstName} ${person.LastName}: ${emails.join(', ')}`);
            checkEmailAvailability(emails);
        } else if (msg.type === 'error') {
            console.error(msg.message);
        } else if (msg.type === 'taskCompleted') {
            const workerId = msg.workerId;
            const worker = workers.find(w => w.id === workerId);
            if (worker) {
                worker.taskCount--;
                console.log(`Worker ${workerId} completed a task. Remaining tasks: ${worker.taskCount}`);
            }
        }
    });

    worker.on('error', (err) => {
        console.error('Worker error:', err);
    });

    // Инициализируем счетчик задач
    worker.taskCount = 0;

    return worker;
}

// Главная функция для обработки файла

async function processFile(filePath) {
    let numWorkers = getOptimalWorkerCount();
    console.log(`Starting with ${numWorkers} workers...`);

    const workers = [];
    let lineCount = 0;
    const startTime = Date.now();

    // Создаем Worker Threads
    for (let i = 0; i < numWorkers; i++) {
        const workerData = {
            logPath,
            errorLogPathJson,
            rawLogPath,
            MAX_CONCURRENT_REQUESTS: 50, // Это значение также можно сделать динамическим
            apiCredentials
        };
        const worker = createWorker(workerData, i);
        workers.push(worker);
    }

    const rl = readline.createInterface({
        input: fs.createReadStream(filePath),
        crlfDelay: Infinity
    });

    // Распределяем строки между Worker Threads
    for await (const line of rl) {
        // Находим воркера с наименьшим количеством задач
        let worker = workers.reduce((prev, curr) => (prev.taskCount < curr.taskCount ? prev : curr));
        worker.postMessage(line);
        worker.taskCount++;
        lineCount++;

        console.log(`Assigned line ${lineCount} to worker ${worker.id}. Worker task count: ${worker.taskCount}`);

        if (lineCount % 1000 === 0) {
            console.log(`Dispatched ${lineCount} lines...`);
        }
    }

    // Оповещаем Workers о завершении
    for (const worker of workers) {
        worker.postMessage(null); // Отправляем null как сигнал завершения
    }

    // Ждем завершения всех Workers
    await Promise.all(workers.map(worker => {
        return new Promise((resolve) => {
            worker.on('exit', () => {
                resolve();
            });
        });
    }));

    const endTime = Date.now();
    console.log(`File processing completed. Total lines: ${lineCount}. Time taken: ${(endTime - startTime) / 1000} seconds`);
}

// Запуск программы
(async () => {
    const startTime = new Date();

    const input = process.argv[2];

    try {
        if (fs.existsSync(input)) {
            await processFile(input);
        } else {
            console.error('Input file does not exist.');
        }
    } catch (err) {
        console.error('An error occurred during processing:', err);
    } finally {
        // Закрываем логгеры
        await logger.flush();
        await errorLogger.flush();
        await rawLogger.flush();
    }

    const endTime = new Date();
    const timeDiff = endTime - startTime;
    console.log(`Total execution time: ${timeDiff / 1000} seconds`);
    process.exit(0); // Завершаем процесс
})();
