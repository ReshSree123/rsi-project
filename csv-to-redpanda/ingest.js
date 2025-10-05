const { Kafka } = require('kafkajs');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');

class CSVToRedpanda {
    constructor(config = {}) {
        this.config = {
            brokers: config.brokers || ['localhost:9092'],
            topic: config.topic || 'trade-data',
            csvFile: config.csvFile || 'trades_data.csv',
            ...config
        };

        this.kafka = new Kafka({
            clientId: 'csv-ingester',
            brokers: this.config.brokers,
            retry: {
                initialRetryTime: 100,
                retries: 8
            }
        });

        this.producer = this.kafka.producer();
        this.metrics = {
            totalProcessed: 0,
            successful: 0,
            failed: 0,
            startTime: null,
            endTime: null
        };
    }

    async connect() {
        try {
            await this.producer.connect();
            console.log('? Connected to Redpanda cluster');
            return true;
        } catch (error) {
            console.error('? Failed to connect to Redpanda:', error.message);
            return false;
        }
    }

    async processCSV() {
        this.metrics.startTime = Date.now();
        console.log(`?? Starting CSV ingestion from: ${this.config.csvFile}`);

        return new Promise((resolve, reject) => {
            let batch = [];
            const batchSize = 100;

            if (!fs.existsSync(this.config.csvFile)) {
                reject(new Error(`CSV file not found: ${this.config.csvFile}`));
                return;
            }

            const fileStream = fs.createReadStream(this.config.csvFile)
                .pipe(csv())
                .on('data', async (row) => {
                    fileStream.pause();
                    this.metrics.totalProcessed++;

                    try {
                        const processedRow = this.processRow(row);
                        batch.push(processedRow);

                        if (batch.length >= batchSize) {
                            await this.sendBatch([...batch]);
                            batch = [];
                        }

                        if (this.metrics.totalProcessed % 500 === 0) {
                            console.log(`?? Processed ${this.metrics.totalProcessed} rows...`);
                        }

                        fileStream.resume();
                    } catch (error) {
                        console.error(`? Error processing row ${this.metrics.totalProcessed}:`, error.message);
                        this.metrics.failed++;
                        fileStream.resume();
                    }
                })
                .on('end', async () => {
                    try {
                        if (batch.length > 0) {
                            await this.sendBatch(batch);
                        }

                        this.metrics.endTime = Date.now();
                        await this.printSummary();
                        resolve(this.metrics);
                    } catch (error) {
                        reject(error);
                    }
                })
                .on('error', (error) => {
                    console.error('? CSV read error:', error.message);
                    reject(error);
                });
        });
    }

    processRow(row) {
        const processed = {};

        for (const [key, value] of Object.entries(row)) {
            if (['price_in_sol', 'amount', 'volume'].includes(key)) {
                processed[key] = value ? parseFloat(value) : null;
            }
            else if (['block_time', 'timestamp', 'created_at'].includes(key)) {
                processed[key] = value ? new Date(value).toISOString() : null;
            }
            else if (['is_verified', 'success'].includes(key)) {
                processed[key] = value ? value.toString().toLowerCase() === 'true' : false;
            }
            else {
                processed[key] = value || null;
            }
        }

        return processed;
    }

    async sendBatch(batch) {
        const messages = batch.map(record => ({
            key: record.token_address || 'default',
            value: JSON.stringify(record)
        }));

        try {
            await this.producer.send({
                topic: this.config.topic,
                messages: messages
            });

            this.metrics.successful += batch.length;
            console.log(`? Sent batch of ${batch.length} messages (Total: ${this.metrics.successful})`);
        } catch (error) {
            this.metrics.failed += batch.length;
            console.error(`? Failed to send batch:`, error.message);
            throw error;
        }
    }

    async printSummary() {
        const duration = (this.metrics.endTime - this.metrics.startTime) / 1000;
        const totalMessages = this.metrics.successful + this.metrics.failed;
        const throughput = this.metrics.successful / duration;

        console.log('\n' + '='.repeat(60));
        console.log('?? INGESTION SUMMARY');
        console.log('='.repeat(60));
        console.log(`?? File: ${this.config.csvFile}`);
        console.log(`?? Topic: ${this.config.topic}`);
        console.log(`??  Duration: ${duration.toFixed(2)} seconds`);
        console.log(`? Successful: ${this.metrics.successful}`);
        console.log(`? Failed: ${this.metrics.failed}`);
        console.log(`?? Throughput: ${throughput.toFixed(2)} messages/second`);
        console.log(`?? Success Rate: ${((this.metrics.successful / totalMessages) * 100).toFixed(1)}%`);
        console.log('='.repeat(60));
    }

    async disconnect() {
        try {
            await this.producer.disconnect();
            console.log('?? Disconnected from Redpanda');
        } catch (error) {
            console.error('Error disconnecting:', error.message);
        }
    }
}

async function main() {
    const config = {
        brokers: ['localhost:9092'],
        topic: 'trade-data',
        csvFile: 'C:\\Users\\reshm\\Desktop\\rsi-project\\trades_data.csv'
    };

    console.log('?? Starting CSV to Redpanda Ingestion...');
    console.log('Configuration:', config);

    const ingester = new CSVToRedpanda(config);

    try {
        const connected = await ingester.connect();
        if (!connected) {
            process.exit(1);
        }

        await ingester.processCSV();

    } catch (error) {
        console.error('?? Ingestion failed:', error.message);
        process.exit(1);
    } finally {
        await ingester.disconnect();
    }
}

process.on('SIGINT', async () => {
    console.log('\n?? Received shutdown signal...');
    process.exit(0);
});

if (require.main === module) {
    main();
}