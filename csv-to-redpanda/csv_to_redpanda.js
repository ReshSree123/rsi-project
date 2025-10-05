// csv_to_redpanda.js
// Usage: node csv_to_redpanda.js
// Make sure your Redpanda is running and "trade-data" topic exists

const fs = require('fs');
const csv = require('csv-parser');
const { Kafka } = require('kafkajs');

const inputFile = 'trades_data.csv';
const topic = 'trade-data';
const broker = process.env.BROKER || "host.docker.internal:9092";// adjust if using Docker

const kafka = new Kafka({
  clientId: 'csv-producer',
  brokers: [broker],
});

const producer = kafka.producer();

async function sendToRedpanda(rows) {
  await producer.connect();
  console.log(`Connected to Redpanda broker: ${broker}`);
  let count = 0;

  for (const row of rows) {
    // convert numeric fields
    if (row.price_in_sol) {
      const n = Number(row.price_in_sol);
      row.price_in_sol = Number.isNaN(n) ? row.price_in_sol : n;
    }
    if (row.block_time) {
      const n = Number(row.block_time);
      if (!Number.isNaN(n)) {
        row.block_time = n > 1e12 ? new Date(n).toISOString()
                         : n > 1e9 ? new Date(n * 1000).toISOString()
                         : row.block_time;
      }
    }

    try {
      await producer.send({
        topic,
        messages: [
          { key: row.token_address, value: JSON.stringify(row) }
        ],
      });
      count++;
      if (count % 100 === 0) console.log(`Sent ${count} messages...`);
    } catch (err) {
      console.error('Error sending message:', err);
    }
  }

  console.log(`✅ Finished sending ${count} messages to topic "${topic}"`);
  await producer.disconnect();
}

async function main() {
  const rows = [];

  fs.createReadStream(inputFile)
    .pipe(csv())
    .on('data', (row) => rows.push(row))
    .on('end', async () => {
      console.log(`Parsed ${rows.length} rows from CSV`);
      await sendToRedpanda(rows);
    })
    .on('error', (err) => console.error('Error parsing CSV:', err));
}

main().catch(err => console.error('Fatal error:', err));
