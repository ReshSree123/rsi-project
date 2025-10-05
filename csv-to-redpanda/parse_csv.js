const fs = require('fs');
const csv = require('csv-parser');

const inputFile = 'trades_data.csv';   // your CSV file
const outputFile = 'trades_data.json'; // JSON output

const rows = []; // array to store all rows

fs.createReadStream(inputFile)
  .pipe(csv()) // parses each row
  .on('data', (row) => {
    // optional: convert price_in_sol to number
    if (row.price_in_sol) {
      const n = Number(row.price_in_sol);
      row.price_in_sol = Number.isNaN(n) ? row.price_in_sol : n;
    }

    // optional: convert block_time from epoch to ISO string
    if (row.block_time) {
      const n = Number(row.block_time);
      if (!Number.isNaN(n)) {
        row.block_time = n > 1e12 ? new Date(n).toISOString()
                         : n > 1e9 ? new Date(n * 1000).toISOString()
                         : row.block_time;
      }
    }

    rows.push(row); // add row to array
  })
  .on('end', () => {
    fs.writeFileSync(outputFile, JSON.stringify(rows, null, 2), 'utf8');
    console.log(`✅ All ${rows.length} rows parsed and saved to ${outputFile}`);
  })
  .on('error', (err) => {
    console.error('Error parsing CSV:', err);
  });

