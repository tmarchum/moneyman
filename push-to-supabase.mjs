/**
 * Push moneyman transaction JSON to Supabase (vaad-chacham)
 * Reads the latest JSON output from moneyman and upserts into bank_transactions + expenses
 */

import fs from 'fs';
import path from 'path';

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://stncskqjrmecjckxldvi.supabase.co';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const BUILDING_ID = process.env.BUILDING_ID;
const OUTPUT_DIR = process.env.OUTPUT_DIR || './output';

if (!SUPABASE_SERVICE_KEY || !BUILDING_ID) {
  console.error('ERROR: Missing SUPABASE_SERVICE_KEY or BUILDING_ID');
  process.exit(1);
}

// Supabase REST helper (no SDK needed)
const headers = {
  'apikey': SUPABASE_SERVICE_KEY,
  'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
  'Content-Type': 'application/json',
  'Prefer': 'return=representation',
};
async function supabaseGet(table, query = '') {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}?${query}`, { headers });
  if (!res.ok) throw new Error(`GET ${table}: ${res.status} ${await res.text()}`);
  return res.json();
}
async function supabaseInsert(table, rows) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
    method: 'POST', headers, body: JSON.stringify(rows),
  });
  if (!res.ok) throw new Error(`INSERT ${table}: ${res.status} ${await res.text()}`);
  return res.json();
}

// Find the latest JSON file in output dir
const files = fs.readdirSync(OUTPUT_DIR).filter(f => f.endsWith('.json')).sort().reverse();
if (files.length === 0) {
  console.log('No transaction JSON files found in', OUTPUT_DIR);
  process.exit(0);
}

const filePath = path.join(OUTPUT_DIR, files[0]);
console.log(`Reading: ${filePath}`);
const transactions = JSON.parse(fs.readFileSync(filePath, 'utf8'));
console.log(`Found ${transactions.length} transactions`);

if (transactions.length === 0) {
  console.log('No transactions to push');
  process.exit(0);
}

// Convert moneyman format to vaad-chacham bank_transactions format
// moneyman format: { type, date, processedDate, originalAmount, chargedAmount, description, memo, installments, status, uniqueId }
const rows = transactions.map(tx => {
  const txDate = tx.date ? tx.date.substring(0, 10) : null; // ISO date → YYYY-MM-DD
  const month = txDate ? txDate.substring(0, 7) : null;
  const amount = tx.chargedAmount || tx.originalAmount || 0;

  return {
    building_id: BUILDING_ID,
    transaction_date: txDate,
    description: tx.description || '',
    credit: amount > 0 ? amount : 0,
    debit: amount < 0 ? Math.abs(amount) : 0,
    source: tx.memo || null,
    notes: tx.status === 'pending' ? 'ממתינה' : null,
    month: month,
    match_status: 'unmatched',
  };
}).filter(r => r.transaction_date);

// Check existing to avoid duplicates
const existing = await supabaseGet('bank_transactions',
  `select=transaction_date,description,credit,debit&building_id=eq.${BUILDING_ID}`);

const existingSet = new Set(
  (existing || []).map(e => `${e.transaction_date}|${e.description}|${e.credit}|${e.debit}`)
);

const newRows = rows.filter(r =>
  !existingSet.has(`${r.transaction_date}|${r.description}|${r.credit}|${r.debit}`)
);

console.log(`New: ${newRows.length}, Duplicates skipped: ${rows.length - newRows.length}`);

if (newRows.length > 0) {
  const insertedTx = await supabaseInsert('bank_transactions', newRows);
  console.log(`Inserted ${newRows.length} transactions`);

  // Auto-create expense records from debit transactions
  const debitTx = (insertedTx || []).filter(tx => tx.debit > 0);
  if (debitTx.length > 0) {
    const expenseRows = debitTx.map(tx => ({
      building_id: BUILDING_ID,
      date: tx.transaction_date,
      description: tx.description || '',
      amount: tx.debit,
      category: 'אחר',
      source: 'bank',
      bank_transaction_id: tx.id,
      notes: '',
    }));
    try {
      await supabaseInsert('expenses', expenseRows);
      console.log(`Created ${debitTx.length} expense records`);
    } catch (e) {
      console.error('Error creating expenses:', e.message);
    }
  }
} else {
  console.log('No new transactions to insert');
}

console.log('Done');
