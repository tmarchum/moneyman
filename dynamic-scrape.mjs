/**
 * Dynamic Scraper Orchestrator
 *
 * Reads bank accounts from Supabase (bank_accounts table),
 * runs moneyman per building, pushes transactions, auto-matches, and triggers agents.
 *
 * No hardcoded credentials — everything comes from the management system.
 */

import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const WORKSPACE = process.env.GITHUB_WORKSPACE || process.cwd();
const OUTPUT_BASE = path.join(WORKSPACE, 'output');

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('ERROR: Missing SUPABASE_URL or SUPABASE_SERVICE_KEY');
  process.exit(1);
}

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

// Map Supabase credential field names to moneyman/israeli-bank-scrapers format
const CREDENTIAL_MAP = {
  pagi:     (c) => ({ username: c.username || c.userCode, password: c.password }),
  hapoalim: (c) => ({ userCode: c.userCode || c.username, password: c.password }),
  leumi:    (c) => ({ username: c.username, password: c.password }),
  mizrahi:  (c) => ({ username: c.username, password: c.password }),
  discount: (c) => ({ id: c.id, password: c.password, num: c.num }),
  otsarHahayal: (c) => ({ username: c.username, password: c.password }),
  beinleumi: (c) => ({ username: c.username, password: c.password }),
  massad:   (c) => ({ username: c.username, password: c.password }),
  yahav:    (c) => ({ username: c.username, password: c.password, nationalID: c.nationalID || c.id }),
};

function mapCredentials(bankType, creds) {
  const mapper = CREDENTIAL_MAP[bankType];
  if (mapper) return mapper(creds);
  // Fallback: pass through as-is
  return creds;
}

// ─── Step 1: Fetch accounts from Supabase ────────────────────────────────────

async function fetchAccounts() {
  const accounts = await supabaseGet('bank_accounts', 'is_active=eq.true&select=*');
  if (!accounts || accounts.length === 0) {
    console.log('No active bank accounts found in Supabase');
    process.exit(0);
  }
  console.log(`Found ${accounts.length} active bank account(s)`);

  // Group by building_id
  const byBuilding = {};
  for (const acc of accounts) {
    if (!byBuilding[acc.building_id]) byBuilding[acc.building_id] = [];
    byBuilding[acc.building_id].push(acc);
  }
  return byBuilding;
}

// ─── Step 2: Run moneyman for a building ─────────────────────────────────────

function runMoneyman(buildingId, accounts) {
  const outputDir = path.join(OUTPUT_BASE, buildingId);
  fs.mkdirSync(outputDir, { recursive: true });

  const moneymanAccounts = accounts.map(acc => ({
    companyId: acc.bank_type,
    ...mapCredentials(acc.bank_type, acc.credentials || {}),
  }));

  const config = {
    accounts: moneymanAccounts,
    storage: {
      localJson: { enabled: true, path: '/output' },
    },
    options: {
      scraping: { daysBack: 30 },
    },
  };

  const configJson = JSON.stringify(config);
  const label = accounts.map(a => a.label || a.bank_type).join(', ');
  console.log(`\n  Running moneyman for [${label}]...`);

  try {
    const result = execSync(
      `docker run --rm ` +
      `-v "${outputDir}:/output" ` +
      `-e MONEYMAN_CONFIG='${configJson.replace(/'/g, "'\\''")}' ` +
      `-e DEBUG=moneyman:* ` +
      `-e TZ=Asia/Jerusalem ` +
      `-e MONEYMAN_UNSAFE_STDOUT=true ` +
      `ghcr.io/daniel-hauser/moneyman:latest`,
      { encoding: 'utf8', timeout: 300000, stdio: ['pipe', 'pipe', 'pipe'] }
    );
    console.log(result.split('\n').filter(l => l.includes('transactions') || l.includes('accounts') || l.includes('✔')).join('\n'));
  } catch (err) {
    // moneyman may exit with error but still output transactions
    const stderr = err.stderr || '';
    const stdout = err.stdout || '';
    console.error(`  Warning: moneyman exited with code ${err.status}`);
    if (stderr) console.error(`  ${stderr.split('\n').slice(-3).join('\n  ')}`);
    // Check if output was still generated
    const hasOutput = fs.readdirSync(outputDir).some(f => f.endsWith('.json'));
    if (!hasOutput) {
      console.error(`  No output generated for building ${buildingId}`);
      return null;
    }
  }

  // Find latest JSON output
  const files = fs.readdirSync(outputDir).filter(f => f.endsWith('.json')).sort().reverse();
  if (files.length === 0) {
    console.log('  No transaction files generated');
    return null;
  }

  const filePath = path.join(outputDir, files[0]);
  const transactions = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  console.log(`  Got ${transactions.length} transactions`);

  // Update last_scraped_at for all accounts
  for (const acc of accounts) {
    fetch(`${SUPABASE_URL}/rest/v1/bank_accounts?id=eq.${acc.id}`, {
      method: 'PATCH', headers: { ...headers, 'Prefer': 'return=minimal' },
      body: JSON.stringify({ last_scraped_at: new Date().toISOString() }),
    }).catch(() => {});
  }

  return { transactions, outputDir };
}

// ─── Step 3: Push transactions to Supabase ───────────────────────────────────

async function pushTransactions(buildingId, transactions) {
  if (!transactions || transactions.length === 0) return 0;

  const rows = transactions.map(tx => {
    const txDate = tx.date ? tx.date.substring(0, 10) : null;
    const month = txDate ? txDate.substring(0, 7) : null;
    const amount = tx.chargedAmount || tx.originalAmount || 0;

    return {
      building_id: buildingId,
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
    `select=transaction_date,description,credit,debit&building_id=eq.${buildingId}`);

  const existingSet = new Set(
    (existing || []).map(e => `${e.transaction_date}|${e.description}|${e.credit}|${e.debit}`)
  );

  const newRows = rows.filter(r =>
    !existingSet.has(`${r.transaction_date}|${r.description}|${r.credit}|${r.debit}`)
  );

  console.log(`  New: ${newRows.length}, Duplicates skipped: ${rows.length - newRows.length}`);

  if (newRows.length > 0) {
    const insertedTx = await supabaseInsert('bank_transactions', newRows);
    console.log(`  Inserted ${newRows.length} transactions`);

    // Auto-create expense records from debit transactions
    const debitTx = (insertedTx || []).filter(tx => tx.debit > 0);
    if (debitTx.length > 0) {
      const expenseRows = debitTx.map(tx => ({
        building_id: buildingId,
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
        console.log(`  Created ${debitTx.length} expense records`);
      } catch (e) {
        console.error(`  Error creating expenses: ${e.message}`);
      }
    }
  }

  return newRows.length;
}

// ─── Step 4: Auto-match (inline, no SDK dependency) ──────────────────────────

async function autoMatch(buildingId) {
  console.log(`  Running auto-match...`);

  const [allTx, units, residents, buildings] = await Promise.all([
    supabaseGet('bank_transactions', `building_id=eq.${buildingId}&select=*`),
    supabaseGet('units', `building_id=eq.${buildingId}&select=id,number,monthly_fee,rooms,board_member`),
    supabaseGet('unit_residents', `select=unit_id,first_name,last_name,is_primary`),
    supabaseGet('buildings', `id=eq.${buildingId}&select=monthly_fee,fee_tiers,board_member_discount`),
  ]);

  const building = buildings?.[0];
  const baseFee = building?.monthly_fee || 440;
  const feeTiers = building?.fee_tiers || [];

  const resMap = {};
  (residents || []).forEach(r => {
    if (r.is_primary || !resMap[r.unit_id]) {
      resMap[r.unit_id] = `${r.first_name || ''} ${r.last_name || ''}`.trim();
    }
  });
  const unitMap = {};
  (units || []).forEach(u => { unitMap[u.id] = u; });

  function calcUnitFee(unit) {
    let fee = unit.monthly_fee || 0;
    if (!fee && feeTiers.length > 0) {
      const tier = feeTiers.find(t => t.rooms === unit.rooms);
      if (tier) fee = Number(tier.fee);
    }
    if (!fee) fee = baseFee;
    if (unit.board_member && building?.board_member_discount) {
      fee = fee * (1 - Number(building.board_member_discount) / 100);
    }
    return fee;
  }

  function normalizeDescription(desc) {
    if (!desc) return '';
    return desc.trim().replace(/\s+/g, ' ').replace(/[,."']/g, '').toLowerCase();
  }

  const GENERIC_WORDS = ['זיכוי', 'מיידי', 'מבנק', 'העברה', 'תשלום', 'הפקדה', 'שיק', 'צק'];
  function extractNameParts(desc) {
    if (!desc) return [];
    const cleaned = desc.replace(/זיכוי\s+מ[^\s]*\s+מ/g, '').replace(/[,."'\/\\]/g, ' ').replace(/\d{5,}/g, '').trim();
    return cleaned.split(/\s+/).filter(p => p.length > 2 && !GENERIC_WORDS.includes(p));
  }

  const SKIP_PATTERNS = ['החזרת שיק', 'משיכת שיק', 'הורא.קבע', 'חברת החשמל', 'עמלה', 'ריבית'];
  function shouldSkip(tx) {
    const desc = tx.description || '';
    return SKIP_PATTERNS.some(p => desc.includes(p));
  }

  // Learn patterns from matched tx
  const matchedTx = (allTx || []).filter(tx => tx.match_status === 'matched' && tx.unit_id);
  const patterns = {};
  const nameToUnit = {};
  const confirmedPatterns = {};

  matchedTx.forEach(tx => {
    const key = normalizeDescription(tx.description);
    if (key && key.length > 5) {
      patterns[key] = tx.unit_id;
      const day = tx.transaction_date ? new Date(tx.transaction_date).getDate() : null;
      confirmedPatterns[key] = { unitId: tx.unit_id, day, amount: Number(tx.credit) || 0 };
    }
    const parts = extractNameParts(tx.description);
    if (parts.length >= 2) {
      const nk = parts.sort().join(' ');
      if (nk.length > 3) nameToUnit[nk] = tx.unit_id;
    }
  });

  function scoreMatch(tx) {
    const key = normalizeDescription(tx.description);
    const credit = Number(tx.credit) || 0;
    const txParts = extractNameParts(tx.description);
    const txDay = tx.transaction_date ? new Date(tx.transaction_date).getDate() : null;

    if (key && confirmedPatterns[key]) {
      const cp = confirmedPatterns[key];
      const unit = unitMap[cp.unitId];
      if (unit) {
        const fee = calcUnitFee(unit);
        if (credit > fee * 2) return null;
        const dayMatch = cp.day && txDay && Math.abs(cp.day - txDay) <= 3;
        const amountMatch = Math.abs(credit - cp.amount) < 50;
        return { unitId: cp.unitId, confidence: 'high', reason: `תיאור זהה${dayMatch ? ' + תאריך' : ''}${amountMatch ? ' + סכום' : ''}` };
      }
    }

    for (const [nameKey, uid] of Object.entries(nameToUnit)) {
      const knownParts = nameKey.split(' ');
      const overlap = knownParts.filter(p => p.length >= 3 && txParts.some(tp => tp.length >= 3 && (tp.includes(p) || p.includes(tp))));
      if (overlap.length >= 2) {
        const unit = unitMap[uid];
        if (!unit) continue;
        const fee = calcUnitFee(unit);
        if (credit > fee * 2) continue;
        const ratio = credit / fee;
        if (ratio >= 0.7 && ratio <= 1.3) return { unitId: uid, confidence: 'high', reason: `שם מאושר (${overlap.join(', ')}) + סכום` };
        return { unitId: uid, confidence: 'medium', reason: `שם מאושר (${overlap.join(', ')})` };
      }
    }

    const desc = (tx.description || '').toLowerCase();
    for (const unit of (units || [])) {
      const name = (resMap[unit.id] || '').trim();
      if (!name) continue;
      const parts = name.split(' ').filter(p => p.length >= 3);
      const matching = parts.filter(p => desc.includes(p.toLowerCase()));
      if (parts.length >= 2 && matching.length >= 2) {
        const fee = calcUnitFee(unit);
        if (credit > fee * 2) continue;
        const ratio = credit / fee;
        if (ratio >= 0.8 && ratio <= 1.2) return { unitId: unit.id, confidence: 'medium', reason: `שם מ-DB (${matching.join(', ')}) + סכום` };
        return { unitId: unit.id, confidence: 'low', reason: `שם מ-DB (${matching.join(', ')})` };
      }
    }
    return null;
  }

  const unmatched = (allTx || []).filter(tx => tx.match_status === 'unmatched' && Number(tx.credit) > 0);
  let autoMatched = 0, suggested = 0;

  for (const tx of unmatched) {
    if (shouldSkip(tx)) continue;
    const result = scoreMatch(tx);
    if (!result) continue;

    const { unitId, confidence, reason } = result;
    const unit = unitMap[unitId];
    if (!unit) continue;

    const existingMatch = matchedTx.find(t => t.unit_id === unitId && t.month === tx.month);
    if (existingMatch && normalizeDescription(existingMatch.description) !== normalizeDescription(tx.description)) continue;

    const newStatus = confidence === 'high' ? 'matched' : 'suggested';
    await fetch(`${SUPABASE_URL}/rest/v1/bank_transactions?id=eq.${tx.id}`, {
      method: 'PATCH',
      headers: { ...headers, 'Prefer': 'return=minimal' },
      body: JSON.stringify({ unit_id: unitId, match_status: newStatus }),
    });

    if (confidence === 'high') {
      const key = normalizeDescription(tx.description);
      if (key) patterns[key] = unitId;
      matchedTx.push({ ...tx, unit_id: unitId, match_status: 'matched' });
      autoMatched++;
    } else {
      suggested++;
    }
  }

  // Sync payments
  const { data: allMatched } = await fetch(
    `${SUPABASE_URL}/rest/v1/bank_transactions?building_id=eq.${buildingId}&match_status=eq.matched&credit=gt.0&select=unit_id,month,credit`,
    { headers }
  ).then(r => r.json().then(d => ({ data: d })));

  const groups = {};
  (allMatched || []).forEach(tx => {
    if (!tx.unit_id || !tx.month) return;
    const k = `${tx.unit_id}|${tx.month}`;
    if (!groups[k]) groups[k] = { unitId: tx.unit_id, month: tx.month, total: 0 };
    groups[k].total += Number(tx.credit) || 0;
  });

  const existingPayments = await supabaseGet('payments', `building_id=eq.${buildingId}&select=*`);
  let created = 0;

  for (const g of Object.values(groups)) {
    const unit = unitMap[g.unitId];
    if (!unit) continue;
    const fee = calcUnitFee(unit);
    const status = g.total >= fee ? 'paid' : 'partial';
    const existing = (existingPayments || []).find(p => p.unit_id === g.unitId && p.month === g.month);
    if (existing) {
      if (existing.status === 'paid' && Number(existing.amount) >= g.total) continue;
      await fetch(`${SUPABASE_URL}/rest/v1/payments?id=eq.${existing.id}`, {
        method: 'PATCH',
        headers: { ...headers, 'Prefer': 'return=minimal' },
        body: JSON.stringify({ amount: g.total, status, paid_at: new Date().toISOString().split('T')[0], method: 'הוראת קבע' }),
      });
    } else {
      try {
        await supabaseInsert('payments', [{
          building_id: buildingId, unit_id: g.unitId, amount: g.total,
          month: g.month, status, paid_at: new Date().toISOString().split('T')[0], method: 'הוראת קבע',
        }]);
        created++;
      } catch (_) {}
    }
  }

  console.log(`  Auto-matched: ${autoMatched}, Suggested: ${suggested}, Payments created: ${created}`);
}

// ─── Step 5: Run managed agents ──────────────────────────────────────────────

async function runManagedAgents(buildingIds) {
  if (!ANTHROPIC_API_KEY) {
    console.log('\nSkipping managed agents (no ANTHROPIC_API_KEY)');
    return;
  }

  // Import and run the agents script inline
  const API_BASE = 'https://api.anthropic.com/v1';
  const AGENT_HEADERS = {
    'x-api-key': ANTHROPIC_API_KEY,
    'anthropic-beta': 'managed-agents-2026-04-01',
    'anthropic-version': '2023-06-01',
    'content-type': 'application/json',
  };
  const ENV_ID = 'env_01LMDFfVfgEKUFSYBUBVof9a';

  const AGENTS = [
    { id: 'agent_011Ca2LtM3B3VQDyKnDvm7Sv', name: 'אנליסט פיננסי' },
    { id: 'agent_011Ca2Lu1GK1Pmoj6rXwnBY7', name: 'מנהל גבייה' },
  ];

  for (const buildingId of buildingIds) {
    console.log(`\n=== Agents for building ${buildingId} ===`);

    for (const agent of AGENTS) {
      console.log(`  Running ${agent.name}...`);
      const startTime = Date.now();

      try {
        const session = await fetch(`${API_BASE}/sessions`, {
          method: 'POST', headers: AGENT_HEADERS,
          body: JSON.stringify({ agent: agent.id, environment_id: ENV_ID }),
        }).then(r => { if (!r.ok) throw new Error(`${r.status}`); return r.json(); });

        const year = new Date().getFullYear();
        const currentMonth = `${year}-${String(new Date().getMonth() + 1).padStart(2, '0')}`;
        const prompt = agent.name === 'אנליסט פיננסי'
          ? `נתח את המצב הפיננסי של בניין ${buildingId}. קרא פרטי בניין, הוצאות והכנסות ${year}. זהה חריגות, בדוק מאזן, חפש חיסכון. כתוב התראות עם write_alerts.`
          : `נהל גבייה לבניין ${buildingId}. קרא דירות, תשלומים ותיקי גבייה. בדוק חובות מ-${year}-01 עד ${currentMonth}. צור/עדכן תיקי גבייה, שלח מיילים, כתוב התראות.`;

        await fetch(`${API_BASE}/sessions/${session.id}/events`, {
          method: 'POST', headers: AGENT_HEADERS,
          body: JSON.stringify({ events: [{ type: 'user.message', content: [{ type: 'text', text: prompt }] }] }),
        });

        // Poll for completion
        const maxWait = 600000;
        const start = Date.now();
        while (Date.now() - start < maxWait) {
          const evts = await fetch(`${API_BASE}/sessions/${session.id}/events`, { headers: AGENT_HEADERS })
            .then(r => r.json());
          const events = evts.data || [];
          if (events.find(e => e.type === 'session.status_idle')) {
            const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
            console.log(`  ✓ ${agent.name} completed in ${elapsed}s`);
            break;
          }
          if (events.find(e => e.type === 'session.error' || e.type === 'error')) {
            throw new Error('Agent error');
          }
          await new Promise(r => setTimeout(r, 3000));
        }
      } catch (err) {
        console.error(`  ✗ ${agent.name}: ${err.message}`);
      }
    }
  }
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log('=== Dynamic Scraper Orchestrator ===');
  console.log(`Date: ${new Date().toISOString()}\n`);

  // 1. Fetch accounts
  const accountsByBuilding = await fetchAccounts();
  const buildingIds = Object.keys(accountsByBuilding);
  console.log(`Buildings: ${buildingIds.length}`);

  let totalNew = 0;

  // 2. Process each building
  for (const [buildingId, accounts] of Object.entries(accountsByBuilding)) {
    const labels = accounts.map(a => a.label || a.bank_type).join(', ');
    console.log(`\n══════════════════════════════════════`);
    console.log(`Building: ${buildingId}`);
    console.log(`Accounts: ${labels}`);

    // 2a. Scrape
    const result = runMoneyman(buildingId, accounts);
    if (!result) {
      console.log('  Skipping (no transactions)');
      continue;
    }

    // 2b. Push to Supabase
    const newCount = await pushTransactions(buildingId, result.transactions);
    totalNew += newCount;

    // 2c. Auto-match
    await autoMatch(buildingId);
  }

  // 3. Run managed agents for all buildings
  if (totalNew > 0 || process.env.FORCE_AGENTS === 'true') {
    await runManagedAgents(buildingIds);
  } else {
    console.log('\nNo new transactions — skipping agents');
  }

  console.log('\n=== Done ===');
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
