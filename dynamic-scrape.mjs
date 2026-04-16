/**
 * Dynamic Scraper Orchestrator
 *
 * Reads bank accounts from Supabase (bank_accounts table),
 * runs moneyman per building, pushes transactions, auto-matches, and triggers agents.
 *
 * No hardcoded credentials — everything comes from the management system.
 */

import { execSync } from "child_process";
import fs from "fs";
import path from "path";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const WORKSPACE = process.env.GITHUB_WORKSPACE || process.cwd();
const OUTPUT_BASE = path.join(WORKSPACE, "output");

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("ERROR: Missing SUPABASE_URL or SUPABASE_SERVICE_KEY");
  process.exit(1);
}

const headers = {
  apikey: SUPABASE_SERVICE_KEY,
  Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
  "Content-Type": "application/json",
  Prefer: "return=representation",
};

async function supabaseGet(table, query = "") {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}?${query}`, {
    headers,
  });
  if (!res.ok)
    throw new Error(`GET ${table}: ${res.status} ${await res.text()}`);
  return res.json();
}

async function supabaseInsert(table, rows) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
    method: "POST",
    headers,
    body: JSON.stringify(rows),
  });
  if (!res.ok)
    throw new Error(`INSERT ${table}: ${res.status} ${await res.text()}`);
  return res.json();
}

// Map Supabase credential field names to moneyman/israeli-bank-scrapers format
const CREDENTIAL_MAP = {
  pagi: (c) => ({ username: c.username || c.userCode, password: c.password }),
  hapoalim: (c) => ({
    userCode: c.userCode || c.username,
    password: c.password,
  }),
  leumi: (c) => ({ username: c.username, password: c.password }),
  mizrahi: (c) => ({ username: c.username, password: c.password }),
  discount: (c) => ({ id: c.id, password: c.password, num: c.num }),
  otsarHahayal: (c) => ({ username: c.username, password: c.password }),
  beinleumi: (c) => ({ username: c.username, password: c.password }),
  massad: (c) => ({ username: c.username, password: c.password }),
  yahav: (c) => ({
    username: c.username,
    password: c.password,
    nationalID: c.nationalID || c.id,
  }),
};

function mapCredentials(bankType, creds) {
  const mapper = CREDENTIAL_MAP[bankType];
  if (mapper) return mapper(creds);
  // Fallback: pass through as-is
  return creds;
}

// ─── Step 1: Fetch accounts from Supabase ────────────────────────────────────

async function fetchAccounts() {
  const accounts = await supabaseGet(
    "bank_accounts",
    "is_active=eq.true&select=*",
  );
  if (!accounts || accounts.length === 0) {
    console.log("No active bank accounts found in Supabase");
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

  const moneymanAccounts = accounts.map((acc) => ({
    companyId: acc.bank_type,
    ...mapCredentials(acc.bank_type, acc.credentials || {}),
  }));

  const config = {
    accounts: moneymanAccounts,
    storage: {
      localJson: { enabled: true, path: "/output" },
    },
    options: {
      scraping: { daysBack: 30 },
    },
  };

  const configJson = JSON.stringify(config);
  const label = accounts.map((a) => a.label || a.bank_type).join(", ");
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
      { encoding: "utf8", timeout: 300000, stdio: ["pipe", "pipe", "pipe"] },
    );
    console.log(
      result
        .split("\n")
        .filter(
          (l) =>
            l.includes("transactions") ||
            l.includes("accounts") ||
            l.includes("✔"),
        )
        .join("\n"),
    );
  } catch (err) {
    // moneyman may exit with error but still output transactions
    const stderr = err.stderr || "";
    const stdout = err.stdout || "";
    console.error(`  Warning: moneyman exited with code ${err.status}`);
    if (stderr) console.error(`  ${stderr.split("\n").slice(-3).join("\n  ")}`);
    // Check if output was still generated
    const hasOutput = fs
      .readdirSync(outputDir)
      .some((f) => f.endsWith(".json"));
    if (!hasOutput) {
      console.error(`  No output generated for building ${buildingId}`);
      return null;
    }
  }

  // Find latest JSON output
  const files = fs
    .readdirSync(outputDir)
    .filter((f) => f.endsWith(".json"))
    .sort()
    .reverse();
  if (files.length === 0) {
    console.log("  No transaction files generated");
    return null;
  }

  const filePath = path.join(outputDir, files[0]);
  const transactions = JSON.parse(fs.readFileSync(filePath, "utf8"));
  console.log(`  Got ${transactions.length} transactions`);

  // Update last_scraped_at for all accounts
  for (const acc of accounts) {
    fetch(`${SUPABASE_URL}/rest/v1/bank_accounts?id=eq.${acc.id}`, {
      method: "PATCH",
      headers: { ...headers, Prefer: "return=minimal" },
      body: JSON.stringify({ last_scraped_at: new Date().toISOString() }),
    }).catch(() => {});
  }

  return { transactions, outputDir };
}

// ─── Step 3: Push transactions to Supabase ───────────────────────────────────

async function pushTransactions(buildingId, transactions) {
  if (!transactions || transactions.length === 0) return 0;

  const rows = transactions
    .map((tx) => {
      const txDate = tx.date ? tx.date.substring(0, 10) : null;
      const month = txDate ? txDate.substring(0, 7) : null;
      const amount = tx.chargedAmount || tx.originalAmount || 0;

      return {
        building_id: buildingId,
        transaction_date: txDate,
        description: tx.description || "",
        credit: amount > 0 ? amount : 0,
        debit: amount < 0 ? Math.abs(amount) : 0,
        source: tx.memo || null,
        notes: tx.status === "pending" ? "ממתינה" : null,
        month: month,
        match_status: "unmatched",
      };
    })
    .filter((r) => r.transaction_date);

  // Check existing to avoid duplicates
  // Use month+description+amount (not exact date) because value date can shift by 1 day
  const existing = await supabaseGet(
    "bank_transactions",
    `select=month,description,credit,debit&building_id=eq.${buildingId}`,
  );

  const existingSet = new Set(
    (existing || []).map(
      (e) => `${e.month}|${e.description}|${e.credit}|${e.debit}`,
    ),
  );

  const newRows = rows.filter(
    (r) =>
      !existingSet.has(`${r.month}|${r.description}|${r.credit}|${r.debit}`),
  );

  console.log(
    `  New: ${newRows.length}, Duplicates skipped: ${rows.length - newRows.length}`,
  );

  if (newRows.length > 0) {
    const insertedTx = await supabaseInsert("bank_transactions", newRows);
    console.log(`  Inserted ${newRows.length} transactions`);

    // Auto-create expense records from debit transactions
    const debitTx = (insertedTx || []).filter((tx) => tx.debit > 0);
    if (debitTx.length > 0) {
      const expenseRows = debitTx.map((tx) => ({
        building_id: buildingId,
        date: tx.transaction_date,
        description: tx.description || "",
        amount: tx.debit,
        category: "אחר",
        source: "bank",
        bank_transaction_id: tx.id,
        notes: "",
      }));
      try {
        await supabaseInsert("expenses", expenseRows);
        console.log(`  Created ${debitTx.length} expense records`);
      } catch (e) {
        console.error(`  Error creating expenses: ${e.message}`);
      }
    }
  }

  return newRows.length;
}

// ─── Step 3b: Remove duplicate transactions ──────────────────────────────────

async function deduplicateTransactions(buildingId) {
  console.log(`  Checking for duplicates...`);

  const allTx = await supabaseGet(
    "bank_transactions",
    `building_id=eq.${buildingId}&select=id,month,description,credit,debit,created_at&order=created_at.asc`,
  );

  // Group by month+description+credit+debit — keep the oldest, delete the rest
  const seen = {};
  const toDelete = [];

  for (const tx of allTx || []) {
    const key = `${tx.month}|${tx.description}|${tx.credit}|${tx.debit}`;
    if (seen[key]) {
      toDelete.push(tx.id);
    } else {
      seen[key] = tx.id;
    }
  }

  if (toDelete.length === 0) {
    console.log(`  No duplicates found`);
    return;
  }

  console.log(`  Found ${toDelete.length} duplicates to remove`);

  // Delete in batches of 50
  for (let i = 0; i < toDelete.length; i += 50) {
    const batch = toDelete.slice(i, i + 50);
    const idList = batch.join(",");

    // Delete linked expenses first
    await fetch(
      `${SUPABASE_URL}/rest/v1/expenses?bank_transaction_id=in.(${idList})`,
      {
        method: "DELETE",
        headers: { ...headers, Prefer: "return=minimal" },
      },
    );

    // Delete the duplicate transactions
    await fetch(`${SUPABASE_URL}/rest/v1/bank_transactions?id=in.(${idList})`, {
      method: "DELETE",
      headers: { ...headers, Prefer: "return=minimal" },
    });
  }

  console.log(`  Removed ${toDelete.length} duplicate transactions`);
}

// ─── Step 4: Auto-match (inline, no SDK dependency) ──────────────────────────

async function autoMatch(buildingId) {
  console.log(`  Running auto-match...`);

  const [allTx, units, residents, buildings] = await Promise.all([
    supabaseGet("bank_transactions", `building_id=eq.${buildingId}&select=*`),
    supabaseGet(
      "units",
      `building_id=eq.${buildingId}&select=id,number,monthly_fee,rooms,board_member`,
    ),
    supabaseGet(
      "unit_residents",
      `select=unit_id,first_name,last_name,is_primary`,
    ),
    supabaseGet(
      "buildings",
      `id=eq.${buildingId}&select=monthly_fee,fee_tiers,board_member_discount`,
    ),
  ]);

  const building = buildings?.[0];
  const baseFee = building?.monthly_fee || 440;
  const feeTiers = building?.fee_tiers || [];

  // Build resident name map (all residents per unit, not just primary)
  const resMap = {}; // unit_id → primary name
  const allResNames = {}; // unit_id → [all names]
  (residents || []).forEach((r) => {
    const fullName = `${r.first_name || ""} ${r.last_name || ""}`.trim();
    if (!fullName) return;
    if (!allResNames[r.unit_id]) allResNames[r.unit_id] = [];
    allResNames[r.unit_id].push(fullName);
    if (r.is_primary || !resMap[r.unit_id]) resMap[r.unit_id] = fullName;
  });

  const unitMap = {};
  (units || []).forEach((u) => {
    unitMap[u.id] = u;
  });

  function calcUnitFee(unit) {
    let fee = unit.monthly_fee || 0;
    if (!fee && feeTiers.length > 0) {
      const tier = feeTiers.find((t) => t.rooms === unit.rooms);
      if (tier) fee = Number(tier.fee);
    }
    if (!fee) fee = baseFee;
    if (unit.board_member && building?.board_member_discount) {
      fee = fee * (1 - Number(building.board_member_discount) / 100);
    }
    return fee;
  }

  function normalizeDescription(desc) {
    if (!desc) return "";
    return desc
      .trim()
      .replace(/\s+/g, " ")
      .replace(/[,."']/g, "")
      .toLowerCase();
  }

  const NOISE_WORDS = [
    "זיכוי",
    "מיידי",
    "מבנק",
    "העברה",
    "תשלום",
    "הפקדה",
    "שיק",
    "צק",
    "ואו",
    "שה",
    "ו/או",
  ];
  function extractNameParts(desc) {
    if (!desc) return [];
    const cleaned = desc
      .replace(/זיכוי\s+מ[^\s]*\s+מ/g, "") // strip "זיכוי מבנק X מ"
      .replace(/[,."'\/\\-]/g, " ")
      .replace(/\d{5,}/g, "")
      .trim();
    return cleaned
      .split(/\s+/)
      .filter((p) => p.length > 2 && !NOISE_WORDS.includes(p));
  }

  // Skip only non-payment transactions (expenses, fees, bounced checks)
  const SKIP_PATTERNS = [
    "החזרת שיק",
    "משיכת שיק",
    "חברת החשמל",
    "עמלה",
    "ריבית",
  ];
  function shouldSkip(tx) {
    const desc = tx.description || "";
    return SKIP_PATTERNS.some((p) => desc.includes(p));
  }

  // ── Matching engine ──
  // Runs in a loop: each match teaches the next round (chain learning)

  const matchedTx = (allTx || []).filter(
    (tx) => tx.match_status === "matched" && tx.unit_id,
  );

  function rebuildPatterns() {
    const descToUnit = {};
    const namePartsToUnit = {};
    matchedTx.forEach((tx) => {
      const key = normalizeDescription(tx.description);
      if (key && key.length > 5) descToUnit[key] = tx.unit_id;
      const parts = extractNameParts(tx.description);
      if (parts.length >= 1) {
        const nk = parts.sort().join(" ");
        if (nk.length > 3) namePartsToUnit[nk] = tx.unit_id;
      }
    });
    return { descToUnit, namePartsToUnit };
  }

  function scoreMatch(tx, descToUnit, namePartsToUnit) {
    const key = normalizeDescription(tx.description);
    const credit = Number(tx.credit) || 0;
    const txParts = extractNameParts(tx.description);

    // ── Tier 1: Exact description match from previously matched tx ──
    if (key && descToUnit[key]) {
      const unitId = descToUnit[key];
      const unit = unitMap[unitId];
      if (unit && credit <= calcUnitFee(unit) * 3) {
        return { unitId, confidence: "high", reason: "תיאור זהה למאושר" };
      }
    }

    // ── Tier 2: Name parts overlap with matched patterns ──
    for (const [nameKey, uid] of Object.entries(namePartsToUnit)) {
      const knownParts = nameKey.split(" ");
      const overlap = knownParts.filter(
        (p) =>
          p.length >= 3 &&
          txParts.some(
            (tp) => tp.length >= 3 && (tp.includes(p) || p.includes(tp)),
          ),
      );
      // 2+ matching parts, or 1 part of 4+ chars with matching amount
      if (
        overlap.length >= 2 ||
        (overlap.length === 1 && overlap[0].length >= 4)
      ) {
        const unit = unitMap[uid];
        if (!unit) continue;
        const fee = calcUnitFee(unit);
        if (credit > fee * 3) continue;
        if (overlap.length >= 2)
          return {
            unitId: uid,
            confidence: "high",
            reason: `שם (${overlap.join(", ")})`,
          };
        // Single strong match — need amount to be in range
        const ratio = credit / fee;
        if (ratio >= 0.5 && ratio <= 1.5)
          return {
            unitId: uid,
            confidence: "high",
            reason: `שם (${overlap[0]}) + סכום`,
          };
      }
    }

    // ── Tier 3: Match against resident DB names ──
    const desc = (tx.description || "").toLowerCase();
    const tier3Candidates = [];
    for (const unit of units || []) {
      const names = allResNames[unit.id] || [];
      for (const name of names) {
        const parts = name.split(" ").filter((p) => p.length >= 3);
        if (parts.length === 0) continue;
        const matching = parts.filter((p) => desc.includes(p.toLowerCase()));
        if (
          matching.length >= 2 ||
          (matching.length === 1 && matching[0].length >= 4)
        ) {
          const fee = calcUnitFee(unit);
          if (credit > fee * 3) continue;
          tier3Candidates.push({ unitId: unit.id, matching, fee });
        }
      }
    }

    // Same-name disambiguation: if name matches 2+ units → suggested, not auto-matched
    if (tier3Candidates.length === 1) {
      const c = tier3Candidates[0];
      if (c.matching.length >= 2)
        return {
          unitId: c.unitId,
          confidence: "high",
          reason: `שם DB (${c.matching.join(", ")})`,
        };
      const ratio = credit / c.fee;
      if (ratio >= 0.5 && ratio <= 1.5)
        return {
          unitId: c.unitId,
          confidence: "medium",
          reason: `שם DB (${c.matching[0]}) + סכום`,
        };
    } else if (tier3Candidates.length > 1) {
      // Multiple units match same name — flag for manual review
      const unitNums = tier3Candidates
        .map((c) => unitMap[c.unitId]?.number)
        .join(",");
      return {
        unitId: tier3Candidates[0].unitId,
        confidence: "low",
        reason: `שם תואם דירות ${unitNums} — לבדיקה ידנית`,
      };
    }

    return null;
  }

  let totalMatched = 0,
    totalSuggested = 0;

  // Run matching in rounds — each round's matches feed the next
  for (let round = 1; round <= 5; round++) {
    const { descToUnit, namePartsToUnit } = rebuildPatterns();
    const unmatched = (allTx || []).filter(
      (tx) => tx.match_status === "unmatched" && Number(tx.credit) > 0,
    );

    let roundMatched = 0;
    for (const tx of unmatched) {
      if (shouldSkip(tx)) continue;
      const result = scoreMatch(tx, descToUnit, namePartsToUnit);
      if (!result) continue;

      const { unitId, confidence, reason } = result;
      const unit = unitMap[unitId];
      if (!unit) continue;

      const newStatus = confidence === "high" ? "matched" : "suggested";
      const notes = reason || null;
      await fetch(`${SUPABASE_URL}/rest/v1/bank_transactions?id=eq.${tx.id}`, {
        method: "PATCH",
        headers: { ...headers, Prefer: "return=minimal" },
        body: JSON.stringify({
          unit_id: unitId,
          match_status: newStatus,
          notes,
        }),
      });

      tx.match_status = newStatus;
      tx.unit_id = unitId;

      if (confidence === "high") {
        matchedTx.push(tx);
        roundMatched++;
        totalMatched++;
      } else {
        totalSuggested++;
      }
    }

    if (roundMatched === 0) break; // No new matches — stop
    console.log(`    round ${round}: ${roundMatched} matched`);
  }

  // ── Sync payments ──
  const finalMatched = (allTx || []).filter(
    (tx) =>
      tx.match_status === "matched" && tx.unit_id && Number(tx.credit) > 0,
  );
  const groups = {};
  finalMatched.forEach((tx) => {
    const k = `${tx.unit_id}|${tx.month}`;
    if (!groups[k])
      groups[k] = { unitId: tx.unit_id, month: tx.month, total: 0 };
    groups[k].total += Number(tx.credit) || 0;
  });

  const existingPayments = await supabaseGet(
    "payments",
    `building_id=eq.${buildingId}&select=*`,
  );
  let created = 0;
  const largePayments = [];

  for (const g of Object.values(groups)) {
    const unit = unitMap[g.unitId];
    if (!unit) continue;
    const fee = calcUnitFee(unit);

    // Flag large payments (>1.5x fee) — might cover multiple months
    if (g.total > fee * 1.5) {
      const monthsCovered = Math.round(g.total / fee);
      largePayments.push({
        unit: unit.number,
        amount: g.total,
        fee,
        month: g.month,
        possibleMonths: monthsCovered,
      });
    }

    const status = g.total >= fee ? "paid" : "partial";
    const existing = (existingPayments || []).find(
      (p) => p.unit_id === g.unitId && p.month === g.month,
    );
    const paymentNotes =
      g.total > fee * 1.5
        ? `תשלום חריג: ${g.total}₪ (תעריף ${fee}₪, ייתכן ${Math.round(g.total / fee)} חודשים)`
        : null;

    if (existing) {
      if (existing.status === "paid" && Number(existing.amount) >= g.total)
        continue;
      await fetch(`${SUPABASE_URL}/rest/v1/payments?id=eq.${existing.id}`, {
        method: "PATCH",
        headers: { ...headers, Prefer: "return=minimal" },
        body: JSON.stringify({
          amount: g.total,
          status,
          paid_at: new Date().toISOString().split("T")[0],
          method: "הוראת קבע",
          ...(paymentNotes ? { notes: paymentNotes } : {}),
        }),
      });
    } else {
      try {
        await supabaseInsert("payments", [
          {
            building_id: buildingId,
            unit_id: g.unitId,
            amount: g.total,
            month: g.month,
            status,
            paid_at: new Date().toISOString().split("T")[0],
            method: "הוראת קבע",
            ...(paymentNotes ? { notes: paymentNotes } : {}),
          },
        ]);
        created++;
      } catch (_) {}
    }
  }

  if (largePayments.length > 0) {
    console.log(
      `  ⚠ ${largePayments.length} large payment(s) flagged for review:`,
    );
    for (const lp of largePayments) {
      console.log(
        `    דירה ${lp.unit}: ${lp.amount}₪ (חודש ${lp.month}, תעריף ${lp.fee}₪, ~${lp.possibleMonths} חודשים)`,
      );
    }
  }

  console.log(
    `  Auto-matched: ${totalMatched}, Suggested: ${totalSuggested}, Payments: ${created}`,
  );
}

// ─── Step 5: Collection analysis (code, not agent) ──────────────────────────

async function runCollectionAnalysis(buildingId) {
  console.log(`  Running collection analysis...`);

  const year = new Date().getFullYear();
  const currentMonth = new Date().getMonth() + 1; // 1-based

  const [units, residents, payments, cases, buildings] = await Promise.all([
    supabaseGet(
      "units",
      `building_id=eq.${buildingId}&select=id,number,rooms,board_member,monthly_fee`,
    ),
    supabaseGet(
      "unit_residents",
      `select=unit_id,first_name,last_name,is_primary`,
    ),
    supabaseGet("payments", `building_id=eq.${buildingId}&select=*`),
    supabaseGet("collection_cases", `building_id=eq.${buildingId}&select=*`),
    supabaseGet(
      "buildings",
      `id=eq.${buildingId}&select=monthly_fee,fee_tiers,board_member_discount`,
    ),
  ]);

  const building = buildings?.[0];
  const feeTiers = building?.fee_tiers || [];
  const baseFee = building?.monthly_fee || 440;

  function calcFee(unit) {
    let fee = unit.monthly_fee || 0;
    if (!fee && feeTiers.length > 0) {
      const tier = feeTiers.find((t) => t.rooms === unit.rooms);
      if (tier) fee = Number(tier.fee);
    }
    if (!fee) fee = baseFee;
    if (unit.board_member && building?.board_member_discount) {
      fee = fee * (1 - Number(building.board_member_discount) / 100);
    }
    return fee;
  }

  // Build resident name map for display
  const resNames = {};
  (residents || []).forEach((r) => {
    if (r.is_primary || !resNames[r.unit_id]) {
      resNames[r.unit_id] = `${r.first_name || ""} ${r.last_name || ""}`.trim();
    }
  });

  // Build payment lookup: unit_id+month → amount
  const paymentMap = {};
  (payments || []).forEach((p) => {
    const key = `${p.unit_id}|${p.month}`;
    paymentMap[key] = (paymentMap[key] || 0) + Number(p.amount || 0);
  });

  // Existing cases lookup: unit_id → case
  const caseMap = {};
  (cases || []).forEach((c) => {
    if (c.status !== "closed") caseMap[c.unit_id] = c;
  });

  let totalDebt = 0;
  let unitsWithDebt = 0;
  let unitsWithDiff = 0;
  let casesCreated = 0;
  let casesClosed = 0;

  for (const unit of units || []) {
    const fee = calcFee(unit);
    const unpaidMonths = [];
    const diffMonths = [];

    for (let m = 1; m <= currentMonth; m++) {
      const monthStr = `${year}-${String(m).padStart(2, "0")}`;
      const paid = paymentMap[`${unit.id}|${monthStr}`] || 0;

      if (paid === 0) {
        unpaidMonths.push(monthStr);
      } else if (paid < fee * 0.9) {
        diffMonths.push({
          month: monthStr,
          paid,
          expected: fee,
          diff: fee - paid,
        });
      }
    }

    const diffDebt = diffMonths.reduce((s, d) => s + d.diff, 0);
    const unpaidDebt = unpaidMonths.length * fee;
    const debt = unpaidDebt + diffDebt;

    if (unpaidMonths.length > 0 || diffMonths.length > 0) {
      // Real debt — create/update collection case
      if (unpaidMonths.length > 0) unitsWithDebt++;
      if (diffMonths.length > 0) unitsWithDiff++;
      totalDebt += debt;
      const resName = resNames[unit.id] || "";
      const existingCase = caseMap[unit.id];
      const caseData = {
        building_id: buildingId,
        unit_id: unit.id,
        unit_number: String(unit.number),
        resident_name: resName,
        status: "open",
        escalation_level: existingCase?.escalation_level || "reminder",
        total_debt: debt,
        months_overdue: unpaidMonths.length + diffMonths.length,
        unpaid_months: [
          ...unpaidMonths,
          ...diffMonths.map((d) => d.month),
        ],
      };

      if (existingCase) {
        await fetch(
          `${SUPABASE_URL}/rest/v1/collection_cases?id=eq.${existingCase.id}`,
          {
            method: "PATCH",
            headers: { ...headers, Prefer: "return=minimal" },
            body: JSON.stringify(caseData),
          },
        );
      } else {
        try {
          await supabaseInsert("collection_cases", [caseData]);
          casesCreated++;
        } catch (_) {}
      }
    } else if (caseMap[unit.id]) {
      // All paid — close existing case
      await fetch(
        `${SUPABASE_URL}/rest/v1/collection_cases?id=eq.${caseMap[unit.id].id}`,
        {
          method: "PATCH",
          headers: { ...headers, Prefer: "return=minimal" },
          body: JSON.stringify({
            status: "closed",
            total_debt: 0,
            months_overdue: 0,
            unpaid_months: [],
          }),
        },
      );
      casesClosed++;
    }
  }

  // Write summary alert
  try {
    await supabaseInsert("agent_alerts", [
      {
        building_id: buildingId,
        agent_type: "collection",
        severity: unitsWithDebt > 0 ? "high" : "low",
        title: `סיכום גבייה — ${new Date().toISOString().split("T")[0]}`,
        message: `דירות חייבות: ${unitsWithDebt}, סכום חוב כולל: ${totalDebt}₪, הפרשים לבירור: ${unitsWithDiff}, תיקים חדשים: ${casesCreated}, תיקים שנסגרו: ${casesClosed}`,
      },
    ]);
  } catch (_) {}

  console.log(
    `  Collection: ${unitsWithDebt} debts (${totalDebt}₪), ${unitsWithDiff} diffs, ${casesCreated} new cases, ${casesClosed} closed`,
  );
}

// ─── Step 6: Finance analysis (code, not agent) ─────────────────────────────

async function runFinanceAnalysis(buildingId) {
  console.log(`  Running finance analysis...`);

  const year = new Date().getFullYear();

  const [expenses, payments, buildings] = await Promise.all([
    supabaseGet(
      "expenses",
      `building_id=eq.${buildingId}&select=amount,category,date,description`,
    ),
    supabaseGet("payments", `building_id=eq.${buildingId}&select=amount,month`),
    supabaseGet("buildings", `id=eq.${buildingId}&select=name`),
  ]);

  // Group expenses by month
  const expByMonth = {};
  let totalExpenses = 0;
  (expenses || []).forEach((e) => {
    const m = (e.date || "").substring(0, 7); // "2026-04-01" → "2026-04"
    if (!m || !m.startsWith(String(year))) return;
    if (!expByMonth[m]) expByMonth[m] = 0;
    expByMonth[m] += Number(e.amount) || 0;
    totalExpenses += Number(e.amount) || 0;
  });

  // Group income by month
  const incByMonth = {};
  let totalIncome = 0;
  (payments || [])
    .filter((p) => (p.month || "").startsWith(String(year)))
    .forEach((p) => {
      const m = p.month;
      if (!incByMonth[m]) incByMonth[m] = 0;
      incByMonth[m] += Number(p.amount) || 0;
      totalIncome += Number(p.amount) || 0;
    });

  const months = [
    ...new Set([...Object.keys(expByMonth), ...Object.keys(incByMonth)]),
  ].sort();
  if (months.length === 0) {
    console.log(`  Finance: no data for ${year}`);
    return;
  }

  // Find expense outliers (>30% above average)
  const expValues = Object.values(expByMonth);
  const avgExp =
    expValues.length > 0
      ? expValues.reduce((a, b) => a + b, 0) / expValues.length
      : 0;
  const outliers = [];
  for (const [m, val] of Object.entries(expByMonth)) {
    if (avgExp > 0 && val > avgExp * 1.3) {
      outliers.push({
        month: m,
        amount: val,
        avg: Math.round(avgExp),
        pct: Math.round((val / avgExp - 1) * 100),
      });
    }
  }

  const balance = totalIncome - totalExpenses;
  const severity =
    balance < 0 ? "high" : outliers.length > 0 ? "medium" : "low";

  const messageParts = [
    `מאזן ${year}: הכנסות ${totalIncome}₪, הוצאות ${totalExpenses}₪, ${balance >= 0 ? "עודף" : "גירעון"} ${Math.abs(balance)}₪`,
  ];
  if (outliers.length > 0) {
    messageParts.push(
      `חריגות הוצאות: ${outliers.map((o) => `${o.month}: ${o.amount}₪ (+${o.pct}% מהממוצע)`).join(", ")}`,
    );
  }

  try {
    await supabaseInsert("agent_alerts", [
      {
        building_id: buildingId,
        agent_type: "expense_analysis",
        severity,
        title: `ניתוח פיננסי — ${new Date().toISOString().split("T")[0]}`,
        message: messageParts.join(". "),
      },
    ]);
  } catch (_) {}

  console.log(
    `  Finance: balance ${balance >= 0 ? "+" : ""}${balance}₪, ${outliers.length} outliers`,
  );
}

// ─── Agent Prompts (weekly review only) ─────────────────────────────────────

const FINANCE_PROMPT = (buildingId, year) =>
  `
נתח מצב פיננסי של בניין ${buildingId}, שנת ${year}.

חוקי יעילות — חובה:
- קרא כל כלי פעם אחת בלבד. אל תקרא שוב נתונים שכבר קיבלת.
- כתוב write_alerts פעם אחת עם כל הממצאים במערך אחד.

צעדים:
1. get_building_info, get_expenses (${year}), get_income (${year}) — שלושתם במקביל
2. נתח: חריגות הוצאות מעל 30%, מאזן הכנסות/הוצאות, הזדמנויות חיסכון
3. write_alerts — התראה אחת מסכמת עם כל הממצאים (high/medium/low)

עבוד רק עם נתונים שקיבלת. אל תנחש.
`.trim();

const COLLECTION_PROMPT = (buildingId, year, currentMonth) =>
  `
בדוק מצב גבייה לבניין ${buildingId}, מ-${year}-01 עד ${currentMonth}.

חוקי יעילות — חובה:
- קרא כל כלי פעם אחת בלבד. אל תחזור על קריאות.
- אל תשלח מיילים. רק תיקים והתראות.

הגדרות:
- "חוב" = 0 ₪ בחודש מסוים. רק זה.
- סכום שונה מהתעריף = "הפרש לבירור", לא חוב. אל תפתח תיק.
- תנועות debit (הוצאות) = התעלם לחלוטין.

צעדים:
1. קרא במקביל: get_building_info, get_units_and_residents, get_payments, get_collection_cases
2. חשב תעריף לכל דירה (fee_tiers לפי חדרים, board_member_discount)
3. לכל דירה × כל חודש: 0₪=חוב, סכום מלא±10%=שולם, אחרת=הפרש
4. upsert_collection_case רק לדירות עם חודש+ ללא תשלום כלל (escalation_level: "reminder")
5. סגור תיקים של דירות ששילמו הכל
6. write_alerts — התראה אחת מסכמת (כמה חייבים, כמה הפרשים, סכום כולל)

סכם בקצרה.
`.trim();

// ─── Step 5: Run managed agents ──────────────────────────────────────────────

async function runManagedAgents(buildingIds) {
  if (!ANTHROPIC_API_KEY) {
    console.log("\nSkipping managed agents (no ANTHROPIC_API_KEY)");
    return;
  }

  // Import and run the agents script inline
  const API_BASE = "https://api.anthropic.com/v1";
  const AGENT_HEADERS = {
    "x-api-key": ANTHROPIC_API_KEY,
    "anthropic-beta": "managed-agents-2026-04-01",
    "anthropic-version": "2023-06-01",
    "content-type": "application/json",
  };
  const ENV_ID = "env_01LMDFfVfgEKUFSYBUBVof9a";

  const AGENTS = [
    { id: "agent_011Ca2LtM3B3VQDyKnDvm7Sv", name: "אנליסט פיננסי" },
    { id: "agent_011Ca2Lu1GK1Pmoj6rXwnBY7", name: "מנהל גבייה" },
  ];

  for (const buildingId of buildingIds) {
    console.log(`\n=== Agents for building ${buildingId} ===`);

    for (const agent of AGENTS) {
      console.log(`  Running ${agent.name}...`);
      const startTime = Date.now();

      try {
        const session = await fetch(`${API_BASE}/sessions`, {
          method: "POST",
          headers: AGENT_HEADERS,
          body: JSON.stringify({ agent: agent.id, environment_id: ENV_ID }),
        }).then((r) => {
          if (!r.ok) throw new Error(`${r.status}`);
          return r.json();
        });

        const year = new Date().getFullYear();
        const currentMonth = `${year}-${String(new Date().getMonth() + 1).padStart(2, "0")}`;
        const prompt =
          agent.name === "אנליסט פיננסי"
            ? FINANCE_PROMPT(buildingId, year)
            : COLLECTION_PROMPT(buildingId, year, currentMonth);

        await fetch(`${API_BASE}/sessions/${session.id}/events`, {
          method: "POST",
          headers: AGENT_HEADERS,
          body: JSON.stringify({
            events: [
              {
                type: "user.message",
                content: [{ type: "text", text: prompt }],
              },
            ],
          }),
        });

        // Poll for completion — wait for agent activity before treating idle as done
        const maxWait = 600000;
        const start = Date.now();
        let sawActivity = false;
        let lastSeenIdx = 0;
        while (Date.now() - start < maxWait) {
          await new Promise((r) => setTimeout(r, 5000));
          const evts = await fetch(
            `${API_BASE}/sessions/${session.id}/events`,
            { headers: AGENT_HEADERS },
          ).then((r) => r.json());
          const events = evts.data || [];

          // Only process new events (avoid duplicate logging)
          const newEvents = events.slice(lastSeenIdx);
          lastSeenIdx = events.length;

          // Track agent activity
          if (!sawActivity) {
            sawActivity = newEvents.some(
              (e) =>
                e.type === "agent.message" ||
                e.type === "agent.tool_use" ||
                e.type === "agent.mcp_tool_use",
            );
            if (sawActivity)
              console.log(`  ⚡ ${agent.name} started working...`);
          }

          // Log only NEW tool calls
          for (const evt of newEvents) {
            if (
              evt.type === "agent.tool_use" ||
              evt.type === "agent.mcp_tool_use"
            ) {
              const toolName = evt.name || evt.tool_name || "?";
              console.log(`    🔧 ${toolName}`);
            }
          }

          // Only accept idle AFTER we've seen activity
          if (
            sawActivity &&
            events.find((e) => e.type === "session.status_idle")
          ) {
            const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
            console.log(`  ✓ ${agent.name} completed in ${elapsed}s`);
            break;
          }

          if (
            events.find((e) => e.type === "session.error" || e.type === "error")
          ) {
            const errEvt = events.find(
              (e) => e.type === "session.error" || e.type === "error",
            );
            throw new Error(`Agent error: ${JSON.stringify(errEvt)}`);
          }
        }
      } catch (err) {
        console.error(`  ✗ ${agent.name}: ${err.message}`);
      }
    }
  }
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log("=== Dynamic Scraper Orchestrator ===");
  console.log(`Date: ${new Date().toISOString()}\n`);

  // 1. Fetch accounts
  const accountsByBuilding = await fetchAccounts();
  const buildingIds = Object.keys(accountsByBuilding);
  console.log(`Buildings: ${buildingIds.length}`);

  let totalNew = 0;

  // 2. Process each building
  for (const [buildingId, accounts] of Object.entries(accountsByBuilding)) {
    const labels = accounts.map((a) => a.label || a.bank_type).join(", ");
    console.log(`\n══════════════════════════════════════`);
    console.log(`Building: ${buildingId}`);
    console.log(`Accounts: ${labels}`);

    // 2a. Scrape
    const result = runMoneyman(buildingId, accounts);
    if (!result) {
      console.log("  Skipping (no transactions)");
      continue;
    }

    // 2b. Push to Supabase
    const newCount = await pushTransactions(buildingId, result.transactions);
    totalNew += newCount;

    // 2c. Deduplicate (month+description+amount)
    await deduplicateTransactions(buildingId);

    // 2d. Auto-match
    await autoMatch(buildingId);

    // 2e. Collection + Finance analysis (code, runs every time)
    await runCollectionAnalysis(buildingId);
    await runFinanceAnalysis(buildingId);
  }

  // 3. Run managed agents only on weekly review (Fridays) or if forced
  const dayOfWeek = new Date().getDay(); // 0=Sun, 5=Fri
  if (process.env.FORCE_AGENTS === "true" || dayOfWeek === 5) {
    console.log("\n=== Weekly agent review ===");
    await runManagedAgents(buildingIds);
  }

  console.log("\n=== Done ===");
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
