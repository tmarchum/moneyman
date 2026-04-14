const { createClient } = require("@supabase/supabase-js");
const path = require("path");
require("dotenv").config({
  path: path.join(__dirname, ".env"),
  override: true,
});

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
);
const BID = process.env.BUILDING_ID;

function normalizeDescription(desc) {
  if (!desc) return "";
  return desc
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[,."']/g, "")
    .toLowerCase();
}

const GENERIC_WORDS = [
  "זיכוי",
  "מיידי",
  "מבנק",
  "העברה",
  "תשלום",
  "הפקדה",
  "שיק",
  "צק",
];

function extractNameParts(desc) {
  if (!desc) return [];
  const cleaned = desc
    .replace(/זיכוי\s+מ[^\s]*\s+מ/g, "")
    .replace(/[,."'\/\\]/g, " ")
    .replace(/\d{5,}/g, "")
    .trim();
  return cleaned
    .split(/\s+/)
    .filter((p) => p.length > 2 && !GENERIC_WORDS.includes(p));
}

// Filter out transactions that should never be auto-matched
const SKIP_PATTERNS = [
  "החזרת שיק",
  "משיכת שיק",
  "הורא.קבע",
  "חברת החשמל",
  "עמלה",
  "ריבית",
];

function shouldSkipTransaction(tx) {
  const desc = tx.description || "";
  for (const pat of SKIP_PATTERNS) {
    if (desc.includes(pat)) return true;
  }
  return false;
}

async function run() {
  console.log("=== שיוך אוטומטי לכל החודשים ===\n");

  const { data: allTx } = await supabase
    .from("bank_transactions")
    .select("*")
    .eq("building_id", BID);
  const { data: units } = await supabase
    .from("units")
    .select("id, number, monthly_fee, rooms, board_member")
    .eq("building_id", BID);
  const { data: residents } = await supabase
    .from("unit_residents")
    .select("unit_id, first_name, last_name, is_primary");
  const { data: building } = await supabase
    .from("buildings")
    .select("monthly_fee, fee_tiers, board_member_discount")
    .eq("id", BID)
    .single();
  const baseFee = building?.monthly_fee || 440;
  const feeTiers = building?.fee_tiers || [];

  const resMap = {};
  (residents || []).forEach((r) => {
    if (r.is_primary || !resMap[r.unit_id]) {
      resMap[r.unit_id] = `${r.first_name || ""} ${r.last_name || ""}`.trim();
    }
  });

  const unitMap = {};
  (units || []).forEach((u) => {
    unitMap[u.id] = u;
  });

  // Learn exact description patterns from matched tx
  const matchedTx = (allTx || []).filter(
    (tx) => tx.match_status === "matched" && tx.unit_id,
  );
  const patterns = {};
  matchedTx.forEach((tx) => {
    const key = normalizeDescription(tx.description);
    if (key && key.length > 5) patterns[key] = tx.unit_id;
  });

  // Name-part patterns from matched tx
  const nameToUnit = {};
  matchedTx.forEach((tx) => {
    const parts = extractNameParts(tx.description);
    if (parts.length < 2) return; // Need at least 2 meaningful name parts
    const key = parts.sort().join(" ");
    if (key.length > 3) nameToUnit[key] = tx.unit_id;
  });

  // Build map of confirmed descriptions → { unitId, day-of-month, amount }
  const confirmedPatterns = {};
  matchedTx.forEach((tx) => {
    const key = normalizeDescription(tx.description);
    if (!key || key.length <= 5) return;
    const day = tx.transaction_date
      ? new Date(tx.transaction_date).getDate()
      : null;
    confirmedPatterns[key] = {
      unitId: tx.unit_id,
      day,
      amount: Number(tx.credit) || 0,
    };
  });

  console.log(
    `${Object.keys(patterns).length} דפוסי תיאור, ${Object.keys(nameToUnit).length} דפוסי שם\n`,
  );

  // --- Fee calculation helper ---
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

  // --- Confidence scoring ---
  // Returns { unitId, confidence, reason } or null
  function scoreMatch(tx) {
    const key = normalizeDescription(tx.description);
    const credit = Number(tx.credit) || 0;
    const txDay = tx.transaction_date
      ? new Date(tx.transaction_date).getDate()
      : null;
    const txParts = extractNameParts(tx.description);

    // --- Tier 1: Exact description from confirmed tx ---
    if (key && confirmedPatterns[key]) {
      const cp = confirmedPatterns[key];
      const unit = unitMap[cp.unitId];
      if (unit) {
        const fee = calcUnitFee(unit);
        if (credit > fee * 2) return null;
        const dayMatch = cp.day && txDay && Math.abs(cp.day - txDay) <= 3;
        const amountMatch = Math.abs(credit - cp.amount) < 50;
        return {
          unitId: cp.unitId,
          confidence: "high",
          reason: `תיאור זהה${dayMatch ? " + תאריך תואם" : ""}${amountMatch ? " + סכום תואם" : ""}`,
        };
      }
    }

    // --- Tier 2: Name-part overlap from confirmed patterns ---
    for (const [nameKey, uid] of Object.entries(nameToUnit)) {
      const knownParts = nameKey.split(" ");
      const overlap = knownParts.filter(
        (p) =>
          p.length >= 3 &&
          txParts.some(
            (tp) => tp.length >= 3 && (tp.includes(p) || p.includes(tp)),
          ),
      );
      if (overlap.length >= 2) {
        const unit = unitMap[uid];
        if (!unit) continue;
        const fee = calcUnitFee(unit);
        if (credit > fee * 2) continue;
        const amountRatio = credit / fee;
        if (amountRatio >= 0.7 && amountRatio <= 1.3) {
          return {
            unitId: uid,
            confidence: "high",
            reason: `שם מאושר (${overlap.join(", ")}) + סכום תואם`,
          };
        }
        return {
          unitId: uid,
          confidence: "medium",
          reason: `שם מאושר (${overlap.join(", ")}) אבל סכום ${credit} ≠ ${fee}`,
        };
      }
    }

    // --- Tier 3: Resident name from DB (never confirmed) ---
    const desc = (tx.description || "").toLowerCase();
    for (const unit of units || []) {
      const name = (resMap[unit.id] || "").trim();
      if (!name) continue;
      const parts = name.split(" ").filter((p) => p.length >= 3);
      const matchingParts = parts.filter((part) =>
        desc.includes(part.toLowerCase()),
      );
      if (parts.length >= 2 && matchingParts.length >= 2) {
        const fee = calcUnitFee(unit);
        if (credit > fee * 2) continue;
        const amountRatio = credit / fee;
        if (amountRatio >= 0.8 && amountRatio <= 1.2) {
          return {
            unitId: unit.id,
            confidence: "medium",
            reason: `שם מ-DB (${matchingParts.join(", ")}) + סכום קרוב לתעריף`,
          };
        }
        return {
          unitId: unit.id,
          confidence: "low",
          reason: `שם מ-DB (${matchingParts.join(", ")}) אבל סכום ${credit} ≠ ${fee}`,
        };
      }
    }

    return null;
  }

  const unmatched = (allTx || []).filter(
    (tx) => tx.match_status === "unmatched" && Number(tx.credit) > 0,
  );
  let autoMatched = 0;
  let suggested = 0;
  const byMonth = {};

  for (const tx of unmatched) {
    if (shouldSkipTransaction(tx)) continue;

    const result = scoreMatch(tx);
    if (!result) continue;

    const { unitId, confidence, reason } = result;
    const unit = unitMap[unitId];
    if (!unit) continue;

    // Skip if unit already has a matched tx this month from a DIFFERENT person
    const existingMatch = matchedTx.find(
      (t) => t.unit_id === unitId && t.month === tx.month,
    );
    if (existingMatch) {
      const existingDesc = normalizeDescription(existingMatch.description);
      const newDesc = normalizeDescription(tx.description);
      if (existingDesc !== newDesc) continue;
    }

    if (confidence === "high") {
      // Auto-match
      await supabase
        .from("bank_transactions")
        .update({ unit_id: unitId, match_status: "matched" })
        .eq("id", tx.id);

      // Track for chain-learning
      const key = normalizeDescription(tx.description);
      if (key) patterns[key] = unitId;
      const newParts = extractNameParts(tx.description);
      const newKey = newParts.sort().join(" ");
      if (newKey.length > 3) nameToUnit[newKey] = unitId;

      matchedTx.push({ ...tx, unit_id: unitId, match_status: "matched" });

      const m = tx.month || "?";
      if (!byMonth[m]) byMonth[m] = 0;
      byMonth[m]++;

      autoMatched++;
      console.log(
        `  ✓ דירה ${unit.number} | ${m} | ${tx.credit} ₪ | ${reason}`,
      );
    } else {
      // Medium/Low → suggest
      await supabase
        .from("bank_transactions")
        .update({ unit_id: unitId, match_status: "suggested" })
        .eq("id", tx.id);
      console.log(
        `  ? הצעה (${confidence}): דירה ${unit.number} | ${tx.credit} ₪ | ${reason}`,
      );
      suggested++;
    }
  }

  console.log(`\nסה"כ שויכו אוטומטית: ${autoMatched}, הצעות: ${suggested}`);
  Object.entries(byMonth)
    .sort()
    .forEach(([m, c]) => console.log(`  ${m}: ${c}`));

  // --- Sync all payments ---
  console.log("\n=== סנכרון תשלומים ===\n");

  const { data: allMatched } = await supabase
    .from("bank_transactions")
    .select("unit_id, month, credit")
    .eq("building_id", BID)
    .eq("match_status", "matched")
    .gt("credit", 0);

  const groups = {};
  (allMatched || []).forEach((tx) => {
    if (!tx.unit_id || !tx.month) return;
    const k = `${tx.unit_id}|${tx.month}`;
    if (!groups[k])
      groups[k] = { unitId: tx.unit_id, month: tx.month, total: 0 };
    groups[k].total += Number(tx.credit) || 0;
  });

  const { data: existingPayments } = await supabase
    .from("payments")
    .select("*")
    .eq("building_id", BID);
  let created = 0,
    updated = 0,
    skipped = 0;

  for (const [k, g] of Object.entries(groups)) {
    const unit = unitMap[g.unitId];
    if (!unit) continue;
    const fee = calcUnitFee(unit);
    const status = g.total >= fee ? "paid" : "partial";
    const existing = (existingPayments || []).find(
      (p) => p.unit_id === g.unitId && p.month === g.month,
    );

    if (existing) {
      if (existing.status === "paid" && Number(existing.amount) >= g.total) {
        skipped++;
        continue;
      }
      const { error } = await supabase
        .from("payments")
        .update({
          amount: g.total,
          status,
          paid_at: new Date().toISOString().split("T")[0],
          method: "הוראת קבע",
        })
        .eq("id", existing.id);
      if (!error) updated++;
      else console.error(`  ✗ ${unit.number} ${g.month}: ${error.message}`);
    } else {
      const { error } = await supabase.from("payments").insert({
        building_id: BID,
        unit_id: g.unitId,
        amount: g.total,
        month: g.month,
        status,
        paid_at: new Date().toISOString().split("T")[0],
        method: "הוראת קבע",
      });
      if (!error) {
        created++;
        console.log(
          `  + דירה ${unit.number} | ${g.month} | ${g.total}/${fee} ₪ | ${status}`,
        );
      } else console.error(`  ✗ ${unit.number} ${g.month}: ${error.message}`);
    }
  }

  console.log(
    `\nתשלומים: ${created} נוצרו, ${updated} עודכנו, ${skipped} דולגו`,
  );
}

run().catch((err) => console.error("Fatal:", err));
