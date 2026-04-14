/**
 * Orchestrator for Anthropic Managed Agents
 * Triggers Finance Analyst + Collection Manager agents after bank scraper runs.
 * Runs in GitHub Actions — no local machine dependency.
 */

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const BUILDING_ID = process.env.BUILDING_ID;

if (!ANTHROPIC_API_KEY || !BUILDING_ID) {
  console.error("ERROR: Missing ANTHROPIC_API_KEY or BUILDING_ID");
  process.exit(1);
}

const API_BASE = "https://api.anthropic.com/v1";
const HEADERS = {
  "x-api-key": ANTHROPIC_API_KEY,
  "anthropic-beta": "managed-agents-2026-04-01",
  "anthropic-version": "2023-06-01",
  "content-type": "application/json",
};

// ─── Agent Configuration ──────────────────────────────────────────────────────

const AGENTS = [
  {
    id: "agent_011Ca2LtM3B3VQDyKnDvm7Sv",
    name: "אנליסט פיננסי",
    prompt: (buildingId) =>
      `
נתח את המצב הפיננסי של בניין ${buildingId}.

1. קרא את פרטי הבניין עם get_building_info
2. קרא את ההוצאות עם get_expenses (שנת ${new Date().getFullYear()})
3. קרא את ההכנסות עם get_income (שנת ${new Date().getFullYear()})
4. נתח:
   - השווה הוצאות בין חודשים, זהה חריגות מעל 20%
   - בדוק מאזן הכנסות מול הוצאות
   - חפש הזדמנויות חיסכון
5. אם יש ממצאים חשובים, כתוב התראות עם write_alerts:
   - agent_type: "expense_analysis" עבור ממצאי הוצאות
   - agent_type: "budget" עבור ממצאי תקציב
   - severity: "high" לגירעון או חריגה מעל 50%
   - severity: "medium" לחריגה 20-50%
   - severity: "low" להזדמנויות חיסכון

סכם את הממצאים בסוף.
`.trim(),
  },
  {
    id: "agent_011Ca2Lu1GK1Pmoj6rXwnBY7",
    name: "מנהל גבייה",
    prompt: (buildingId) => {
      const now = new Date();
      const year = now.getFullYear();
      const currentMonth = `${year}-${String(now.getMonth() + 1).padStart(2, "0")}`;
      return `
נהל את הגבייה של בניין ${buildingId}.

1. קרא פרטי בניין עם get_building_info
2. קרא דירות ודיירים עם get_units_and_residents
3. קרא תשלומים עם get_payments
4. קרא תיקי גבייה קיימים עם get_collection_cases

5. לכל דירה, בדוק אם שילמה את כל החודשים מ-${year}-01 עד ${currentMonth}:
   - חשב את התעריף לפי fee_tiers (by_rooms), board_member_discount
   - אם יש חוב - בדוק אם כבר יש תיק גבייה פתוח

6. עבור דירות עם חוב:
   - אם אין תיק: צור תיק חדש עם upsert_collection_case (escalation_level: "reminder")
   - אם יש תיק: בדוק אם צריך להסלים:
     * reminder → warning (אחרי 7 ימים)
     * warning → formal (אחרי 14 ימים)
     * formal → legal (אחרי 30 ימים)
   - אם צריך לשלוח מייל והדייר יש לו כתובת מייל, שלח עם send_email:
     * תזכורת ידידותית / אזהרה / מכתב רשמי / התראה משפטית
     * הכל בעברית, בשם ועד הבית
     * כולל פירוט חודשים וסכום החוב

7. עבור דירות ששילמו הכל:
   - אם יש תיק פתוח, סגור אותו (status: "closed", auto_closed: true)

8. צור התראות לועד עם write_alerts (agent_type: "collection"):
   - סיכום מצב גבייה
   - הסלמות חדשות
   - תיקים שנסגרו

סכם את הפעולות שביצעת.
`.trim();
    },
  },
];

const ENV_ID = "env_01LMDFfVfgEKUFSYBUBVof9a";

// ─── API Helpers ──────────────────────────────────────────────────────────────

async function createSession(agentId) {
  const res = await fetch(`${API_BASE}/sessions`, {
    method: "POST",
    headers: HEADERS,
    body: JSON.stringify({
      agent: agentId,
      environment_id: ENV_ID,
    }),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Failed to create session: ${res.status} ${err}`);
  }
  return await res.json();
}

async function sendMessage(sessionId, text) {
  const res = await fetch(`${API_BASE}/sessions/${sessionId}/events`, {
    method: "POST",
    headers: HEADERS,
    body: JSON.stringify({
      events: [
        {
          type: "user.message",
          content: [{ type: "text", text }],
        },
      ],
    }),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Failed to send message: ${res.status} ${err}`);
  }
  return await res.json();
}

async function pollForCompletion(sessionId, maxWaitMs = 600000) {
  const startTime = Date.now();
  let lastEventIndex = 0;

  while (Date.now() - startTime < maxWaitMs) {
    const res = await fetch(`${API_BASE}/sessions/${sessionId}/events`, {
      headers: HEADERS,
    });
    if (!res.ok) {
      const err = await res.text();
      throw new Error(`Failed to poll events: ${res.status} ${err}`);
    }

    const data = await res.json();
    const events =
      data.data || data.events || (Array.isArray(data) ? data : []);

    // Check for completion
    const idleEvent = events.find((e) => e.type === "session.status_idle");
    if (idleEvent) {
      // Find last agent message
      const agentMessages = events.filter((e) => e.type === "agent.message");
      if (agentMessages.length > 0) {
        const lastMsg = agentMessages[agentMessages.length - 1];
        const textBlocks = (lastMsg.content || []).filter(
          (b) => b.type === "text",
        );
        return textBlocks.map((b) => b.text).join("\n");
      }
      return "(no response)";
    }

    // Check for error
    const errorEvent = events.find(
      (e) => e.type === "session.error" || e.type === "error",
    );
    if (errorEvent) {
      throw new Error(`Agent error: ${JSON.stringify(errorEvent)}`);
    }

    // Log tool use progress
    const newEvents = events.slice(lastEventIndex);
    for (const evt of newEvents) {
      if (evt.type === "agent.tool_use" || evt.type === "agent.mcp_tool_use") {
        const toolName = evt.name || evt.tool_name || "?";
        console.log(`  🔧 ${toolName}`);
      }
    }
    lastEventIndex = events.length;

    // Wait before polling again
    await new Promise((r) => setTimeout(r, 3000));
  }

  throw new Error("Agent timed out after " + maxWaitMs / 1000 + "s");
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function runAgent(agentConfig) {
  console.log(`\n=== ${agentConfig.name} ===`);
  const startTime = Date.now();

  try {
    // Create session
    console.log("  יוצר session...");
    const session = await createSession(agentConfig.id);
    console.log(`  session: ${session.id}`);

    // Send prompt
    console.log("  שולח משימה...");
    const prompt = agentConfig.prompt(BUILDING_ID);
    await sendMessage(session.id, prompt);

    // Wait for completion
    console.log("  מחכה לתשובה...");
    const result = await pollForCompletion(session.id);

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`  ✓ הושלם ב-${elapsed} שניות`);
    console.log(`  תוצאה: ${result.substring(0, 200)}...`);

    return { success: true, result };
  } catch (err) {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.error(`  ✗ שגיאה אחרי ${elapsed} שניות: ${err.message}`);
    return { success: false, error: err.message };
  }
}

async function main() {
  console.log("=== הפעלת סוכנים מנוהלים ===");
  console.log(`בניין: ${BUILDING_ID}`);
  console.log(`תאריך: ${new Date().toISOString()}\n`);

  const results = [];

  // Run agents sequentially (to avoid rate limits and keep logs clean)
  for (const agent of AGENTS) {
    const result = await runAgent(agent);
    results.push({ name: agent.name, ...result });
  }

  // Summary
  console.log("\n=== סיכום ===");
  for (const r of results) {
    console.log(
      `  ${r.success ? "✓" : "✗"} ${r.name}: ${r.success ? "הצליח" : r.error}`,
    );
  }

  const anyFailed = results.some((r) => !r.success);
  if (anyFailed) {
    console.error("\nחלק מהסוכנים נכשלו");
    process.exit(1);
  }

  console.log("\nכל הסוכנים הושלמו בהצלחה");
}

main();
