import http from "http";
import { spawn } from "child_process";
import { readFileSync, writeFileSync, existsSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CONFIG_FILE = join(__dirname, "web-ui-config.json");
const HTML_FILE = join(__dirname, "web-ui.html");
const PORT = process.env.PORT || 3000;

const BANKS = [
  { id: "hapoalim", name: "בנק הפועלים" },
  { id: "leumi", name: "בנק לאומי" },
  { id: "discount", name: "בנק דיסקונט" },
  { id: "mizrahi", name: "בנק מזרחי-טפחות" },
  { id: "pagi", name: "פאגי / בינלאומי" },
  { id: "beinleumi", name: "הבינלאומי הראשון" },
  { id: "otsarHahayal", name: "אוצר החייל" },
  { id: "massad", name: "בנק מסד" },
  { id: "yahav", name: "בנק יהב" },
  { id: "visaCal", name: "ויזה כאל" },
  { id: "max", name: "מקס (לאומי קארד)" },
  { id: "isracard", name: "ישראכארט" },
  { id: "amex", name: "אמריקן אקספרס" },
  { id: "behatsdaa", name: "בהצדעה" },
];

const DEFAULT_PRIVATE_KEY = `-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC5DtCYzIzhI0H5
stHoipIZjKvEmxHcJGaEiXpmfLUsbAQ+jh2L10kQq12nxIFo1Dt3QI4kAKk024Sc
Lrz/g6z6cBFchd/z5AHkOMGGY8kkhFSkMRpczdgmrtfTKfFS36+p85oFC09eoGX0
PMoB1cHTAk/pKOfqRtvuvmr+tSFPgTkxXMPpg69kyCYJN3HjP6QwQhYK4dgXiQ+t
LEdqYYaJFi6WTieq93ytCG9zNW0CBQSY5PMuqNq/iy7+aierRFX6Zo05+AEIIofu
k4cXawSvZcTb52wrzdk0XMqOcy3lJhi+fH9pjOx5lk25vxOfaQ9/zg1Se9cPPAMq
UeUfv0GNAgMBAAECggEAQCNMVMkAQr9vjFVXvxrXzBcfKUL9i6jqByGG1KKAQGcn
iW7D+sWgwzBBg3XtzCFSguBS41N/UZyLd34TbxN6DkptGf4kQmlR5oFtQWCwRAHB
PC7wjh2hvrZ2gu9Ufn6caXDOftUOqyM4cs/my4AEb1erzomo51+rtjE08BZi9ySg
a1gM0itIUVrK/Pr2YNp3G4InHUPr82xuZ0ieY3CzEvqsfFYkK0OKUmtK/cBGZolV
BWtTFG+vagyj5Xho5UNZ1L88TgxuIXnyOk9jZPzpnMiOsYWtKE/OU5RLn2Kx6TmQ
y1ykNC+jXRj+ROtWLFSuTH9rGPAAV7IVU45e5ldDWQKBgQD1uruWxBVlFo6pdx+S
xM30515F3QaRR3ivqO0LlERmntFVgrWaJel5LBLXFINLtK0CVeKWp5PEv6pT0hdM
p82r0J2Eyjzm497loHAo8MOGF6FrIzDgZxfgTjy7A6Bfr+rWO9UwvpbW9rOGhPvM
AZtZ5XkKCiru5qUkhepDZ8Z2EwKBgQDAyufdE9j4226z0lYMHPgqbWo4GbcycWMD
TtjD6q58oWsyTj2vBcplFbxB0paVRGulrT3o2V8c4KiA/BUFXz+VlQBnnQH991yA
D/OPsTYbDfLKigax1y8InfbGOC8Y0fGMy6u1LPsAyaN3l3maWKM8F7tU7C2MQvrw
4c9zgFDd3wKBgF6P0YifRKx2FchZMylD2w4Xy0uPVuupWWQf2bjPAdOL4nrJpiD/
3eznbQifuDb1/G4dpuja7B6Ws3E2NAknuhoYWcW0HeOsZSZwqzjWDigYB+I21KRG
iAWllfFR3/FyvShcNhpf/aQTo9psaomDRMk/aWjXqNXupDZ94jy2PsVJAoGAPj0C
zz8KC4SjX0/m0XBEuUWrRcMffhxWr4mztsO7YqaluY7CoQ8IgMucg89dJ4D4E3sz
AkmyR9tK6qD2lE5kc4CvqcNpEjjZ1snPgjLeWauOFs6qTJ1AJNMCCIm4wpV8Gkzh
+NI1kdKGgCQZcLduswaiRk8cgSxaYIs1cn8ZHBcCgYA5jVhPm2WRKLJD4Fq9gfxc
CXMeRU8uKRCtse2aeOAcSNjlPokcfD6ht5lga34maiAXC63G0yhd0EsXzq27lCMj
aQZteYxUfZAcG9Adh3ZoUrkeqhOtwycXX6/PnUq4plrl+avjiisgU+RLtfGpx6Sz
vFzlQkUXgt4U2NuiJ763NA==
-----END PRIVATE KEY-----`;

function loadConfig() {
  if (existsSync(CONFIG_FILE)) {
    try {
      return JSON.parse(readFileSync(CONFIG_FILE, "utf8"));
    } catch {}
  }
  return {
    accounts: [
      { companyId: "pagi", username: "I652LRB", password: "Tm256914" },
    ],
    googleSheets: {
      serviceAccountEmail: "moneyman@moneyman-488313.iam.gserviceaccount.com",
      serviceAccountPrivateKey: DEFAULT_PRIVATE_KEY,
      sheetId: "1H7-mNuUOva9zpN4Qm5SlWn9cLnVnHL4q4FOkOINyEPg",
      worksheetName: "גיליון1",
    },
  };
}

function saveConfig(cfg) {
  writeFileSync(CONFIG_FILE, JSON.stringify(cfg, null, 2), "utf8");
}

// SSE clients
const sseClients = new Set();
function broadcast(msg) {
  const data = `data: ${JSON.stringify(msg)}\n\n`;
  for (const res of sseClients) res.write(data);
}

let activeProcess = null;

function startScraper(cfg) {
  if (activeProcess) {
    broadcast({ type: "error", text: "תהליך כבר רץ, המתן לסיומו." });
    return;
  }
  const env = {
    ...process.env,
    MONEYMAN_CONFIG: JSON.stringify({ accounts: cfg.accounts }),
    MONEYMAN_UNSAFE_STDOUT: "true",
    TZ: "Asia/Jerusalem",
    DEBUG: "moneyman:*",
  };
  const gs = cfg.googleSheets || {};
  if (gs.serviceAccountEmail)
    env.GOOGLE_SERVICE_ACCOUNT_EMAIL = gs.serviceAccountEmail;
  if (gs.serviceAccountPrivateKey)
    env.GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY = gs.serviceAccountPrivateKey;
  if (gs.sheetId) env.GOOGLE_SHEET_ID = gs.sheetId;
  if (gs.worksheetName) env.GOOGLE_WORKSHEET_NAME = gs.worksheetName;

  broadcast({ type: "start", text: "▶ מתחיל סריקה..." });

  activeProcess = spawn("node", [join(__dirname, "dst/index.js")], { env });
  activeProcess.stdout.on("data", (d) =>
    broadcast({ type: "log", text: d.toString().trimEnd() }),
  );
  activeProcess.stderr.on("data", (d) =>
    broadcast({ type: "log", text: d.toString().trimEnd() }),
  );
  activeProcess.on("exit", (code) => {
    broadcast({ type: "done", text: `✓ הסתיים (קוד ${code})` });
    activeProcess = null;
  });
}

function parseBody(req) {
  return new Promise((resolve) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      try {
        resolve(JSON.parse(body));
      } catch {
        resolve({});
      }
    });
  });
}

function getHtml() {
  const html = readFileSync(HTML_FILE, "utf8");
  return html.replace("__BANKS_JSON__", JSON.stringify(BANKS));
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const path = url.pathname;

  if (path === "/" || path === "/index.html") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(getHtml());
    return;
  }

  if (path === "/api/config" && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
    res.end(JSON.stringify(loadConfig()));
    return;
  }

  if (path === "/api/config" && req.method === "POST") {
    const body = await parseBody(req);
    saveConfig(body);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  if (path === "/api/run" && req.method === "POST") {
    startScraper(loadConfig());
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  if (path === "/api/logs") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    });
    res.write(":ping\n\n");
    sseClients.add(res);
    req.on("close", () => sseClients.delete(res));
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

server.listen(PORT, () => {
  console.log(`\nMoneyMan UI מוכן: http://localhost:${PORT}\n`);
});
