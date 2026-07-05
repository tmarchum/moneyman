// Local moneyman runner — loads ALL secrets from .env.local (gitignored).
// No credentials may appear in this file: it is committed to a PUBLIC repo.
import { spawn } from "child_process";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const envFile = path.join(__dirname, ".env.local");

if (!fs.existsSync(envFile)) {
  console.error("ERROR: .env.local not found. Create it with MONEYMAN_CONFIG, GOOGLE_* etc.");
  process.exit(1);
}

// Minimal .env parser (supports multi-line quoted values like private keys)
const raw = fs.readFileSync(envFile, "utf8");
const lines = raw.split("\n");
for (let i = 0; i < lines.length; i++) {
  const line = lines[i];
  const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
  if (!m) continue;
  let [, key, val] = m;
  // Multi-line value wrapped in double quotes or backticks
  if ((val.startsWith('"') && !val.endsWith('"')) || (val.startsWith("`") && !val.endsWith("`"))) {
    const quote = val[0];
    val = val.slice(1);
    while (++i < lines.length && !lines[i].endsWith(quote)) val += "\n" + lines[i];
    if (i < lines.length) val += "\n" + lines[i].slice(0, -1);
  } else if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("`") && val.endsWith("`"))) {
    val = val.slice(1, -1);
  }
  // PEM keys are often stored single-line with \n escapes — restore real newlines
  if (key.includes("PRIVATE_KEY") && val.includes("\\n")) val = val.replace(/\\n/g, "\n");
  if (!(key in process.env)) process.env[key] = val;
}

process.env.TZ = process.env.TZ || "Asia/Jerusalem";

const p = spawn("node", ["dst/index.js"], {
  env: process.env,
  stdio: "inherit",
});
p.on("exit", (code) => process.exit(code));
