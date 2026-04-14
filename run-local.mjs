import { spawn } from "child_process";

process.env.MONEYMAN_CONFIG = JSON.stringify({
  accounts: [{ companyId: "pagi", username: "I652LRB", password: "Tm256914" }],
});
process.env.MONEYMAN_UNSAFE_STDOUT = "true";
process.env.TZ = "Asia/Jerusalem";
process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL =
  "moneyman@moneyman-488313.iam.gserviceaccount.com";
process.env.GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY = `-----BEGIN PRIVATE KEY-----
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
process.env.GOOGLE_SHEET_ID = "1H7-mNuUOva9zpN4Qm5SlWn9cLnVnHL4q4FOkOINyEPg";
process.env.GOOGLE_WORKSHEET_NAME = "גיליון1";
process.env.DEBUG = "moneyman:*";

const p = spawn("node", ["dst/index.js"], {
  env: process.env,
  stdio: "inherit",
});
p.on("exit", (code) => process.exit(code));
