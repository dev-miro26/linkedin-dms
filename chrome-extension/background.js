/* Desearch LinkedIn DMs — Chrome Extension Background Service Worker */

const DEFAULT_SERVICE_URL = "http://localhost:8899";

// ─── State helpers ──────────────────────────────────────────────────
async function getServiceUrl() {
  const { serviceUrl } = await chrome.storage.local.get("serviceUrl");
  return serviceUrl || DEFAULT_SERVICE_URL;
}

async function getAccountId() {
  const { accountId } = await chrome.storage.local.get("accountId");
  return accountId || null;
}

async function setStatus(status) {
  await chrome.storage.local.set({ lastStatus: status, lastStatusAt: new Date().toISOString() });
}

// ─── Cookie watcher ─────────────────────────────────────────────────
chrome.cookies.onChanged.addListener(async ({ cookie, removed }) => {
  if (removed) return;
  if (!cookie.domain.includes("linkedin.com")) return;
  if (cookie.name !== "li_at") return;

  const accountId = await getAccountId();
  if (!accountId) {
    await setStatus("no_account");
    return;
  }

  try {
    const jsession = await chrome.cookies.get({
      url: "https://www.linkedin.com",
      name: "JSESSIONID",
    });

    const serviceUrl = await getServiceUrl();
    const resp = await fetch(`${serviceUrl}/accounts/refresh`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        account_id: accountId,
        li_at: cookie.value,
        jsessionid: jsession?.value || null,
      }),
    });

    if (resp.ok) {
      await setStatus("cookies_refreshed");
    } else {
      const body = await resp.text();
      await setStatus(`refresh_error_${resp.status}`);
      console.error("[desearch] Cookie refresh failed:", resp.status, body);
    }
  } catch (err) {
    await setStatus("refresh_network_error");
    console.error("[desearch] Cookie refresh network error:", err);
  }
});

// ─── Header interceptor (x-li-track, csrf-token) ───────────────────
chrome.webRequest.onSendHeaders.addListener(
  (details) => {
    if (!details.requestHeaders) return;
    const track = details.requestHeaders.find(
      (h) => h.name.toLowerCase() === "x-li-track"
    );
    const csrf = details.requestHeaders.find(
      (h) => h.name.toLowerCase() === "csrf-token"
    );
    if (track || csrf) {
      const data = {};
      if (track) data.xLiTrack = track.value;
      if (csrf) data.csrfToken = csrf.value;
      chrome.storage.local.set(data);
    }
  },
  { urls: ["https://www.linkedin.com/voyager/api/*"] },
  ["requestHeaders"]
);

// ─── Account registration ───────────────────────────────────────────
async function registerAccount(label) {
  const liAtCookie = await chrome.cookies.get({
    url: "https://www.linkedin.com",
    name: "li_at",
  });
  if (!liAtCookie) {
    throw new Error("No li_at cookie found. Please log in to LinkedIn first.");
  }

  const jsession = await chrome.cookies.get({
    url: "https://www.linkedin.com",
    name: "JSESSIONID",
  });

  const serviceUrl = await getServiceUrl();
  const resp = await fetch(`${serviceUrl}/accounts`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      label: label || "chrome-extension",
      li_at: liAtCookie.value,
      jsessionid: jsession?.value || null,
    }),
  });

  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(`Registration failed (${resp.status}): ${body}`);
  }

  const data = await resp.json();
  await chrome.storage.local.set({ accountId: data.account_id, accountLabel: label });
  await setStatus("registered");
  return data.account_id;
}

// ─── Manual sync trigger ────────────────────────────────────────────
async function triggerSync() {
  const accountId = await getAccountId();
  if (!accountId) {
    throw new Error("No account registered yet.");
  }

  const serviceUrl = await getServiceUrl();
  const resp = await fetch(`${serviceUrl}/sync`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ account_id: accountId }),
  });

  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(`Sync failed (${resp.status}): ${body}`);
  }

  const data = await resp.json();
  await setStatus("synced");
  return data;
}

// ─── Auth check ─────────────────────────────────────────────────────
async function checkAuth() {
  const accountId = await getAccountId();
  if (!accountId) {
    throw new Error("No account registered yet.");
  }

  const serviceUrl = await getServiceUrl();
  const resp = await fetch(`${serviceUrl}/auth/check?account_id=${accountId}`);

  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(`Auth check failed (${resp.status}): ${body}`);
  }

  return await resp.json();
}

// ─── Message handler for popup ──────────────────────────────────────
chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  const handler = async () => {
    switch (msg.action) {
      case "register":
        return { accountId: await registerAccount(msg.label) };
      case "sync":
        return await triggerSync();
      case "checkAuth":
        return await checkAuth();
      case "getStatus": {
        const state = await chrome.storage.local.get([
          "accountId",
          "accountLabel",
          "lastStatus",
          "lastStatusAt",
          "serviceUrl",
          "xLiTrack",
          "csrfToken",
        ]);
        return state;
      }
      case "setServiceUrl":
        await chrome.storage.local.set({ serviceUrl: msg.url });
        return { ok: true };
      default:
        throw new Error(`Unknown action: ${msg.action}`);
    }
  };

  handler()
    .then((result) => sendResponse({ ok: true, data: result }))
    .catch((err) => sendResponse({ ok: false, error: err.message }));

  return true; // keep message channel open for async response
});
