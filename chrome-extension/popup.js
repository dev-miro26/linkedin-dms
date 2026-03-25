/* Desearch LinkedIn DMs — Popup UI Logic */

const $ = (sel) => document.querySelector(sel);

const linkedinAccountDisplay = $("#linkedinAccountDisplay");
const statusBadge = $("#statusBadge");
const headersDisplay = $("#headersDisplay");
const lastUpdated = $("#lastUpdated");
const serviceUrlInput = $("#serviceUrl");
const saveUrlBtn = $("#saveUrlBtn");
const registerHint = $("#registerHint");
const actionsSection = $("#actionsSection");
const syncBtn = $("#syncBtn");
const authCheckBtn = $("#authCheckBtn");
const messageEl = $("#message");

// ─── Helpers ────────────────────────────────────────────────────────
function showMessage(text, type) {
  messageEl.textContent = text;
  messageEl.className = type; // "success" or "error"
  setTimeout(() => {
    messageEl.className = "";
    messageEl.textContent = "";
  }, 5000);
}

function statusToBadge(status) {
  if (!status) return '<span class="badge badge-idle">idle</span>';
  const map = {
    registered: ["registered", "ok"],
    cookies_refreshed: ["cookies refreshed", "ok"],
    headers_synced: ["headers synced", "ok"],
    synced: ["synced", "ok"],
    no_account: ["no account", "warn"],
    auto_register_failed: ["auto-register failed", "error"],
    refresh_network_error: ["network error", "error"],
  };
  if (map[status]) {
    const [label, cls] = map[status];
    return `<span class="badge badge-${cls}">${label}</span>`;
  }
  if (status.startsWith("refresh_error_")) {
    return `<span class="badge badge-error">error ${status.replace("refresh_error_", "")}</span>`;
  }
  return `<span class="badge badge-idle">${status}</span>`;
}

function sendMessage(msg) {
  return new Promise((resolve) => {
    chrome.runtime.sendMessage(msg, (resp) => resolve(resp));
  });
}

function formatLinkedInAccountLine(d) {
  if (!d.accountId) return "—";
  const pub = d.linkedinPublicId;
  const mid = d.linkedinMemberId;
  if (pub && mid) return `@${pub} · ${mid}`;
  if (pub) return `@${pub}`;
  if (mid) return String(mid);
  return "—";
}

function applyStatusPayload(d) {
  serviceUrlInput.value = d.serviceUrl || "";
  serviceUrlInput.placeholder = "http://localhost:8899";

  if (d.accountId) {
    linkedinAccountDisplay.textContent = formatLinkedInAccountLine(d);
    actionsSection.style.display = "block";
    registerHint.style.display = "none";
  } else {
    linkedinAccountDisplay.textContent = "—";
    actionsSection.style.display = "none";
    registerHint.textContent =
      "Sign in to LinkedIn in this browser. The extension registers with your service automatically when you are logged in.";
    registerHint.style.display = "block";
  }

  statusBadge.innerHTML = statusToBadge(d.lastStatus);

  const parts = [];
  if (d.xLiTrack) parts.push("x-li-track");
  if (d.csrfToken) parts.push("csrf-token");
  headersDisplay.textContent = parts.length > 0 ? parts.join(", ") : "—";

  if (d.lastStatusAt) {
    const dt = new Date(d.lastStatusAt);
    lastUpdated.textContent = `Last updated: ${dt.toLocaleTimeString()}`;
  }
}

async function loadStatus() {
  await sendMessage({ action: "ensureRegistered" });
  const resp = await sendMessage({ action: "getStatus" });
  if (!resp?.ok) return;
  const d = resp.data;
  applyStatusPayload(d);

  if (!d.accountId) return;

  const idResp = await sendMessage({ action: "fetchIdentity" });
  if (idResp?.ok && idResp.data) {
    applyStatusPayload({ ...d, ...idResp.data });
  }
}

// ─── Save service URL ───────────────────────────────────────────────
saveUrlBtn.addEventListener("click", async () => {
  const url = serviceUrlInput.value.trim().replace(/\/+$/, "");
  if (!url) {
    showMessage("Please enter a service URL.", "error");
    return;
  }
  try {
    new URL(url);
  } catch {
    showMessage("Invalid URL format.", "error");
    return;
  }
  const resp = await sendMessage({ action: "setServiceUrl", url });
  if (resp?.ok) {
    showMessage("Service URL saved.", "success");
  } else {
    showMessage(resp?.error || "Failed to save URL.", "error");
  }
});

// ─── Sync ───────────────────────────────────────────────────────────
syncBtn.addEventListener("click", async () => {
  syncBtn.disabled = true;
  syncBtn.textContent = "Syncing...";

  const resp = await sendMessage({ action: "sync" });

  syncBtn.disabled = false;
  syncBtn.textContent = "Sync Now";

  if (resp?.ok) {
    const d = resp.data;
    const msg = `Synced: ${d.synced_threads || 0} threads, ${d.messages_inserted || 0} new messages`;
    showMessage(msg, "success");
    await loadStatus();
  } else {
    showMessage(resp?.error || "Sync failed.", "error");
  }
});

// ─── Auth check ─────────────────────────────────────────────────────
authCheckBtn.addEventListener("click", async () => {
  authCheckBtn.disabled = true;
  authCheckBtn.textContent = "Checking...";

  const resp = await sendMessage({ action: "checkAuth" });

  authCheckBtn.disabled = false;
  authCheckBtn.textContent = "Check Auth";

  if (resp?.ok) {
    const d = resp.data;
    if (d.status === "ok") {
      showMessage("Auth is valid.", "success");
    } else {
      showMessage(`Auth failed: ${d.error || "unknown"}`, "error");
    }
  } else {
    showMessage(resp?.error || "Auth check failed.", "error");
  }
});

// ─── Init ───────────────────────────────────────────────────────────
loadStatus();
