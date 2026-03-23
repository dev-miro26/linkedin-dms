/* Desearch LinkedIn DMs — Popup UI Logic */

const $ = (sel) => document.querySelector(sel);

const accountDisplay = $("#accountDisplay");
const statusBadge = $("#statusBadge");
const headersDisplay = $("#headersDisplay");
const lastUpdated = $("#lastUpdated");
const serviceUrlInput = $("#serviceUrl");
const saveUrlBtn = $("#saveUrlBtn");
const accountLabel = $("#accountLabel");
const registerBtn = $("#registerBtn");
const registerSection = $("#registerSection");
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
    synced: ["synced", "ok"],
    no_account: ["no account", "warn"],
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

// ─── Load current state ─────────────────────────────────────────────
async function loadStatus() {
  const resp = await sendMessage({ action: "getStatus" });
  if (!resp?.ok) return;
  const d = resp.data;

  // Service URL
  serviceUrlInput.value = d.serviceUrl || "";
  serviceUrlInput.placeholder = "http://localhost:8899";

  // Account
  if (d.accountId) {
    accountDisplay.textContent = `#${d.accountId}` + (d.accountLabel ? ` (${d.accountLabel})` : "");
    registerSection.style.display = "none";
    actionsSection.style.display = "block";
  } else {
    accountDisplay.textContent = "Not registered";
    registerSection.style.display = "block";
    actionsSection.style.display = "none";
  }

  // Status badge
  statusBadge.innerHTML = statusToBadge(d.lastStatus);

  // Captured headers
  const parts = [];
  if (d.xLiTrack) parts.push("x-li-track");
  if (d.csrfToken) parts.push("csrf-token");
  headersDisplay.textContent = parts.length > 0 ? parts.join(", ") : "—";

  // Last updated
  if (d.lastStatusAt) {
    const dt = new Date(d.lastStatusAt);
    lastUpdated.textContent = `Last updated: ${dt.toLocaleTimeString()}`;
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

// ─── Register account ───────────────────────────────────────────────
registerBtn.addEventListener("click", async () => {
  const label = accountLabel.value.trim();
  if (!label) {
    showMessage("Please enter an account label.", "error");
    return;
  }
  registerBtn.disabled = true;
  registerBtn.textContent = "Registering...";

  const resp = await sendMessage({ action: "register", label });

  registerBtn.disabled = false;
  registerBtn.textContent = "Register Account";

  if (resp?.ok) {
    showMessage(`Account registered (ID: ${resp.data.accountId})`, "success");
    await loadStatus();
  } else {
    showMessage(resp?.error || "Registration failed.", "error");
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
