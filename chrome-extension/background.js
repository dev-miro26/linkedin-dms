const DEFAULT_SERVICE_URL = "http://localhost:8899";
const AUTO_REGISTER_LABEL = "chrome-extension";

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

async function getBridgeHeaderPayload() {
  const { xLiTrack, csrfToken } = await chrome.storage.local.get(["xLiTrack", "csrfToken"]);
  const out = {};
  if (xLiTrack != null && xLiTrack !== "") out.x_li_track = xLiTrack;
  if (csrfToken != null && csrfToken !== "") out.csrf_token = csrfToken;
  return out;
}

let registerInFlight = null;
async function tryAutoRegister() {
  if (registerInFlight) return registerInFlight;
  registerInFlight = (async () => {
    try {
      const existing = await getAccountId();
      if (existing) return;
      const liAtCookie = await chrome.cookies.get({
        url: "https://www.linkedin.com",
        name: "li_at",
      });
      if (!liAtCookie) return;
      await registerAccount(AUTO_REGISTER_LABEL);
    } catch (err) {
      await setStatus("auto_register_failed");
      console.error("[desearch] Auto-register failed:", err);
    } finally {
      registerInFlight = null;
    }
  })();
  return registerInFlight;
}

let headerRefreshTimer = null;
async function pushAccountRefreshFromCookies() {
  const accountId = await getAccountId();
  if (!accountId) return;
  const liAtCookie = await chrome.cookies.get({
    url: "https://www.linkedin.com",
    name: "li_at",
  });
  if (!liAtCookie) return;
  const jsession = await chrome.cookies.get({
    url: "https://www.linkedin.com",
    name: "JSESSIONID",
  });
  const bridge = await getBridgeHeaderPayload();
  const serviceUrl = await getServiceUrl();
  try {
    const resp = await fetch(`${serviceUrl}/accounts/refresh`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        account_id: accountId,
        li_at: liAtCookie.value,
        jsessionid: jsession?.value ?? null,
        ...bridge,
      }),
    });
    if (resp.ok) {
      await setStatus("headers_synced");
      fetchLinkedInIdentity();
    } else {
      const body = await resp.text();
      console.error("[desearch] Header bridge refresh failed:", resp.status, body);
    }
  } catch (err) {
    console.error("[desearch] Header bridge refresh network error:", err);
  }
}

function scheduleHeaderBridgeRefresh() {
  if (headerRefreshTimer) clearTimeout(headerRefreshTimer);
  headerRefreshTimer = setTimeout(() => {
    headerRefreshTimer = null;
    pushAccountRefreshFromCookies();
  }, 450);
}

chrome.cookies.onChanged.addListener(async ({ cookie, removed }) => {
  if (removed) return;
  if (!cookie.domain.includes("linkedin.com")) return;
  if (cookie.name !== "li_at") return;

  let accountId = await getAccountId();
  if (!accountId) {
    await tryAutoRegister();
    accountId = await getAccountId();
    if (!accountId) {
      await setStatus("no_account");
      return;
    }
  }

  try {
    const jsession = await chrome.cookies.get({
      url: "https://www.linkedin.com",
      name: "JSESSIONID",
    });
    const bridge = await getBridgeHeaderPayload();
    const serviceUrl = await getServiceUrl();
    const resp = await fetch(`${serviceUrl}/accounts/refresh`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        account_id: accountId,
        li_at: cookie.value,
        jsessionid: jsession?.value ?? null,
        ...bridge,
      }),
    });

    if (resp.ok) {
      await setStatus("cookies_refreshed");
      fetchLinkedInIdentity();
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
      chrome.storage.local.set(data, () => scheduleHeaderBridgeRefresh());
    }
  },
  { urls: ["https://www.linkedin.com/voyager/api/*"] },
  ["requestHeaders"]
);

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

  const bridge = await getBridgeHeaderPayload();
  const serviceUrl = await getServiceUrl();
  const resp = await fetch(`${serviceUrl}/accounts`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      label: label || AUTO_REGISTER_LABEL,
      li_at: liAtCookie.value,
      jsessionid: jsession?.value ?? null,
      ...bridge,
    }),
  });

  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(`Registration failed (${resp.status}): ${body}`);
  }

  const data = await resp.json();
  await chrome.storage.local.set({
    accountId: data.account_id,
    accountLabel: label || AUTO_REGISTER_LABEL,
  });
  await setStatus("registered");
  await fetchLinkedInIdentity();
  return data.account_id;
}

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

async function fetchLinkedInIdentity() {
  const accountId = await getAccountId();
  if (!accountId) {
    return { linkedinPublicId: null, linkedinMemberId: null };
  }

  const serviceUrl = await getServiceUrl();
  try {
    const resp = await fetch(
      `${serviceUrl}/auth/identity?account_id=${encodeURIComponent(accountId)}`
    );
    if (!resp.ok) {
      const body = await resp.text();
      console.error("[desearch] Identity fetch failed:", resp.status, body);
      const state = await chrome.storage.local.get([
        "linkedinPublicId",
        "linkedinMemberId",
      ]);
      return {
        linkedinPublicId: state.linkedinPublicId ?? null,
        linkedinMemberId: state.linkedinMemberId ?? null,
      };
    }
    const data = await resp.json();
    if (data.status !== "ok") {
      console.error("[desearch] Identity:", data.error);
      const state = await chrome.storage.local.get([
        "linkedinPublicId",
        "linkedinMemberId",
      ]);
      return {
        linkedinPublicId: state.linkedinPublicId ?? null,
        linkedinMemberId: state.linkedinMemberId ?? null,
      };
    }
    const out = {
      linkedinPublicId: data.public_identifier || null,
      linkedinMemberId: data.member_id || null,
    };
    await chrome.storage.local.set(out);
    return out;
  } catch (err) {
    console.error("[desearch] Identity fetch error:", err);
    const state = await chrome.storage.local.get([
      "linkedinPublicId",
      "linkedinMemberId",
    ]);
    return {
      linkedinPublicId: state.linkedinPublicId ?? null,
      linkedinMemberId: state.linkedinMemberId ?? null,
    };
  }
}

chrome.runtime.onInstalled.addListener(() => {
  tryAutoRegister();
});

chrome.runtime.onStartup.addListener(() => {
  tryAutoRegister();
});

tryAutoRegister();

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
          "linkedinPublicId",
          "linkedinMemberId",
        ]);
        return state;
      }
      case "fetchIdentity":
        return await fetchLinkedInIdentity();
      case "setServiceUrl":
        await chrome.storage.local.set({ serviceUrl: msg.url });
        return { ok: true };
      case "ensureRegistered":
        await tryAutoRegister();
        return { accountId: await getAccountId() };
      default:
        throw new Error(`Unknown action: ${msg.action}`);
    }
  };

  handler()
    .then((result) => sendResponse({ ok: true, data: result }))
    .catch((err) => sendResponse({ ok: false, error: err.message }));

  return true;
});
