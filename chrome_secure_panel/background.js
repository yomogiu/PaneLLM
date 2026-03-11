const BROKER_URL = "http://127.0.0.1:7777";
const BROKER_CLIENT_HEADER = "chrome-sidepanel-v1";
const RELAY_POLL_TIMEOUT_MS = 25_000;
const RELAY_INITIAL_BACKOFF_MS = 1_000;
const RELAY_MAX_BACKOFF_MS = 15_000;
const RELAY_COMMAND_LOOP_VERSION = "0.2.0";
const PAGE_CONTEXT_SELECTION_CHARS = 1_200;
const PAGE_CONTEXT_TEXT_CHARS = 3_000;
const FORCED_BROWSER_ACTION_ROUTE_TIMEOUT_MS = 300_000;
const HOST_POLICY_STORAGE_KEY = "assistant.pageHostPolicy.v1";
const MAX_POLICY_HOSTS = 256;

// Keep this default allowlist intentionally tight. Add domains only when required.
const DEFAULT_ALLOWED_PAGE_HOSTS = Object.freeze([
  "127.0.0.1",
  "localhost",
  "google.com",
  "www.google.com",
  "arxiv.org",
  "www.arxiv.org"
]);
const HIGH_RISK_PATTERN =
  /\b(delete|transfer|wire|bank|purchase|buy|checkout|submit|password|token|credential|2fa|otp|security code)\b/i;

let relayLoopStarted = false;
let relayBackoffMs = RELAY_INITIAL_BACKOFF_MS;
const relayClientId = `ext_${crypto.randomUUID()}`;
const inflightRouteQueries = new Map();
const hostPolicy = {
  allowedHosts: [],
  blockedHosts: []
};
const hostPolicyReady = initializeHostPolicy();

chrome.runtime.onInstalled.addListener(async () => {
  await chrome.sidePanel.setPanelBehavior({ openPanelOnActionClick: true });
  startBrokerCommandLoop().catch((error) => {
    console.error("[secure-panel] command loop start failed on install:", error);
  });
});

chrome.runtime.onStartup.addListener(() => {
  startBrokerCommandLoop().catch((error) => {
    console.error("[secure-panel] command loop start failed on startup:", error);
  });
});

startBrokerCommandLoop().catch((error) => {
  console.error("[secure-panel] command loop start failed:", error);
});

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  handleMessage(message)
    .then((result) => sendResponse({ ok: true, ...result }))
    .catch((error) => {
      const payload = {
        ok: false,
        error: String(error?.message || error)
      };
      if (error?.code) {
        payload.error_code = String(error.code);
      }
      if (error?.data && typeof error.data === "object") {
        payload.error_data = error.data;
      }
      sendResponse(payload);
    });
  return true;
});

async function handleMessage(message) {
  await hostPolicyReady;
  if (!message || typeof message !== "object") {
    throw new Error("Invalid message payload.");
  }
  if (message.type === "assistant.health") {
    return { health: await checkBrokerHealth() };
  }
  if (message.type === "assistant.query") {
    return await routeAssistantQuery(message);
  }
  if (message.type === "assistant.query.cancel") {
    return await cancelAssistantQuery(message);
  }
  if (message.type === "assistant.codex.run.start") {
    return await startCodexRun(message);
  }
  if (message.type === "assistant.codex.run.events") {
    return await pollCodexRunEvents(message);
  }
  if (message.type === "assistant.codex.run.approval") {
    return await submitCodexRunApproval(message);
  }
  if (message.type === "assistant.codex.run.cancel") {
    return await cancelCodexRun(message);
  }
  if (message.type === "assistant.history.list") {
    return await listConversations();
  }
  if (message.type === "assistant.history.get") {
    return await getConversation(message);
  }
  if (message.type === "assistant.history.delete") {
    return await deleteConversation(message);
  }
  if (message.type === "assistant.history.rewrite") {
    return await rewriteConversation(message);
  }
  if (message.type === "assistant.models.get") {
    return await getModels();
  }
  if (message.type === "assistant.mlx.status") {
    return await getMlxStatus();
  }
  if (message.type === "assistant.mlx.config") {
    return await updateMlxConfig(message);
  }
  if (message.type === "assistant.mlx.session.start") {
    return await startMlxSession();
  }
  if (message.type === "assistant.mlx.session.stop") {
    return await stopMlxSession();
  }
  if (message.type === "assistant.mlx.session.restart") {
    return await restartMlxSession();
  }
  if (message.type === "assistant.mlx.adapters.list") {
    return await listMlxAdapters();
  }
  if (message.type === "assistant.mlx.adapters.load") {
    return await loadMlxAdapter(message);
  }
  if (message.type === "assistant.mlx.adapters.unload") {
    return await unloadMlxAdapter();
  }
  if (message.type === "assistant.tools.page_hosts.get") {
    return { policy: getHostPolicySnapshot() };
  }
  if (message.type === "assistant.tools.page_hosts.allow") {
    return { policy: await allowHost(message.host) };
  }
  if (message.type === "assistant.tools.page_hosts.block") {
    return { policy: await blockHost(message.host) };
  }
  if (message.type === "assistant.tools.page_hosts.remove_allow") {
    return { policy: await removeAllowedHost(message.host) };
  }
  if (message.type === "assistant.tools.page_hosts.unblock") {
    return { policy: await unblockHost(message.host) };
  }
  if (message.type === "assistant.tools.page_hosts.allow_active_tab") {
    const activeTab = await getActiveTab();
    if (!activeTab?.url) {
      throw new Error("Unable to resolve the active tab URL.");
    }
    const host = extractUrlHost(activeTab.url);
    if (!host) {
      throw new Error("Active tab host is invalid.");
    }
    return { policy: await allowHost(host), host };
  }
  if (message.type === "assistant.tools.page_hosts.active_tab") {
    const activeTab = await getActiveTab();
    if (!activeTab?.url) {
      return { active_tab: null };
    }
    const host = extractUrlHost(activeTab.url);
    if (!host) {
      return { active_tab: null };
    }
    const snapshot = getHostPolicySnapshot();
    return {
      active_tab: {
        host,
        url: String(activeTab.url),
        allowed: isHostAllowed(activeTab.url, snapshot.effective_allowed_hosts),
        blocked: hostMatchesAllowedList(host, snapshot.blocked_hosts)
      }
    };
  }
  if (message.type === "assistant.browser.tool.call") {
    return await brokerRequest("POST", "/browser/tools/call", {
      name: message.name,
      arguments: message.arguments || {}
    });
  }
  throw new Error(`Unsupported message type: ${String(message.type)}`);
}

async function checkBrokerHealth() {
  return await brokerRequest("GET", "/health");
}

async function buildAssistantBrokerPayload(message) {
  await hostPolicyReady;
  validatePromptMessage(message);
  const needsPageAccess = Boolean(message.includePageContext);
  const forceBrowserAction = message.forceBrowserAction === true;
  let tab = null;

  if (needsPageAccess) {
    tab = await getActiveTab();
    if (!tab?.id || !tab.url) {
      throw new Error("Unable to resolve the active tab.");
    }
    if (!isHostAllowed(tab.url)) {
      throw createHostNotAllowlistedError(
        tab.url,
        "Active tab is not in the extension allowlist."
      );
    }
  }

  const pageContext = message.includePageContext && tab ? await capturePageContext(tab) : null;
  const riskSignals = detectRiskSignals(message.prompt);

  return {
    session_id: message.sessionId,
    prompt: message.prompt,
    page_context: pageContext,
    // Keep broker-side browser policy aligned with the extension runtime allowlist.
    allowed_hosts: normalizeAllowedHosts(),
    force_browser_action: forceBrowserAction,
    confirmed: message.confirmed === true,
    risk_signals: riskSignals
  };
}

async function routeAssistantQuery(message) {
  validateQueryMessage(message);
  const requestId = normalizeRequestId(message.requestId);
  const brokerPayload = await buildAssistantBrokerPayload(message);
  brokerPayload.backend = message.backend;
  brokerPayload.request_id = requestId;

  const controller = new AbortController();
  const timeoutMs = message.forceBrowserAction === true
    ? FORCED_BROWSER_ACTION_ROUTE_TIMEOUT_MS
    : 0;
  let timedOut = false;
  let timeoutHandle = null;
  if (timeoutMs > 0) {
    timeoutHandle = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, timeoutMs);
  }
  inflightRouteQueries.set(requestId, controller);
  try {
    return await brokerRequest("POST", "/route", brokerPayload, { signal: controller.signal });
  } catch (error) {
    if (error?.name === "AbortError") {
      if (timedOut) {
        try {
          await brokerRequest("POST", "/route/cancel", {
            session_id: message.sessionId,
            request_id: requestId
          });
        } catch {
          // Best effort cancel after timeout.
        }
        throw new Error("Browser action timed out waiting for a final response. Request cancelled.");
      }
      return {
        cancelled: true,
        request_id: requestId,
        session_id: message.sessionId,
        answer: null
      };
    }
    throw error;
  } finally {
    if (timeoutHandle) {
      clearTimeout(timeoutHandle);
    }
    inflightRouteQueries.delete(requestId);
  }
}

async function cancelAssistantQuery(message) {
  if (!message?.sessionId || typeof message.sessionId !== "string") {
    throw new Error("sessionId is required.");
  }
  const requestId = normalizeRequestId(message.requestId);
  const controller = inflightRouteQueries.get(requestId);
  if (controller) {
    controller.abort();
  }
  return await brokerRequest("POST", "/route/cancel", {
    session_id: message.sessionId,
    request_id: requestId
  });
}

async function startCodexRun(message) {
  const brokerPayload = await buildAssistantBrokerPayload(message);
  brokerPayload.backend = message.backend;
  if (message.rewriteMessageIndex !== undefined) {
    brokerPayload.rewrite_message_index = normalizeRewriteMessageIndex(message.rewriteMessageIndex);
  }
  return await brokerRequest("POST", "/runs", brokerPayload);
}

async function pollCodexRunEvents(message) {
  if (!message?.runId || typeof message.runId !== "string") {
    throw new Error("runId is required.");
  }
  const after = Number.isInteger(message.after) ? message.after : 0;
  const timeoutMs = Number.isInteger(message.timeoutMs) ? message.timeoutMs : 20_000;
  const path = `/runs/${encodeURIComponent(message.runId)}/events?after=${encodeURIComponent(after)}&timeout_ms=${encodeURIComponent(timeoutMs)}`;
  return await brokerRequest("GET", path);
}

async function submitCodexRunApproval(message) {
  if (!message?.runId || typeof message.runId !== "string") {
    throw new Error("runId is required.");
  }
  if (!message?.approvalId || typeof message.approvalId !== "string") {
    throw new Error("approvalId is required.");
  }
  if (message.decision !== "approve" && message.decision !== "deny") {
    throw new Error("decision must be 'approve' or 'deny'.");
  }
  const path = `/runs/${encodeURIComponent(message.runId)}/approval`;
  return await brokerRequest("POST", path, {
    approval_id: message.approvalId,
    decision: message.decision
  });
}

async function cancelCodexRun(message) {
  if (!message?.runId || typeof message.runId !== "string") {
    throw new Error("runId is required.");
  }
  const path = `/runs/${encodeURIComponent(message.runId)}/cancel`;
  return await brokerRequest("POST", path, {});
}

async function listConversations() {
  return await brokerRequest("GET", "/conversations");
}

async function getConversation(message) {
  if (!message?.sessionId || typeof message.sessionId !== "string") {
    throw new Error("sessionId is required.");
  }
  const path = `/conversations/${encodeURIComponent(message.sessionId)}`;
  return await brokerRequest("GET", path);
}

async function deleteConversation(message) {
  if (!message?.sessionId || typeof message.sessionId !== "string") {
    throw new Error("sessionId is required.");
  }
  const path = `/conversations/${encodeURIComponent(message.sessionId)}`;
  return await brokerRequest("DELETE", path);
}

async function rewriteConversation(message) {
  validateRewriteMessage(message);
  const requestId = normalizeRequestId(message.requestId);
  const brokerPayload = await buildAssistantBrokerPayload(message);
  brokerPayload.backend = message.backend;
  brokerPayload.request_id = requestId;
  brokerPayload.rewrite_message_index = normalizeRewriteMessageIndex(message.messageIndex);

  const controller = new AbortController();
  inflightRouteQueries.set(requestId, controller);
  try {
    const path = `/conversations/${encodeURIComponent(message.sessionId)}/rewrite`;
    return await brokerRequest("POST", path, brokerPayload, { signal: controller.signal });
  } catch (error) {
    if (error?.name === "AbortError") {
      return {
        cancelled: true,
        request_id: requestId,
        session_id: message.sessionId,
        answer: null
      };
    }
    throw error;
  } finally {
    inflightRouteQueries.delete(requestId);
  }
}

async function getModels() {
  return await brokerRequest("GET", "/models");
}

async function getMlxStatus() {
  return await brokerRequest("GET", "/mlx/status");
}

async function updateMlxConfig(message) {
  const generation = message?.generation && typeof message.generation === "object" ? message.generation : {};
  const body = { generation };
  if (Object.prototype.hasOwnProperty.call(message || {}, "systemPrompt")) {
    body.system_prompt = typeof message.systemPrompt === "string" ? message.systemPrompt : "";
  } else if (Object.prototype.hasOwnProperty.call(message || {}, "system_prompt")) {
    body.system_prompt = typeof message.system_prompt === "string" ? message.system_prompt : "";
  }
  return await brokerRequest("POST", "/mlx/config", body);
}

async function startMlxSession() {
  return await brokerRequest("POST", "/mlx/session/start", {});
}

async function stopMlxSession() {
  return await brokerRequest("POST", "/mlx/session/stop", {});
}

async function restartMlxSession() {
  return await brokerRequest("POST", "/mlx/session/restart", {});
}

async function listMlxAdapters() {
  return await brokerRequest("GET", "/mlx/adapters");
}

async function loadMlxAdapter(message) {
  const body = {};
  if (typeof message?.adapterId === "string" && message.adapterId.trim()) {
    body.adapter_id = message.adapterId.trim();
  }
  if (typeof message?.path === "string" && message.path.trim()) {
    body.path = message.path.trim();
  }
  if (typeof message?.name === "string" && message.name.trim()) {
    body.name = message.name.trim();
  }
  return await brokerRequest("POST", "/mlx/adapters/load", body);
}

async function unloadMlxAdapter() {
  return await brokerRequest("POST", "/mlx/adapters/unload", {});
}

async function brokerRequest(method, path, body = null, options = {}) {
  const headers = {
    "X-Assistant-Client": BROKER_CLIENT_HEADER
  };
  if (body !== null) {
    headers["Content-Type"] = "application/json";
  }

  const response = await fetch(`${BROKER_URL}${path}`, {
    method,
    headers,
    body: body === null ? undefined : JSON.stringify(body),
    signal: options.signal
  });
  let parsed = null;
  try {
    parsed = await response.json();
  } catch {
    parsed = null;
  }
  if (!response.ok) {
    const detail = parsed?.error ? ` ${parsed.error}` : "";
    const error = new Error(`Broker request failed (${response.status}).${detail}`);
    if (parsed?.error_code) {
      error.code = String(parsed.error_code);
    }
    if (parsed?.error_data && typeof parsed.error_data === "object") {
      error.data = parsed.error_data;
    }
    throw error;
  }
  return parsed || {};
}

function validateQueryMessage(message) {
  validatePromptMessage(message);
  const backend = String(message.backend || "codex").trim();
  if (backend !== "llama" && backend !== "codex" && backend !== "mlx") {
    throw new Error("backend must be 'llama', 'codex', or 'mlx'.");
  }
  if (message.requestId !== undefined && typeof message.requestId !== "string") {
    throw new Error("requestId must be a string when provided.");
  }
}

function validateRewriteMessage(message) {
  validateQueryMessage(message);
  normalizeRewriteMessageIndex(message.messageIndex);
}

function validatePromptMessage(message) {
  if (!message.sessionId || typeof message.sessionId !== "string") {
    throw new Error("sessionId is required.");
  }
  if (!message.prompt || typeof message.prompt !== "string") {
    throw new Error("prompt is required.");
  }
  if (message.forceBrowserAction !== undefined && typeof message.forceBrowserAction !== "boolean") {
    throw new Error("forceBrowserAction must be a boolean when provided.");
  }
}

function detectRiskSignals(prompt) {
  if (HIGH_RISK_PATTERN.test(prompt)) {
    return ["high_risk_prompt"];
  }
  return [];
}

function normalizeRequestId(value) {
  const requestId = String(value || "").trim();
  if (!requestId) {
    throw new Error("requestId is required.");
  }
  if (!/^[A-Za-z0-9._-]{1,128}$/.test(requestId)) {
    throw new Error("requestId is invalid.");
  }
  return requestId;
}

function normalizeRewriteMessageIndex(value) {
  const index = Number(value);
  if (!Number.isInteger(index) || index < 0) {
    throw new Error("messageIndex must be a non-negative integer.");
  }
  return index;
}

function normalizeHost(rawValue) {
  const input = String(rawValue || "").trim().toLowerCase();
  if (!input) {
    return "";
  }

  const wildcardTrimmed = input.replace(/^\*\./, "");
  let candidate = wildcardTrimmed;
  try {
    const parsed = wildcardTrimmed.includes("://")
      ? new URL(wildcardTrimmed)
      : new URL(`https://${wildcardTrimmed}`);
    candidate = String(parsed.hostname || "").trim().toLowerCase();
  } catch {
    candidate = wildcardTrimmed;
  }

  candidate = candidate
    .split("/")[0]
    .split("?")[0]
    .split("#")[0]
    .trim()
    .toLowerCase();

  if (candidate.includes(":")) {
    const colonIndex = candidate.lastIndexOf(":");
    const maybePort = candidate.slice(colonIndex + 1);
    if (/^\d+$/.test(maybePort)) {
      candidate = candidate.slice(0, colonIndex);
    }
  }

  candidate = candidate.replace(/^\.+|\.+$/g, "");
  if (!candidate || candidate.length > 253 || !/^[a-z0-9.-]+$/.test(candidate)) {
    return "";
  }
  return candidate;
}

function normalizeHostList(values, maxEntries = MAX_POLICY_HOSTS) {
  const parts = Array.isArray(values) ? values : [];
  const deduped = [];
  for (const part of parts) {
    const host = normalizeHost(part);
    if (!host || deduped.includes(host)) {
      continue;
    }
    deduped.push(host);
    if (deduped.length >= maxEntries) {
      break;
    }
  }
  return deduped;
}

function normalizeBlockedHosts(blockedHosts = null) {
  const source = blockedHosts === null ? hostPolicy.blockedHosts : blockedHosts;
  return normalizeHostList(source);
}

function normalizeAllowedHosts(allowedHosts = null) {
  const source =
    allowedHosts === null
      ? [...DEFAULT_ALLOWED_PAGE_HOSTS, ...hostPolicy.allowedHosts]
      : allowedHosts;
  const normalized = normalizeHostList(source);
  const blockedSet = new Set(normalizeBlockedHosts());
  return normalized.filter((host) => !blockedSet.has(host));
}

function hostPermissionOrigins(host) {
  const normalized = normalizeHost(host);
  if (!normalized) {
    return [];
  }
  const origins = new Set([
    `http://${normalized}/*`,
    `https://${normalized}/*`
  ]);
  if (!normalized.startsWith("*.")) {
    origins.add(`http://*.${normalized}/*`);
    origins.add(`https://*.${normalized}/*`);
  }
  return [...origins];
}

async function ensureHostPermission(rawHost) {
  const host = normalizeHost(rawHost);
  if (!host) {
    return;
  }

  if (!chrome.permissions?.contains || !chrome.permissions?.request) {
    return;
  }

  const origins = hostPermissionOrigins(host);
  if (origins.length === 0) {
    return;
  }

  let allowed = false;
  try {
    allowed = await chrome.permissions.contains({ origins });
  } catch (error) {
    console.warn(`[secure-panel] permission contains check failed for ${host}:`, String(error?.message || error));
    return;
  }
  if (allowed) {
    return;
  }

  try {
    const granted = await chrome.permissions.request({ origins });
    if (granted) {
      return;
    }
  } catch (error) {
    console.warn(`[secure-panel] permission request failed for ${host}:`, String(error?.message || error));
  }

  const error = new Error(
    `Extension host permission for ${host} was not granted. Allow page access when Chrome prompts.`
  );
  error.code = "host_permission_not_granted";
  throw error;
}

async function ensureUrlHostPermission(rawUrl) {
  if (typeof rawUrl !== "string") {
    return;
  }
  try {
    const parsed = new URL(rawUrl);
    if (!urlProtocolAllowed(parsed)) {
      return;
    }
    await ensureHostPermission(parsed.hostname);
  } catch {
    return;
  }
}

function urlProtocolAllowed(parsedUrl) {
  return ["http:", "https:"].includes(String(parsedUrl?.protocol || ""));
}

function extractUrlHost(rawUrl) {
  try {
    const parsed = new URL(String(rawUrl || ""));
    return normalizeHost(parsed.hostname || "");
  } catch {
    return "";
  }
}

function hostMatchesAllowedList(host, hosts) {
  return hosts.some((allowed) => host === allowed || host.endsWith(`.${allowed}`));
}

function isHostAllowed(rawUrl, allowedHosts = null) {
  const host = extractUrlHost(rawUrl);
  if (!host) {
    return false;
  }
  if (hostMatchesAllowedList(host, normalizeBlockedHosts())) {
    return false;
  }
  return hostMatchesAllowedList(host, normalizeAllowedHosts(allowedHosts));
}

function createHostNotAllowlistedError(rawUrl, message, allowedHosts = null) {
  const error = new Error(message || "Host is not in the extension allowlist.");
  error.code = "host_not_allowlisted";
  error.data = {
    host: extractUrlHost(rawUrl),
    url: typeof rawUrl === "string" ? rawUrl : "",
    effective_allowed_hosts: normalizeAllowedHosts(allowedHosts),
    blocked_hosts: normalizeBlockedHosts()
  };
  return error;
}

async function initializeHostPolicy() {
  try {
    const stored = await chrome.storage.local.get(HOST_POLICY_STORAGE_KEY);
    const raw = stored?.[HOST_POLICY_STORAGE_KEY];
    if (raw && typeof raw === "object") {
      hostPolicy.allowedHosts = normalizeHostList(raw.allowed_hosts ?? raw.allowedHosts ?? []);
      hostPolicy.blockedHosts = normalizeHostList(raw.blocked_hosts ?? raw.blockedHosts ?? []);
    }
  } catch (error) {
    console.warn("[secure-panel] failed to load host policy:", String(error?.message || error));
  }
}

async function persistHostPolicy() {
  try {
    await chrome.storage.local.set({
      [HOST_POLICY_STORAGE_KEY]: {
        allowed_hosts: [...hostPolicy.allowedHosts],
        blocked_hosts: [...hostPolicy.blockedHosts]
      }
    });
  } catch (error) {
    console.warn("[secure-panel] failed to persist host policy:", String(error?.message || error));
  }
}

function getHostPolicySnapshot() {
  const defaultHosts = normalizeHostList([...DEFAULT_ALLOWED_PAGE_HOSTS]);
  const customAllowedHosts = normalizeHostList(hostPolicy.allowedHosts);
  const blockedHosts = normalizeHostList(hostPolicy.blockedHosts);
  const effectiveAllowedHosts = normalizeAllowedHosts([...defaultHosts, ...customAllowedHosts]);
  return {
    default_hosts: defaultHosts,
    custom_allowed_hosts: customAllowedHosts,
    blocked_hosts: blockedHosts,
    effective_allowed_hosts: effectiveAllowedHosts
  };
}

function parseHostForPolicy(rawHost) {
  const host = normalizeHost(rawHost);
  if (host) {
    return host;
  }
  const error = new Error("host is required and must be a valid hostname.");
  error.code = "invalid_host";
  throw error;
}

async function updateHostPolicyState(nextAllowedHosts, nextBlockedHosts) {
  const blocked = normalizeHostList(nextBlockedHosts);
  const blockedSet = new Set(blocked);
  const allowed = normalizeHostList(nextAllowedHosts).filter((host) => !blockedSet.has(host));
  hostPolicy.allowedHosts = allowed;
  hostPolicy.blockedHosts = blocked;
  await persistHostPolicy();
  return getHostPolicySnapshot();
}

async function allowHost(rawHost) {
  await hostPolicyReady;
  const host = parseHostForPolicy(rawHost);
  const defaultHosts = new Set(normalizeHostList([...DEFAULT_ALLOWED_PAGE_HOSTS]));
  const nextAllowed = hostPolicy.allowedHosts.filter((value) => value !== host);
  if (!defaultHosts.has(host)) {
    nextAllowed.push(host);
  }
  const nextBlocked = hostPolicy.blockedHosts.filter((value) => value !== host);
  return await updateHostPolicyState(nextAllowed, nextBlocked);
}

async function blockHost(rawHost) {
  await hostPolicyReady;
  const host = parseHostForPolicy(rawHost);
  const nextAllowed = hostPolicy.allowedHosts.filter((value) => value !== host);
  const nextBlocked = [...hostPolicy.blockedHosts.filter((value) => value !== host), host];
  return await updateHostPolicyState(nextAllowed, nextBlocked);
}

async function removeAllowedHost(rawHost) {
  await hostPolicyReady;
  const host = parseHostForPolicy(rawHost);
  const nextAllowed = hostPolicy.allowedHosts.filter((value) => value !== host);
  return await updateHostPolicyState(nextAllowed, hostPolicy.blockedHosts);
}

async function unblockHost(rawHost) {
  await hostPolicyReady;
  const host = parseHostForPolicy(rawHost);
  const nextBlocked = hostPolicy.blockedHosts.filter((value) => value !== host);
  return await updateHostPolicyState(hostPolicy.allowedHosts, nextBlocked);
}

function ensureUrl(rawUrl, allowedHosts = null) {
  if (typeof rawUrl !== "string" || rawUrl.trim().length === 0) {
    throw new Error("Missing url");
  }
  const parsed = new URL(rawUrl);
  if (!["http:", "https:"].includes(parsed.protocol)) {
    throw new Error("URL protocol must be http or https.");
  }
  const url = parsed.toString();
  if (!isHostAllowed(url, allowedHosts)) {
    throw createHostNotAllowlistedError(url, "Target URL host is not allowlisted.", allowedHosts);
  }
  return url;
}

function ensureSelector(selector) {
  if (typeof selector !== "string" || selector.trim().length === 0) {
    throw new Error("Missing selector");
  }
  return selector.trim();
}

function parseTabId(tabId) {
  const parsed = Number.parseInt(String(tabId), 10);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`Invalid tabId: ${tabId}`);
  }
  return parsed;
}

async function getActiveTab() {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  return tab || null;
}

async function resolveTabId(tabId) {
  if (tabId !== undefined && tabId !== null) {
    return parseTabId(tabId);
  }
  const activeTab = await getActiveTab();
  if (!activeTab || typeof activeTab.id !== "number") {
    throw new Error("No active tab available.");
  }
  return activeTab.id;
}

async function getAllowedTab(tabId, allowedHosts) {
  const tab = await chrome.tabs.get(tabId);
  if (!tab?.url || !isHostAllowed(tab.url, allowedHosts)) {
    throw createHostNotAllowlistedError(tab?.url || "", "Tab URL is not allowlisted.", allowedHosts);
  }
  return tab;
}

async function waitForTabLoad(tabId, timeoutMs) {
  const tab = await chrome.tabs.get(tabId);
  if (tab.status === "complete") {
    return;
  }

  await new Promise((resolve, reject) => {
    let done = false;
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("Timed out waiting for tab load."));
    }, timeoutMs);

    const listener = (updatedTabId, changeInfo) => {
      if (updatedTabId !== tabId) {
        return;
      }
      if (changeInfo.status === "complete") {
        cleanup();
        resolve();
      }
    };

    function cleanup() {
      if (done) {
        return;
      }
      done = true;
      clearTimeout(timer);
      chrome.tabs.onUpdated.removeListener(listener);
    }

    chrome.tabs.onUpdated.addListener(listener);
  });
}

async function capturePageContext(tab) {
  const fallback = {
    title: typeof tab?.title === "string" ? tab.title.slice(0, 200) : "",
    url: typeof tab?.url === "string" ? tab.url : "",
    selection: "",
    text_excerpt: ""
  };

  try {
    await ensureUrlHostPermission(tab?.url);
    const tabId = parseTabId(tab?.id);
    const [injected] = await chrome.scripting.executeScript({
      target: { tabId },
      func: (selectionLimit, textLimit) => {
        const normalize = (value, limit) =>
          String(value || "").replace(/\s+/g, " ").trim().slice(0, limit);
        const selection = normalize(
          typeof window.getSelection === "function" ? window.getSelection()?.toString() : "",
          selectionLimit
        );
        const bodyText =
          typeof document.body?.innerText === "string"
            ? document.body.innerText
            : typeof document.documentElement?.innerText === "string"
              ? document.documentElement.innerText
              : typeof document.body?.textContent === "string"
                ? document.body.textContent
                : typeof document.documentElement?.textContent === "string"
                  ? document.documentElement.textContent
                  : "";
        return {
          title: normalize(document.title, 200),
          url: String(location?.href || "").slice(0, 2000),
          selection,
          text_excerpt: normalize(bodyText, textLimit)
        };
      },
      args: [PAGE_CONTEXT_SELECTION_CHARS, PAGE_CONTEXT_TEXT_CHARS]
    });

    const result = injected?.result;
    if (!result || typeof result !== "object") {
      return fallback;
    }

    return {
      title: typeof result.title === "string" && result.title ? result.title : fallback.title,
      url: typeof result.url === "string" && result.url ? result.url : fallback.url,
      selection: typeof result.selection === "string" ? result.selection : "",
      text_excerpt: typeof result.text_excerpt === "string" ? result.text_excerpt : ""
    };
  } catch (error) {
    console.warn("[secure-panel] page context capture fallback:", String(error?.message || error));
    return fallback;
  }
}

async function runInTab(tabId, func, args = []) {
  const tab = await chrome.tabs.get(tabId);
  await ensureUrlHostPermission(tab?.url);
  const results = await chrome.scripting.executeScript({
    target: { tabId },
    func,
    args
  });
  return results?.[0]?.result ?? null;
}

async function startBrokerCommandLoop() {
  if (relayLoopStarted) {
    return;
  }
  relayLoopStarted = true;

  while (true) {
    try {
      await brokerRequest("POST", "/extension/register", {
        client_id: relayClientId,
        version: RELAY_COMMAND_LOOP_VERSION,
        platform: "chrome-sidepanel"
      });

      const next = await brokerRequest(
        "GET",
        `/extension/next?client_id=${encodeURIComponent(relayClientId)}&timeout_ms=${RELAY_POLL_TIMEOUT_MS}`
      );

      if (next?.command) {
        await executeAndReportBrokerCommand(next.command);
      }

      relayBackoffMs = RELAY_INITIAL_BACKOFF_MS;
    } catch (error) {
      console.warn("[secure-panel] broker command loop error:", String(error?.message || error));
      await delay(relayBackoffMs);
      relayBackoffMs = Math.min(relayBackoffMs * 2, RELAY_MAX_BACKOFF_MS);
    }
  }
}

async function executeAndReportBrokerCommand(command) {
  const commandId = command?.command_id || command?.commandId;
  if (!commandId) {
    return;
  }

  let success = true;
  let data = null;
  let error = null;

  try {
    data = await executeBrokerCommand(command.method, command.args || {});
  } catch (commandError) {
    success = false;
    error = {
      message: String(commandError?.message || commandError || "Command execution failed.")
    };
  }

  await brokerRequest("POST", "/extension/result", {
    client_id: relayClientId,
    command_id: commandId,
    success,
    data,
    error
  });
}

async function executeBrokerCommand(method, args) {
  await hostPolicyReady;
  const allowedHosts = normalizeAllowedHosts(args.allowedHosts);
  switch (method) {
    case "navigate":
      return await commandNavigate(args, allowedHosts);
    case "open_tab":
      return await commandOpenTab(args, allowedHosts);
    case "switch_tab":
      return await commandSwitchTab(args, allowedHosts);
    case "focus_tab":
      return await commandFocusTab(args, allowedHosts);
    case "close_tab":
      return await commandCloseTab(args, allowedHosts);
    case "get_tabs":
      return await commandGetTabs(allowedHosts);
    case "describe_session_tabs":
      return await commandDescribeSessionTabs(allowedHosts);
    case "group_tabs":
      return await commandGroupTabs(args, allowedHosts);
    case "click":
      return await commandClick(args, allowedHosts);
    case "type":
      return await commandType(args, allowedHosts);
    case "press_key":
      return await commandPressKey(args, allowedHosts);
    case "scroll":
      return await commandScroll(args, allowedHosts);
    case "get_content":
      return await commandGetContent(args, allowedHosts);
    default:
      throw new Error(`Unsupported command method: ${method}`);
  }
}

async function commandNavigate(args, allowedHosts) {
  const tabId = await resolveTabId(args.tabId);
  const url = ensureUrl(args.url, allowedHosts);
  await ensureUrlHostPermission(url);
  await chrome.tabs.update(tabId, { url });
  await waitForTabLoad(tabId, 20_000);
  const tab = await chrome.tabs.get(tabId);
  if (!tab?.url || !isHostAllowed(tab.url, allowedHosts)) {
    throw new Error("Navigation completed on a non-allowlisted host.");
  }
  return {
    tabId,
    requestedUrl: url,
    finalUrl: tab.url,
    title: tab.title ?? null
  };
}

async function commandOpenTab(args, allowedHosts) {
  const url = ensureUrl(args.url, allowedHosts);
  await ensureUrlHostPermission(url);
  const tab = await chrome.tabs.create({ url, active: true });
  if (typeof tab?.id !== "number") {
    throw new Error("Unable to open browser tab.");
  }
  let resolvedTab = tab;
  try {
    await waitForTabLoad(tab.id, 20_000);
    const latest = await chrome.tabs.get(tab.id);
    if (latest) {
      resolvedTab = latest;
    }
  } catch {
    // Keep best-effort behavior: if loading is slow, still return the opened tab.
  }

  let policyUrl = resolvedTab.pendingUrl || resolvedTab.url || url;
  try {
    const parsed = new URL(policyUrl);
    if (!["http:", "https:"].includes(parsed.protocol)) {
      policyUrl = url;
    }
  } catch {
    policyUrl = url;
  }

  if (!isHostAllowed(policyUrl, allowedHosts)) {
    await chrome.tabs.remove(tab.id);
    throw new Error("Opened tab host is not allowlisted.");
  }
  return {
    tabId: tab.id,
    url: resolvedTab.url || resolvedTab.pendingUrl || url,
    title: resolvedTab.title ?? null
  };
}

async function commandSwitchTab(args, allowedHosts) {
  const tabId = parseTabId(args.tabId);
  await getAllowedTab(tabId, allowedHosts);
  const tab = await chrome.tabs.update(tabId, { active: true });
  return {
    tabId,
    url: tab.url ?? null,
    title: tab.title ?? null
  };
}

async function commandFocusTab(args, allowedHosts) {
  const tabId = parseTabId(args.tabId);
  await getAllowedTab(tabId, allowedHosts);
  const tab = await chrome.tabs.update(tabId, { active: true });
  return {
    tabId,
    focused: true,
    url: tab.url ?? null,
    title: tab.title ?? null
  };
}

async function commandCloseTab(args, allowedHosts) {
  const tabId = parseTabId(args.tabId);
  await getAllowedTab(tabId, allowedHosts);
  await chrome.tabs.remove(tabId);
  return {
    tabId,
    closed: true
  };
}

async function commandGetTabs(allowedHosts) {
  const tabs = await chrome.tabs.query({ currentWindow: true });
  const filtered = tabs.filter((tab) => typeof tab.id === "number" && tab.url && isHostAllowed(tab.url, allowedHosts));
  const activeTab = filtered.find((tab) => tab.active);
  return {
    activeTabId: typeof activeTab?.id === "number" ? activeTab.id : null,
    tabs: filtered.map((tab) => ({
      tabId: tab.id,
      title: tab.title ?? null,
      url: tab.url ?? null,
      active: tab.active === true,
      groupId: typeof tab.groupId === "number" && tab.groupId >= 0 ? tab.groupId : null
    }))
  };
}

async function commandDescribeSessionTabs(allowedHosts) {
  const tabsResult = await commandGetTabs(allowedHosts);
  const groupsRaw = await chrome.tabGroups.query({ windowId: chrome.windows.WINDOW_ID_CURRENT });
  const groupById = new Map(
    groupsRaw.map((group) => [
      group.id,
      {
        groupId: group.id,
        groupName: group.title ?? "Session Group",
        color: group.color ?? "grey",
        collapsed: group.collapsed === true,
        tabIds: []
      }
    ])
  );

  for (const tab of tabsResult.tabs) {
    if (typeof tab.groupId === "number" && tab.groupId >= 0) {
      const group = groupById.get(tab.groupId);
      if (group) {
        group.tabIds.push(tab.tabId);
      }
    }
  }

  const groups = [...groupById.values()].filter((group) => group.tabIds.length > 0);
  return {
    activeTabId: tabsResult.activeTabId,
    tabCount: tabsResult.tabs.length,
    tabs: tabsResult.tabs.map((tab) => {
      const group = typeof tab.groupId === "number" ? groupById.get(tab.groupId) : null;
      return {
        tabId: tab.tabId,
        title: tab.title,
        url: tab.url,
        groupId: tab.groupId,
        groupName: group?.groupName ?? null
      };
    }),
    groups
  };
}

async function commandGroupTabs(args, allowedHosts) {
  const tabIds = Array.isArray(args.tabIds) ? args.tabIds.map(parseTabId) : [];
  if (tabIds.length === 0) {
    throw new Error("group_tabs requires at least one tabId.");
  }
  for (const tabId of tabIds) {
    await getAllowedTab(tabId, allowedHosts);
  }

  const groupName = typeof args.groupName === "string" && args.groupName.trim() ? args.groupName.trim() : "Session Group";
  const color = normalizeGroupColor(args.color);
  const collapsed = args.collapsed === true;

  const groupId = await chrome.tabs.group({ tabIds });
  await chrome.tabGroups.update(groupId, {
    title: groupName,
    color,
    collapsed
  });

  return {
    groupId,
    groupName,
    color,
    collapsed,
    tabIds
  };
}

async function commandClick(args, allowedHosts) {
  const tabId = await resolveTabId(args.tabId);
  await getAllowedTab(tabId, allowedHosts);
  const selector = ensureSelector(args.selector);
  const result = await runInTab(
    tabId,
    (sel) => {
      const element = document.querySelector(sel);
      if (!element) {
        return { clicked: false, error: "selector_not_found" };
      }
      element.scrollIntoView({ block: "center", inline: "center" });
      const rect = element.getBoundingClientRect();
      const x = Math.round(rect.left + rect.width / 2);
      const y = Math.round(rect.top + rect.height / 2);
      element.click();
      return {
        clicked: true,
        x,
        y,
        tagName: String(element.tagName || "").toLowerCase(),
        textPreview: String((element.innerText || "").slice(0, 120)),
        finalUrl: window.location.href,
        title: document.title
      };
    },
    [selector]
  );

  if (!result?.clicked) {
    throw new Error(`click failed: ${selector}`);
  }
  return result;
}

async function commandType(args, allowedHosts) {
  const tabId = await resolveTabId(args.tabId);
  await getAllowedTab(tabId, allowedHosts);
  const selector = ensureSelector(args.selector);
  const text = typeof args.text === "string" ? args.text : "";
  const clear = args.clear !== false;

  const result = await runInTab(
    tabId,
    (sel, value, shouldClear) => {
      const element = document.querySelector(sel);
      if (!element) {
        return { ok: false, error: "selector_not_found" };
      }
      element.scrollIntoView({ block: "center", inline: "center" });
      try {
        element.focus({ preventScroll: true });
      } catch {
        element.focus();
      }

      const isInput = element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement;
      const isEditable = element.isContentEditable;
      if (!isInput && !isEditable) {
        return { ok: false, error: "not_editable" };
      }

      if (isInput) {
        const current = shouldClear ? "" : String(element.value ?? "");
        element.value = current + value;
      } else {
        const current = shouldClear ? "" : String(element.textContent ?? "");
        element.textContent = current + value;
      }

      element.dispatchEvent(new Event("input", { bubbles: true }));
      element.dispatchEvent(new Event("change", { bubbles: true }));

      return {
        ok: true,
        typedChars: value.length,
        tagName: String(element.tagName || "").toLowerCase(),
        type: String(element.getAttribute("type") || "").toLowerCase()
      };
    },
    [selector, text, clear]
  );

  if (!result?.ok) {
    throw new Error(`type failed: ${selector}`);
  }

  return {
    typedChars: Number(result.typedChars ?? text.length),
    tagName: result.tagName ?? null,
    type: result.type ?? null
  };
}

async function commandPressKey(args, allowedHosts) {
  const tabId = await resolveTabId(args.tabId);
  await getAllowedTab(tabId, allowedHosts);
  const key = typeof args.key === "string" && args.key.length > 0 ? args.key : "Enter";
  const modifiers = Array.isArray(args.modifiers)
    ? args.modifiers.filter((modifier) => typeof modifier === "string")
    : [];
  const repeat = Math.max(1, Number.parseInt(String(args.repeat ?? 1), 10));
  const delayMs = Math.max(0, Number.parseInt(String(args.delayMs ?? 0), 10));

  await runInTab(
    tabId,
    async (pressedKey, modifierList, totalRepeat, perRepeatDelayMs) => {
      const target = document.activeElement || document.body;
      const modifierFlags = {
        altKey: modifierList.includes("alt"),
        ctrlKey: modifierList.includes("ctrl") || modifierList.includes("control"),
        metaKey: modifierList.includes("meta") || modifierList.includes("cmd") || modifierList.includes("command"),
        shiftKey: modifierList.includes("shift")
      };

      for (let index = 0; index < totalRepeat; index += 1) {
        target.dispatchEvent(
          new KeyboardEvent("keydown", {
            key: pressedKey,
            code: pressedKey,
            bubbles: true,
            cancelable: true,
            ...modifierFlags
          })
        );
        target.dispatchEvent(
          new KeyboardEvent("keyup", {
            key: pressedKey,
            code: pressedKey,
            bubbles: true,
            cancelable: true,
            ...modifierFlags
          })
        );

        if (perRepeatDelayMs > 0 && index < totalRepeat - 1) {
          await new Promise((resolve) => setTimeout(resolve, perRepeatDelayMs));
        }
      }
      return true;
    },
    [key, modifiers.map((value) => value.toLowerCase()), repeat, delayMs]
  );

  return {
    key,
    code: key,
    repeat,
    modifiers
  };
}

async function commandScroll(args, allowedHosts) {
  const tabId = await resolveTabId(args.tabId);
  await getAllowedTab(tabId, allowedHosts);
  const deltaX = Number.parseFloat(String(args.deltaX ?? 0));
  const deltaY = Number.parseFloat(String(args.deltaY ?? 600));
  const selector =
    typeof args.selector === "string" && args.selector.trim().length > 0 ? args.selector.trim() : null;

  const result = await runInTab(
    tabId,
    (x, y, sel) => {
      if (sel) {
        const element = document.querySelector(sel);
        if (!element) {
          return { ok: false, error: "selector_not_found" };
        }
        element.scrollBy({ left: x, top: y, behavior: "instant" });
        const rect = element.getBoundingClientRect();
        return {
          ok: true,
          x: Math.round(rect.left + rect.width / 2),
          y: Math.round(rect.top + rect.height / 2),
          deltaX: x,
          deltaY: y
        };
      }

      window.scrollBy({ left: x, top: y, behavior: "instant" });
      return {
        ok: true,
        x: Math.round((window.innerWidth || 1200) / 2),
        y: Math.round((window.innerHeight || 900) / 2),
        deltaX: x,
        deltaY: y
      };
    },
    [deltaX, deltaY, selector]
  );

  if (!result?.ok) {
    throw new Error(`scroll failed${selector ? `: ${selector}` : ""}`);
  }
  return result;
}

async function commandGetContent(args, allowedHosts) {
  const tabId = await resolveTabId(args.tabId);
  await getAllowedTab(tabId, allowedHosts);
  const requestedMaxChars = Number.parseInt(String(args.maxChars ?? 6000), 10);
  const maxChars = Number.isFinite(requestedMaxChars) && requestedMaxChars > 0
    ? Math.min(requestedMaxChars, 50_000)
    : 6_000;
  const selector =
    typeof args.selector === "string" && args.selector.trim().length > 0 ? args.selector.trim() : null;

  return await runInTab(
    tabId,
    (limit, sel) => {
      const node = sel ? document.querySelector(sel) : document.documentElement;
      if (!node) {
        return {
          html: "",
          fullLength: 0,
          truncated: false,
          finalUrl: window.location.href,
          selector: sel,
          selectorMatched: false
        };
      }
      const html = typeof node.outerHTML === "string" ? node.outerHTML : String(node.textContent ?? "");
      return {
        html: html.slice(0, limit),
        fullLength: html.length,
        truncated: html.length > limit,
        finalUrl: window.location.href,
        selector: sel,
        selectorMatched: sel ? true : null
      };
    },
    [maxChars, selector]
  );
}

function normalizeGroupColor(value) {
  const allowed = new Set([
    "grey",
    "blue",
    "red",
    "yellow",
    "green",
    "pink",
    "purple",
    "cyan",
    "orange"
  ]);
  const normalized =
    typeof value === "string" && value.trim().length > 0 ? value.trim().toLowerCase() : "grey";
  return allowed.has(normalized) ? normalized : "grey";
}

async function delay(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
