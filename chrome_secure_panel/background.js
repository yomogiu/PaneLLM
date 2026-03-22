const BROKER_URL = "http://127.0.0.1:7777";
const BROKER_CLIENT_HEADER = "chrome-sidepanel-v1";
const RELAY_POLL_TIMEOUT_MS = 25_000;
const RELAY_INITIAL_BACKOFF_MS = 1_000;
const RELAY_MAX_BACKOFF_MS = 15_000;
const RELAY_COMMAND_LOOP_VERSION = "0.2.0";
const PAGE_CONTEXT_TEXT_CHARS = 5_000;
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
const hostPolicy = {
  allowedHosts: []
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
  if (message.type === "assistant.run.start") {
    return await startAssistantRun(message);
  }
  if (message.type === "assistant.run.events") {
    return await pollAssistantRunEvents(message);
  }
  if (message.type === "assistant.run.approval") {
    return await submitAssistantRunApproval(message);
  }
  if (message.type === "assistant.run.cancel") {
    return await cancelAssistantRun(message);
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
  if (message.type === "assistant.paper.get") {
    return await getPaperWorkspace(message);
  }
  if (message.type === "assistant.paper.memory_query") {
    return await queryPaperMemory(message);
  }
  if (message.type === "assistant.paper.summary_request") {
    return await requestPaperSummary(message);
  }
  if (message.type === "assistant.paper.highlights_capture") {
    return await capturePaperHighlights(message);
  }
  if (message.type === "assistant.paper.summary_generate") {
    return await generatePaperSummary(message);
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
  if (message.type === "assistant.mlx.training.datasets.import") {
    return await importTrainingDataset(message);
  }
  if (message.type === "assistant.mlx.training.datasets.list") {
    return await listTrainingDatasets();
  }
  if (message.type === "assistant.mlx.training.datasets.get") {
    return await getTrainingDataset(message);
  }
  if (message.type === "assistant.mlx.training.datasets.delete") {
    return await deleteTrainingDataset(message);
  }
  if (message.type === "assistant.mlx.training.job.start") {
    return await startTrainingJob(message);
  }
  if (message.type === "assistant.mlx.training.job.get") {
    return await getTrainingJob(message);
  }
  if (message.type === "assistant.mlx.training.runs.list") {
    return await listTrainingRuns();
  }
  if (message.type === "assistant.mlx.training.runs.get") {
    return await getTrainingRun(message);
  }
  if (message.type === "assistant.mlx.training.checkpoint.promote") {
    return await promoteTrainingCheckpoint(message);
  }
  if (message.type === "assistant.jobs.list") {
    return await listJobs(message);
  }
  if (message.type === "assistant.jobs.cancel") {
    return await cancelJob(message);
  }
  if (message.type === "assistant.read.context.capture") {
    return await captureReadAssistantContext(message);
  }
  if (message.type === "assistant.experiments.job.start") {
    return await startExperimentJob(message);
  }
  if (message.type === "assistant.experiments.job.get") {
    return await getExperimentJob(message);
  }
  if (message.type === "assistant.experiments.list") {
    return await listExperiments();
  }
  if (message.type === "assistant.experiments.get") {
    return await getExperiment(message);
  }
  if (message.type === "assistant.experiments.compare") {
    return await compareExperiments(message);
  }
  if (message.type === "assistant.tools.browser_config.get") {
    return await getBrowserConfig();
  }
  if (message.type === "assistant.tools.browser_config.update") {
    return await updateBrowserConfig(message);
  }
  if (message.type === "assistant.tools.page_hosts.get") {
    return { policy: getHostPolicySnapshot() };
  }
  if (message.type === "assistant.tools.page_hosts.allow") {
    return { policy: await allowHost(message.host) };
  }
  if (message.type === "assistant.tools.page_hosts.remove_allow") {
    return { policy: await removeAllowedHost(message.host) };
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
        title: String(activeTab.title || ""),
        allowed: isHostAllowed(activeTab.url, snapshot.effective_allowed_hosts)
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


async function captureReadAssistantContext(_message) {
  const activeTab = await getActiveTab();
  if (!activeTab?.url || typeof activeTab.id !== "number") {
    return {
      ok: false,
      error: "Unable to resolve the active tab.",
      context: null,
      active_tab: null
    };
  }
  const host = extractUrlHost(activeTab.url);
  if (!host) {
    return {
      ok: false,
      error: "Active tab host is invalid.",
      context: null,
      active_tab: null
    };
  }
  const snapshot = getHostPolicySnapshot();
  const activeInfo = {
    host,
    url: String(activeTab.url),
    title: String(activeTab.title || ""),
    allowed: isHostAllowed(activeTab.url, snapshot.effective_allowed_hosts)
  };
  if (!activeInfo.allowed) {
    return {
      ok: false,
      error: "Active tab is not in the extension allowlist.",
      context: null,
      active_tab: activeInfo
    };
  }
  return {
    context: await capturePageContext(activeTab),
    active_tab: activeInfo
  };
}

async function buildAssistantBrokerPayload(message) {
  await hostPolicyReady;
  validatePromptMessage(message);
  const includePageContext = Boolean(message.includePageContext);
  const forceBrowserAction = message.forceBrowserAction === true;
  let tab = null;

  if (includePageContext) {
    tab = await getActiveTab();
    if (tab?.id && tab.url) {
      if (!isHostAllowed(tab.url)) {
        throw createHostNotAllowlistedError(
          tab.url,
          "Active tab is not in the extension allowlist."
        );
      }
    } else {
      tab = null;
    }
  }

  const pageContext = message.includePageContext && tab ? await capturePageContext(tab) : null;
  const riskSignals = detectRiskSignals(message.prompt);
  const chatTemplateKwargs =
    message.chatTemplateKwargs && typeof message.chatTemplateKwargs === "object"
      ? message.chatTemplateKwargs
      : message.chat_template_kwargs && typeof message.chat_template_kwargs === "object"
        ? message.chat_template_kwargs
        : null;
  const reasoningBudget =
    message.reasoningBudget !== undefined
      ? message.reasoningBudget
      : message.reasoning_budget;
  const paperContext =
    message.paperContext && typeof message.paperContext === "object"
      ? message.paperContext
      : message.paper_context && typeof message.paper_context === "object"
        ? message.paper_context
        : null;
  const highlightContext =
    message.highlightContext && typeof message.highlightContext === "object"
      ? message.highlightContext
      : message.highlight_context && typeof message.highlight_context === "object"
        ? message.highlight_context
        : null;

  const payload = {
    session_id: message.sessionId,
    prompt: message.prompt,
    request_prompt_suffix: String(message.requestPromptSuffix || "").trim(),
    include_page_context: includePageContext,
    includePageContext: includePageContext,
    page_context: pageContext,
    // Keep broker-side browser policy aligned with the extension runtime allowlist.
    allowed_hosts: normalizeAllowedHosts(),
    force_browser_action: forceBrowserAction,
    confirmed: message.confirmed === true,
    risk_signals: riskSignals
  };
  if (paperContext) {
    payload.paper_context = paperContext;
  }
  if (highlightContext) {
    payload.highlight_context = highlightContext;
  }
  if (chatTemplateKwargs) {
    payload.chat_template_kwargs = chatTemplateKwargs;
  }
  if (reasoningBudget !== undefined) {
    payload.reasoning_budget = reasoningBudget;
  }
  return payload;
}

async function startAssistantRun(message) {
  validateRunStartMessage(message);
  const brokerPayload = await buildAssistantBrokerPayload(message);
  brokerPayload.backend = message.backend;
  if (message.rewriteMessageIndex !== undefined) {
    brokerPayload.rewrite_message_index = normalizeRewriteMessageIndex(message.rewriteMessageIndex);
  }
  return await brokerRequest("POST", "/runs", brokerPayload);
}

async function pollAssistantRunEvents(message) {
  if (!message?.runId || typeof message.runId !== "string") {
    throw new Error("runId is required.");
  }
  const after = Number.isInteger(message.after) ? message.after : 0;
  const timeoutMs = Number.isInteger(message.timeoutMs) ? message.timeoutMs : 20_000;
  const path = `/runs/${encodeURIComponent(message.runId)}/events?after=${encodeURIComponent(after)}&timeout_ms=${encodeURIComponent(timeoutMs)}`;
  return await brokerRequest("GET", path);
}

async function submitAssistantRunApproval(message) {
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

async function cancelAssistantRun(message) {
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

async function getPaperWorkspace(message) {
  const source = String(message?.source || "").trim();
  const paperId = String(message?.paperId || message?.paper_id || "").trim();
  if (!source) {
    throw new Error("source is required.");
  }
  if (!paperId) {
    throw new Error("paperId is required.");
  }
  const params = new URLSearchParams({
    source,
    paper_id: paperId
  });
  return await brokerRequest("GET", `/papers/lookup?${params.toString()}`);
}

async function requestPaperSummary(message) {
  const paper = message?.paper && typeof message.paper === "object" ? message.paper : null;
  if (!paper) {
    throw new Error("paper is required.");
  }
  return await brokerRequest("POST", "/papers/summary_request", {
    paper,
    conversation_id:
      typeof message.conversationId === "string"
        ? message.conversationId
        : typeof message.conversation_id === "string"
          ? message.conversation_id
          : ""
  });
}

async function queryPaperMemory(message) {
  const paper = message?.paper && typeof message.paper === "object" ? message.paper : null;
  if (!paper) {
    throw new Error("paper is required.");
  }
  return await brokerRequest("POST", "/papers/memory_query", {
    paper,
    query: typeof message?.query === "string" ? message.query : "",
    limit: Number.isFinite(Number(message?.limit)) ? Number(message.limit) : 8,
    exclude_conversation_id:
      typeof message?.excludeConversationId === "string"
        ? message.excludeConversationId
        : typeof message?.exclude_conversation_id === "string"
          ? message.exclude_conversation_id
          : ""
  });
}

async function capturePaperHighlights(message) {
  const paper = message?.paper && typeof message.paper === "object" ? message.paper : null;
  if (!paper) {
    throw new Error("paper is required.");
  }
  return await brokerRequest("POST", "/papers/highlights_capture", {
    paper,
    conversation_id:
      typeof message.conversationId === "string"
        ? message.conversationId
        : typeof message.conversation_id === "string"
          ? message.conversation_id
          : ""
  });
}

async function generatePaperSummary(message) {
  const paper = message?.paper && typeof message.paper === "object" ? message.paper : null;
  if (!paper) {
    throw new Error("paper is required.");
  }
  const sessionId =
    typeof message?.sessionId === "string"
      ? message.sessionId
      : typeof message?.session_id === "string"
        ? message.session_id
        : "";
  if (!sessionId) {
    throw new Error("sessionId is required.");
  }
  const capture = await captureReadAssistantContext(message);
  if (capture?.ok === false) {
    throw new Error(
      typeof capture.error === "string"
        ? capture.error
        : capture.error?.message || "Unable to capture the current page context."
    );
  }
  if (!capture?.context || typeof capture.context !== "object") {
    throw new Error("Page context is unavailable for the active tab.");
  }
  return await brokerRequest("POST", "/papers/summary_generate", {
    paper,
    session_id: sessionId,
    backend: typeof message?.backend === "string" ? message.backend : "codex",
    page_context: capture.context
  });
}

async function getModels() {
  return await brokerRequest("GET", "/models");
}


async function getMlxStatus() {
  const models = await getModels();
  return { mlx: models.mlx || {} };
}

async function updateMlxConfig(_message) {
  throw new Error("The Models tab has been deprecated.");
}

async function startMlxSession() {
  throw new Error("The Models tab has been deprecated.");
}

async function stopMlxSession() {
  throw new Error("The Models tab has been deprecated.");
}

async function restartMlxSession() {
  throw new Error("The Models tab has been deprecated.");
}

async function listMlxAdapters() {
  return { adapters: [], active_adapter: null };
}

async function loadMlxAdapter(_message) {
  throw new Error("The Models tab has been deprecated.");
}

async function unloadMlxAdapter() {
  throw new Error("The Models tab has been deprecated.");
}

async function importTrainingDataset(_message) {
  throw new Error("The Models tab has been deprecated.");
}

async function listTrainingDatasets() {
  return { datasets: [] };
}

async function getTrainingDataset(_message) {
  throw new Error("The Models tab has been deprecated.");
}

async function deleteTrainingDataset(_message) {
  throw new Error("The Models tab has been deprecated.");
}

async function startTrainingJob(_message) {
  throw new Error("The Models tab has been deprecated.");
}

async function getTrainingJob(_message) {
  throw new Error("The Models tab has been deprecated.");
}

async function listTrainingRuns() {
  return { runs: [] };
}

async function getTrainingRun(_message) {
  throw new Error("The Models tab has been deprecated.");
}

async function promoteTrainingCheckpoint(_message) {
  throw new Error("The Models tab has been deprecated.");
}

async function listJobs(message) {
  const params = new URLSearchParams();
  if (typeof message?.kind === "string" && message.kind.trim()) {
    params.set("kind", message.kind.trim());
  }
  if (typeof message?.status === "string" && message.status.trim()) {
    params.set("status", message.status.trim());
  }
  const query = params.toString();
  return await brokerRequest("GET", query ? `/jobs?${query}` : "/jobs");
}

async function cancelJob(message) {
  if (!message?.jobId || typeof message.jobId !== "string") {
    throw new Error("jobId is required.");
  }
  const path = `/jobs/${encodeURIComponent(message.jobId)}/cancel`;
  return await brokerRequest("POST", path, {});
}

async function startExperimentJob(message) {
  const body = {};
  if (typeof message?.kind === "string" && message.kind.trim()) {
    body.kind = message.kind.trim();
  }
  if (typeof message?.modelPath === "string" && message.modelPath.trim()) {
    body.model_path = message.modelPath.trim();
  }
  if (typeof message?.adapterPath === "string" && message.adapterPath.trim()) {
    body.adapter_path = message.adapterPath.trim();
  }
  if (typeof message?.adapterId === "string" && message.adapterId.trim()) {
    body.adapter_id = message.adapterId.trim();
  }
  if (Array.isArray(message?.promptSet)) {
    body.prompt_set = message.promptSet;
  }
  if (message?.generation && typeof message.generation === "object") {
    body.generation = message.generation;
  }
  if (typeof message?.systemPrompt === "string") {
    body.system_prompt = message.systemPrompt;
  }
  return await brokerRequest("POST", "/experiments/jobs", body);
}

async function getExperimentJob(message) {
  if (!message?.jobId || typeof message.jobId !== "string") {
    throw new Error("jobId is required.");
  }
  const path = `/experiments/jobs/${encodeURIComponent(message.jobId)}`;
  return await brokerRequest("GET", path);
}

async function listExperiments() {
  return await brokerRequest("GET", "/experiments");
}

async function getExperiment(message) {
  if (!message?.experimentId || typeof message.experimentId !== "string") {
    throw new Error("experimentId is required.");
  }
  const path = `/experiments/${encodeURIComponent(message.experimentId)}`;
  return await brokerRequest("GET", path);
}

async function compareExperiments(message) {
  if (!message?.experimentId || typeof message.experimentId !== "string") {
    throw new Error("experimentId is required.");
  }
  if (!message?.otherExperimentId || typeof message.otherExperimentId !== "string") {
    throw new Error("otherExperimentId is required.");
  }
  const path =
    `/experiments/${encodeURIComponent(message.experimentId)}/compare/${encodeURIComponent(message.otherExperimentId)}`;
  return await brokerRequest("GET", path);
}

async function getBrowserConfig() {
  return await brokerRequest("GET", "/browser/config");
}

async function updateBrowserConfig(message) {
  const body = {};
  if (Object.prototype.hasOwnProperty.call(message || {}, "agentMaxSteps")) {
    body.agent_max_steps = message.agentMaxSteps;
  } else if (Object.prototype.hasOwnProperty.call(message || {}, "agent_max_steps")) {
    body.agent_max_steps = message.agent_max_steps;
  }
  return await brokerRequest("POST", "/browser/config", body);
}

async function brokerRequest(method, path, body = null, options = {}) {
  const headers = {
    "X-Assistant-Client": BROKER_CLIENT_HEADER
  };
  if (body !== null) {
    headers["Content-Type"] = "application/json";
  }

  let response;
  try {
    response = await fetch(`${BROKER_URL}${path}`, {
      method,
      headers,
      body: body === null ? undefined : JSON.stringify(body),
      signal: options.signal
    });
  } catch (error) {
    if (error?.name === "AbortError") {
      throw error;
    }
    const detail = String(error?.message || error || "unknown fetch failure");
    if (!options.suppressConnectionError) {
      console.error("[secure-panel] broker request failed to reach local broker:", {
        method,
        path,
        detail
      });
    }
    const message =
      `Could not reach the local broker at ${BROKER_URL}${path}. `
      + "Make sure broker/local_broker.py is running and the extension can access localhost.";
    const wrapped = new Error(message);
    wrapped.code = "broker_unreachable";
    throw wrapped;
  }
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

function validateRunStartMessage(message) {
  validatePromptMessage(message);
  const backend = String(message.backend || "codex").trim();
  if (backend !== "llama" && backend !== "codex" && backend !== "mlx") {
    throw new Error("backend must be 'llama', 'codex', or 'mlx'.");
  }
  if (message.rewriteMessageIndex !== undefined) {
    normalizeRewriteMessageIndex(message.rewriteMessageIndex);
  }
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
  if (message.includePageContext !== undefined && typeof message.includePageContext !== "boolean") {
    throw new Error("includePageContext must be a boolean when provided.");
  }
}

function detectRiskSignals(prompt) {
  if (HIGH_RISK_PATTERN.test(prompt)) {
    return ["high_risk_prompt"];
  }
  return [];
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

function normalizeAllowedHosts(allowedHosts = null) {
  const source =
    allowedHosts === null
      ? [...DEFAULT_ALLOWED_PAGE_HOSTS, ...hostPolicy.allowedHosts]
      : allowedHosts;
  return normalizeHostList(source);
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
  return hostMatchesAllowedList(host, normalizeAllowedHosts(allowedHosts));
}

function createHostNotAllowlistedError(rawUrl, message, allowedHosts = null) {
  const error = new Error(message || "Host is not in the extension allowlist.");
  error.code = "host_not_allowlisted";
  error.data = {
    host: extractUrlHost(rawUrl),
    url: typeof rawUrl === "string" ? rawUrl : "",
    effective_allowed_hosts: normalizeAllowedHosts(allowedHosts)
  };
  return error;
}

async function initializeHostPolicy() {
  let needsMigration = false;
  try {
    const stored = await chrome.storage.local.get(HOST_POLICY_STORAGE_KEY);
    const raw = stored?.[HOST_POLICY_STORAGE_KEY];
    if (raw && typeof raw === "object") {
      hostPolicy.allowedHosts = normalizeHostList(raw.allowed_hosts ?? raw.allowedHosts ?? []);
      needsMigration =
        Object.prototype.hasOwnProperty.call(raw, "blocked_hosts")
        || Object.prototype.hasOwnProperty.call(raw, "blockedHosts");
    }
  } catch (error) {
    console.warn("[secure-panel] failed to load host policy:", String(error?.message || error));
    return;
  }
  if (needsMigration) {
    await persistHostPolicy();
  }
}

async function persistHostPolicy() {
  try {
    await chrome.storage.local.set({
      [HOST_POLICY_STORAGE_KEY]: {
        allowed_hosts: [...hostPolicy.allowedHosts]
      }
    });
  } catch (error) {
    console.warn("[secure-panel] failed to persist host policy:", String(error?.message || error));
  }
}

function getHostPolicySnapshot() {
  const defaultHosts = normalizeHostList([...DEFAULT_ALLOWED_PAGE_HOSTS]);
  const customAllowedHosts = normalizeHostList(hostPolicy.allowedHosts);
  const effectiveAllowedHosts = normalizeAllowedHosts([...defaultHosts, ...customAllowedHosts]);
  return {
    default_hosts: defaultHosts,
    custom_allowed_hosts: customAllowedHosts,
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

async function updateHostPolicyState(nextAllowedHosts) {
  hostPolicy.allowedHosts = normalizeHostList(nextAllowedHosts);
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
  return await updateHostPolicyState(nextAllowed);
}

async function removeAllowedHost(rawHost) {
  await hostPolicyReady;
  const host = parseHostForPolicy(rawHost);
  const nextAllowed = hostPolicy.allowedHosts.filter((value) => value !== host);
  return await updateHostPolicyState(nextAllowed);
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

function normalizePositiveInt(value, defaultValue, { min = 0, max = Number.MAX_SAFE_INTEGER } = {}) {
  const parsed = Number.parseInt(String(value ?? defaultValue), 10);
  if (!Number.isInteger(parsed)) {
    return defaultValue;
  }
  return Math.min(max, Math.max(min, parsed));
}

function normalizeLocator(rawLocator, options = {}) {
  const defaultVisible =
    Object.prototype.hasOwnProperty.call(options, "defaultVisible") ? options.defaultVisible : true;
  const allowVisibility =
    Object.prototype.hasOwnProperty.call(options, "allowVisibility") ? options.allowVisibility : true;
  if (!rawLocator || typeof rawLocator !== "object" || Array.isArray(rawLocator)) {
    throw new Error("Missing locator.");
  }

  const normalized = {};
  let hasLocatorField = false;
  for (const key of ["selector", "text", "label", "role", "placeholder", "name"]) {
    if (typeof rawLocator[key] === "string" && rawLocator[key].trim().length > 0) {
      normalized[key] = rawLocator[key].trim();
      hasLocatorField = true;
    }
  }

  if (!hasLocatorField) {
    throw new Error("Locator requires selector, text, label, role, placeholder, or name.");
  }

  normalized.exact = rawLocator.exact === true;
  normalized.index = normalizePositiveInt(rawLocator.index, 0, { min: 0, max: 100 });

  if (allowVisibility) {
    if (rawLocator.visible === true || rawLocator.visible === false) {
      normalized.visible = rawLocator.visible;
    } else {
      normalized.visible = defaultVisible;
    }
  } else {
    normalized.visible = null;
  }

  return normalized;
}

function normalizeWaitCondition(value) {
  const normalized = typeof value === "string" ? value.trim().toLowerCase() : "";
  if (["present", "visible", "hidden", "gone"].includes(normalized)) {
    return normalized;
  }
  return "visible";
}

function normalizeSelectOptionRequest(args) {
  const normalized = {};
  let selectedModeCount = 0;

  if (typeof args.value === "string") {
    normalized.value = args.value;
    selectedModeCount += 1;
  }
  if (typeof args.text === "string") {
    normalized.text = args.text;
    selectedModeCount += 1;
  }
  if (args.optionIndex !== undefined && args.optionIndex !== null && String(args.optionIndex).trim() !== "") {
    normalized.optionIndex = normalizePositiveInt(args.optionIndex, 0, { min: 0, max: 10_000 });
    selectedModeCount += 1;
  }

  if (selectedModeCount !== 1) {
    throw new Error("select_option requires exactly one of value, text, or optionIndex.");
  }

  return normalized;
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
    title: "",
    url: typeof tab?.url === "string" ? tab.url : "",
    content_kind: "unknown",
    selection: "",
    text_excerpt: "",
    heading_path: [],
    selection_context: null
  };

  try {
    await ensureUrlHostPermission(tab?.url);
    const tabId = parseTabId(tab?.id);
    const [injected] = await chrome.scripting.executeScript({
      target: { tabId },
      func: (textLimit, selectionLimit, contextLimit, headingDepth) => {
        const ROOT_SELECTOR = "article, main, [role='main']";
        const BLOCK_SELECTOR = [
          "article",
          "section",
          "main",
          "div",
          "p",
          "li",
          "blockquote",
          "pre",
          "figure",
          "figcaption",
          "h1",
          "h2",
          "h3",
          "h4",
          "h5",
          "h6"
        ].join(", ");
        const HEADING_SELECTOR = "h1, h2, h3, h4, h5, h6";
        const normalizeSpace = (value) => String(value || "").replace(/\s+/g, " ").trim();
        const clip = (value, limit) => normalizeSpace(value).slice(0, limit);
        const isVisible = (element) => {
          if (!(element instanceof Element)) {
            return false;
          }
          const style = window.getComputedStyle(element);
          if (!style || style.display === "none" || style.visibility === "hidden" || Number(style.opacity) === 0) {
            return false;
          }
          const rect = element.getBoundingClientRect();
          return rect.width > 0 && rect.height > 0;
        };
        const getText = (element) => clip(element?.innerText || element?.textContent || "", textLimit);
        const closestElement = (node) => {
          if (!node) {
            return null;
          }
          if (node instanceof Element) {
            return node;
          }
          return node.parentElement || null;
        };
        const pickRoot = () => {
          const candidates = Array.from(document.querySelectorAll(ROOT_SELECTOR)).filter(isVisible);
          let best = null;
          let bestLength = 0;
          for (const candidate of candidates) {
            const length = getText(candidate).length;
            if (length > bestLength) {
              best = candidate;
              bestLength = length;
            }
          }
          return best || document.body || document.documentElement || null;
        };
        const root = pickRoot();
        const selectionObject = typeof window.getSelection === "function" ? window.getSelection() : null;
        const selection = clip(selectionObject?.toString() || "", selectionLimit);
        const range = selectionObject && selectionObject.rangeCount > 0 ? selectionObject.getRangeAt(0) : null;
        const selectedElement = closestElement(range?.commonAncestorContainer) || root;
        const nearestBlock = (element) => {
          let current = element instanceof Element ? element : null;
          while (current && current !== root && !current.matches?.(BLOCK_SELECTOR)) {
            current = current.parentElement;
          }
          return current || root;
        };
        const focusBlock = nearestBlock(selectedElement);
        const focusText = clip(getText(focusBlock), contextLimit * 3);
        let selectionContext = null;
        if (selection) {
          const focusLower = focusText.toLowerCase();
          const selectionLower = selection.toLowerCase();
          const probe = selectionLower.slice(0, Math.min(selectionLower.length, 180));
          const index = probe ? focusLower.indexOf(probe) : -1;
          if (index >= 0) {
            const before = focusText.slice(Math.max(0, index - contextLimit), index).trim();
            const afterStart = Math.min(focusText.length, index + selection.length);
            const after = focusText.slice(afterStart, afterStart + contextLimit).trim();
            selectionContext = {
              before: before.slice(0, contextLimit),
              focus: selection.slice(0, contextLimit),
              after: after.slice(0, contextLimit)
            };
          } else {
            selectionContext = {
              before: "",
              focus: selection.slice(0, contextLimit),
              after: focusText.slice(0, contextLimit)
            };
          }
        }
        const headingPath = [];
        const seen = new Set();
        let probe = focusBlock instanceof Element ? focusBlock : root;
        while (probe && headingPath.length < headingDepth) {
          let current = probe;
          let found = null;
          while (current && !found) {
            if (current.matches?.(HEADING_SELECTOR) && isVisible(current)) {
              found = current;
              break;
            }
            const nested = current.querySelector?.(HEADING_SELECTOR);
            if (nested && isVisible(nested)) {
              found = nested;
              break;
            }
            current = current.previousElementSibling;
          }
          const headingText = clip(found?.innerText || found?.textContent || "", 160);
          if (headingText && !seen.has(headingText)) {
            seen.add(headingText);
            headingPath.unshift(headingText);
          }
          probe = probe?.parentElement || null;
        }
        const textExcerpt = getText(root);
        return {
          title: clip(document.title || "", 240),
          url: clip(location?.href || "", 2000),
          content_kind: textExcerpt ? "html" : "unknown",
          selection,
          text_excerpt: textExcerpt,
          heading_path: headingPath,
          selection_context: selectionContext
        };
      },
      args: [PAGE_CONTEXT_TEXT_CHARS, 1200, 500, 4]
    });
    const result = injected?.result;
    if (!result || typeof result !== "object") {
      return fallback;
    }
    return {
      title: typeof result.title === "string" ? result.title : fallback.title,
      url: typeof result.url === "string" && result.url ? result.url : fallback.url,
      content_kind: result.content_kind === "html" ? "html" : "unknown",
      selection: typeof result.selection === "string" ? result.selection : "",
      text_excerpt: typeof result.text_excerpt === "string" ? result.text_excerpt : "",
      heading_path: Array.isArray(result.heading_path) ? result.heading_path.filter((item) => typeof item === "string") : [],
      selection_context:
        result.selection_context && typeof result.selection_context === "object"
          ? {
              before: typeof result.selection_context.before === "string" ? result.selection_context.before : "",
              focus: typeof result.selection_context.focus === "string" ? result.selection_context.focus : "",
              after: typeof result.selection_context.after === "string" ? result.selection_context.after : ""
            }
          : null
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

function runLocatorTaskInPage(task) {
  const TEXT_PREVIEW_LIMIT = 160;
  const VALUE_PREVIEW_LIMIT = 120;

  function normalizeText(value) {
    return String(value || "").replace(/\s+/g, " ").trim();
  }

  function preview(value, limit) {
    const text = normalizeText(value);
    return text.length > limit ? text.slice(0, Math.max(1, limit - 3)) + "..." : text;
  }

  function matchesText(haystack, needle, exact) {
    const left = normalizeText(haystack).toLowerCase();
    const right = normalizeText(needle).toLowerCase();
    if (!right) {
      return true;
    }
    return exact ? left === right : left.includes(right);
  }

  function getRole(element) {
    const explicitRole = normalizeText(element.getAttribute?.("role"));
    if (explicitRole) {
      return explicitRole.toLowerCase();
    }
    const tag = String(element.tagName || "").toLowerCase();
    if (tag === "a" && element.hasAttribute("href")) {
      return "link";
    }
    if (tag === "button") {
      return "button";
    }
    if (tag === "textarea") {
      return "textbox";
    }
    if (tag === "select") {
      return "combobox";
    }
    if (tag === "option") {
      return "option";
    }
    if (tag === "input") {
      const type = String(element.getAttribute("type") || "text").toLowerCase();
      if (["button", "submit", "reset"].includes(type)) {
        return "button";
      }
      if (type === "checkbox") {
        return "checkbox";
      }
      if (type === "radio") {
        return "radio";
      }
      return "textbox";
    }
    return "";
  }

  function getElementText(element) {
    return normalizeText(element.innerText || element.textContent || "");
  }

  function getLabelText(element) {
    const values = [];
    const ariaLabel = normalizeText(element.getAttribute?.("aria-label"));
    if (ariaLabel) {
      values.push(ariaLabel);
    }

    const labelledBy = normalizeText(element.getAttribute?.("aria-labelledby"));
    if (labelledBy) {
      for (const id of labelledBy.split(/\s+/)) {
        const labelNode = id ? document.getElementById(id) : null;
        const text = normalizeText(labelNode?.innerText || labelNode?.textContent || "");
        if (text) {
          values.push(text);
        }
      }
    }

    if (element.labels && element.labels.length > 0) {
      for (const label of Array.from(element.labels)) {
        const text = normalizeText(label?.innerText || label?.textContent || "");
        if (text) {
          values.push(text);
        }
      }
    }

    return normalizeText(values.join(" "));
  }

  function isVisible(element) {
    const style = window.getComputedStyle(element);
    if (!style || style.display === "none" || style.visibility === "hidden" || Number(style.opacity) === 0) {
      return false;
    }
    const rect = element.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  }

  function isEditable(element) {
    return (
      element instanceof HTMLInputElement ||
      element instanceof HTMLTextAreaElement ||
      element instanceof HTMLSelectElement ||
      element.isContentEditable === true
    );
  }

  function isEnabled(element) {
    if ("disabled" in element) {
      return element.disabled !== true;
    }
    return normalizeText(element.getAttribute?.("aria-disabled")).toLowerCase() !== "true";
  }

  function getValuePreview(element) {
    if (element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement) {
      return preview(element.value, VALUE_PREVIEW_LIMIT);
    }
    if (element instanceof HTMLSelectElement) {
      const selected = element.selectedOptions?.[0] || null;
      return preview(selected?.text || selected?.value || "", VALUE_PREVIEW_LIMIT);
    }
    if (element.isContentEditable) {
      return preview(element.textContent || "", VALUE_PREVIEW_LIMIT);
    }
    return "";
  }

  function buildElementSnapshot(element) {
    const rect = element.getBoundingClientRect();
    const label = getLabelText(element);
    const selectedOption = element instanceof HTMLSelectElement ? element.selectedOptions?.[0] || null : null;
    return {
      tagName: String(element.tagName || "").toLowerCase(),
      role: getRole(element) || null,
      textPreview: preview(getElementText(element), TEXT_PREVIEW_LIMIT),
      label: label || null,
      name: normalizeText(element.getAttribute?.("name")) || null,
      placeholder: normalizeText(element.getAttribute?.("placeholder")) || null,
      valuePreview: getValuePreview(element) || null,
      visible: isVisible(element),
      enabled: isEnabled(element),
      editable: isEditable(element),
      checked: "checked" in element ? element.checked === true : null,
      selected: element instanceof HTMLOptionElement ? element.selected === true : null,
      selectedValue: element instanceof HTMLSelectElement ? String(element.value || "") : null,
      selectedText: selectedOption ? preview(selectedOption.text || "", TEXT_PREVIEW_LIMIT) : null,
      optionCount: element instanceof HTMLSelectElement ? element.options.length : null,
      href: typeof element.href === "string" && element.href ? element.href : null,
      src: typeof element.src === "string" && element.src ? element.src : null,
      x: Math.round(rect.left + rect.width / 2),
      y: Math.round(rect.top + rect.height / 2),
      width: Math.round(rect.width),
      height: Math.round(rect.height)
    };
  }

  function collectCandidates(locator) {
    if (locator.selector) {
      return Array.from(document.querySelectorAll(locator.selector));
    }
    return Array.from(document.querySelectorAll("*"));
  }

  function matchesNonVisibilityFilters(element, locator) {
    if (locator.text && !matchesText(getElementText(element), locator.text, locator.exact)) {
      return false;
    }
    if (locator.label && !matchesText(getLabelText(element), locator.label, locator.exact)) {
      return false;
    }
    if (locator.role && !matchesText(getRole(element), locator.role, locator.exact)) {
      return false;
    }
    if (locator.placeholder && !matchesText(element.getAttribute?.("placeholder"), locator.placeholder, locator.exact)) {
      return false;
    }
    if (locator.name && !matchesText(element.getAttribute?.("name"), locator.name, locator.exact)) {
      return false;
    }
    return true;
  }

  function queryElements(locator) {
    const semanticMatches = collectCandidates(locator).filter((element) =>
      matchesNonVisibilityFilters(element, locator)
    );
    const visibleMatches = semanticMatches.filter((element) => isVisible(element));
    let matches = semanticMatches;
    if (locator.visible === true) {
      matches = visibleMatches;
    } else if (locator.visible === false) {
      matches = semanticMatches.filter((element) => !isVisible(element));
    }
    return {
      semanticMatches,
      visibleMatches,
      matches
    };
  }

  const locator = task?.locator || {};
  const query = queryElements(locator);
  const semanticMatches = query.semanticMatches;
  const visibleMatches = query.visibleMatches;
  const matches = query.matches;
  const selectedElement =
    Number.isInteger(locator.index) && locator.index >= 0 ? matches[locator.index] || null : matches[0] || null;

  if (task?.kind === "probe") {
    return {
      totalCount: semanticMatches.length,
      visibleCount: visibleMatches.length,
      firstMatch: semanticMatches[0] ? buildElementSnapshot(semanticMatches[0]) : null,
      firstVisible: visibleMatches[0] ? buildElementSnapshot(visibleMatches[0]) : null
    };
  }

  if (task?.kind === "find_one") {
    return {
      found: Boolean(selectedElement),
      matchCount: matches.length,
      visibleCount: visibleMatches.length,
      element: selectedElement ? buildElementSnapshot(selectedElement) : null
    };
  }

  if (task?.kind === "find_elements") {
    const limit = Number.isInteger(task.limit) ? Math.max(1, Math.min(task.limit, 20)) : 10;
    return {
      matchCount: matches.length,
      visibleCount: visibleMatches.length,
      elements: matches.slice(0, limit).map((element) => buildElementSnapshot(element))
    };
  }

  if (task?.kind === "get_state") {
    return {
      found: Boolean(selectedElement),
      matchCount: matches.length,
      visibleCount: visibleMatches.length,
      element: selectedElement ? buildElementSnapshot(selectedElement) : null
    };
  }

  if (task?.kind === "select_option") {
    if (!selectedElement) {
      return { ok: false, error: "locator_not_found", matchCount: matches.length };
    }
    if (!(selectedElement instanceof HTMLSelectElement)) {
      return { ok: false, error: "not_select", matchCount: matches.length };
    }

    let option = null;
    if (Number.isInteger(task.optionIndex)) {
      option = selectedElement.options[task.optionIndex] || null;
    } else if (typeof task.value === "string") {
      option = Array.from(selectedElement.options).find((item) => item.value === task.value) || null;
    } else if (typeof task.text === "string") {
      option =
        Array.from(selectedElement.options).find(
          (item) => normalizeText(item.text).toLowerCase() === normalizeText(task.text).toLowerCase()
        ) || null;
    }

    if (!option) {
      return { ok: false, error: "option_not_found", matchCount: matches.length };
    }

    selectedElement.value = option.value;
    option.selected = true;
    selectedElement.dispatchEvent(new Event("input", { bubbles: true }));
    selectedElement.dispatchEvent(new Event("change", { bubbles: true }));

    return {
      ok: true,
      matchCount: matches.length,
      selectedValue: option.value,
      selectedText: preview(option.text || "", TEXT_PREVIEW_LIMIT),
      selectedIndex: option.index,
      element: buildElementSnapshot(selectedElement)
    };
  }

  throw new Error(`Unsupported locator task: ${String(task?.kind || "")}`);
}

async function startBrokerCommandLoop() {
  if (relayLoopStarted) {
    return;
  }
  relayLoopStarted = true;
  const relayRequestOptions = { suppressConnectionError: true };

  while (true) {
    try {
      await brokerRequest("POST", "/extension/register", {
        client_id: relayClientId,
        version: RELAY_COMMAND_LOOP_VERSION,
        platform: "chrome-sidepanel"
      }, relayRequestOptions);

      const next = await brokerRequest(
        "GET",
        `/extension/next?client_id=${encodeURIComponent(relayClientId)}&timeout_ms=${RELAY_POLL_TIMEOUT_MS}`,
        null,
        relayRequestOptions
      );

      if (next?.command) {
        await executeAndReportBrokerCommand(next.command);
      }

      relayBackoffMs = RELAY_INITIAL_BACKOFF_MS;
    } catch (error) {
      if (error?.code !== "broker_unreachable") {
        console.warn("[secure-panel] broker command loop error:", String(error?.message || error));
      }
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
  }, {
    suppressConnectionError: true
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
    case "highlight":
      return await commandHighlight(args, allowedHosts);
    case "get_content":
      return await commandGetContent(args, allowedHosts);
    case "find_one":
      return await commandFindOne(args, allowedHosts);
    case "find_elements":
      return await commandFindElements(args, allowedHosts);
    case "wait_for":
      return await commandWaitFor(args, allowedHosts);
    case "get_element_state":
      return await commandGetElementState(args, allowedHosts);
    case "select_option":
      return await commandSelectOption(args, allowedHosts);
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

function runGetContentTaskInPage(task) {
  const RAW_MAX_CHARS_DEFAULT = 6_000;
  const RAW_MAX_CHARS_LIMIT = 50_000;
  const NAV_MAX_CHARS_DEFAULT = 1_200;
  const NAV_MAX_CHARS_LIMIT = 6_000;
  const NAV_MAX_ITEMS_DEFAULT = 10;
  const NAV_MAX_ITEMS_LIMIT = 20;
  const FIELD_PREVIEW_LIMIT = 120;
  const TEXT_PREVIEW_LIMIT = 160;
  const HEADING_PREVIEW_LIMIT = 120;

  function normalizeText(value) {
    return String(value || "").replace(/\s+/g, " ").trim();
  }

  function clipText(value, limit) {
    const text = normalizeText(value);
    if (!limit || text.length <= limit) {
      return text;
    }
    if (limit <= 3) {
      return text.slice(0, limit);
    }
    return `${text.slice(0, Math.max(1, limit - 3))}...`;
  }

  function toPositiveInt(value, fallback, min, max) {
    const parsed = Number.parseInt(String(value ?? fallback), 10);
    if (!Number.isInteger(parsed)) {
      return fallback;
    }
    return Math.min(max, Math.max(min, parsed));
  }

  function escapeSelectorValue(value) {
    if (typeof CSS !== "undefined" && typeof CSS.escape === "function") {
      return CSS.escape(String(value));
    }
    return String(value).replace(/["\\]/g, "\\$&");
  }

  function selectorCount(selector) {
    try {
      return document.querySelectorAll(selector).length;
    } catch {
      return 0;
    }
  }

  function uniqueAttributeSelector(tagName, attr, rawValue) {
    const value = normalizeText(rawValue);
    if (!value) {
      return "";
    }
    const selector = `${tagName}[${attr}="${escapeSelectorValue(value)}"]`;
    return selectorCount(selector) === 1 ? selector : "";
  }

  function getRole(element) {
    const explicitRole = normalizeText(element.getAttribute?.("role"));
    if (explicitRole) {
      return explicitRole.toLowerCase();
    }
    const tag = String(element.tagName || "").toLowerCase();
    if (tag === "a" && element.hasAttribute("href")) {
      return "link";
    }
    if (tag === "button") {
      return "button";
    }
    if (tag === "textarea") {
      return "textbox";
    }
    if (tag === "select") {
      return "combobox";
    }
    if (tag === "option") {
      return "option";
    }
    if (tag === "input") {
      const type = String(element.getAttribute("type") || "text").toLowerCase();
      if (["button", "submit", "reset"].includes(type)) {
        return "button";
      }
      if (type === "checkbox") {
        return "checkbox";
      }
      if (type === "radio") {
        return "radio";
      }
      return "textbox";
    }
    return "";
  }

  function isVisible(element) {
    const style = window.getComputedStyle(element);
    if (!style || style.display === "none" || style.visibility === "hidden" || Number(style.opacity) === 0) {
      return false;
    }
    const rect = element.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  }

  function isEditable(element) {
    return (
      element instanceof HTMLInputElement ||
      element instanceof HTMLTextAreaElement ||
      element instanceof HTMLSelectElement ||
      element.isContentEditable === true
    );
  }

  function isEnabled(element) {
    if ("disabled" in element) {
      return element.disabled !== true;
    }
    return normalizeText(element.getAttribute?.("aria-disabled")).toLowerCase() !== "true";
  }

  function getElementText(element) {
    return normalizeText(element.innerText || element.textContent || "");
  }

  function getLabelText(element) {
    const values = [];
    const ariaLabel = normalizeText(element.getAttribute?.("aria-label"));
    if (ariaLabel) {
      values.push(ariaLabel);
    }

    const labelledBy = normalizeText(element.getAttribute?.("aria-labelledby"));
    if (labelledBy) {
      for (const id of labelledBy.split(/\s+/)) {
        const labelNode = id ? document.getElementById(id) : null;
        const text = normalizeText(labelNode?.innerText || labelNode?.textContent || "");
        if (text) {
          values.push(text);
        }
      }
    }

    if (element.labels && element.labels.length > 0) {
      for (const label of Array.from(element.labels)) {
        const text = normalizeText(label?.innerText || label?.textContent || "");
        if (text) {
          values.push(text);
        }
      }
    }

    return normalizeText(values.join(" "));
  }

  function getValuePreview(element) {
    if (element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement) {
      return clipText(element.value, FIELD_PREVIEW_LIMIT);
    }
    if (element instanceof HTMLSelectElement) {
      const selected = element.selectedOptions?.[0] || null;
      return clipText(selected?.text || selected?.value || "", FIELD_PREVIEW_LIMIT);
    }
    if (element.isContentEditable) {
      return clipText(element.textContent || "", FIELD_PREVIEW_LIMIT);
    }
    return "";
  }

  function buildSelectorHint(element) {
    if (!(element instanceof Element)) {
      return "";
    }

    if (element.id) {
      const selector = `#${escapeSelectorValue(element.id)}`;
      if (selectorCount(selector) === 1) {
        return selector;
      }
    }

    const tagName = String(element.tagName || "").toLowerCase() || "*";
    const attributes = [
      ["data-testid", element.getAttribute?.("data-testid")],
      ["data-test", element.getAttribute?.("data-test")],
      ["name", element.getAttribute?.("name")],
      ["aria-label", element.getAttribute?.("aria-label")],
      ["placeholder", element.getAttribute?.("placeholder")],
      ["href", element.getAttribute?.("href")]
    ];

    for (const [attr, value] of attributes) {
      const selector = uniqueAttributeSelector(tagName, attr, value);
      if (selector) {
        return selector;
      }
    }

    const path = [];
    let current = element;
    while (current && current.nodeType === Node.ELEMENT_NODE && current !== document.body) {
      const currentTag = String(current.tagName || "").toLowerCase();
      if (!currentTag) {
        break;
      }

      if (current.id) {
        path.unshift(`#${escapeSelectorValue(current.id)}`);
        const selector = path.join(" > ");
        if (selectorCount(selector) === 1) {
          return selector;
        }
        break;
      }

      let segment = currentTag;
      const parent = current.parentElement;
      if (parent) {
        const siblings = Array.from(parent.children).filter(
          (child) => String(child.tagName || "").toLowerCase() === currentTag
        );
        if (siblings.length > 1) {
          const index = siblings.indexOf(current) + 1;
          segment += `:nth-of-type(${index})`;
        }
      }
      path.unshift(segment);
      const selector = path.join(" > ");
      if (selectorCount(selector) === 1) {
        return selector;
      }
      current = parent;
    }

    return path.join(" > ");
  }

  function interactiveSortKey(element) {
    const rect = element.getBoundingClientRect();
    return [Math.round(rect.top), Math.round(rect.left)];
  }

  function buildInteractiveItem(element) {
    const tagName = String(element.tagName || "").toLowerCase();
    const hrefRaw = typeof element.href === "string" ? element.href : "";
    const href = /^https?:/i.test(hrefRaw) ? hrefRaw : null;
    const type =
      element instanceof HTMLInputElement || element instanceof HTMLButtonElement
        ? normalizeText(element.getAttribute?.("type")) || null
        : null;

    return {
      selector: buildSelectorHint(element) || null,
      tagName,
      type,
      role: getRole(element) || null,
      textPreview: clipText(getElementText(element), TEXT_PREVIEW_LIMIT) || null,
      label: clipText(getLabelText(element), FIELD_PREVIEW_LIMIT) || null,
      name: clipText(element.getAttribute?.("name"), FIELD_PREVIEW_LIMIT) || null,
      placeholder: clipText(element.getAttribute?.("placeholder"), FIELD_PREVIEW_LIMIT) || null,
      valuePreview: getValuePreview(element) || null,
      href,
      enabled: isEnabled(element),
      editable: isEditable(element)
    };
  }

  function dedupeBySignature(items) {
    const deduped = [];
    const seen = new Set();
    for (const item of items) {
      const signature = JSON.stringify([
        item.selector,
        item.tagName,
        item.role,
        item.textPreview,
        item.text,
        item.label,
        item.name,
        item.placeholder
      ]);
      if (seen.has(signature)) {
        continue;
      }
      seen.add(signature);
      deduped.push(item);
    }
    return deduped;
  }

  function buildFormSummary(form, maxItems) {
    const fields = Array.from(form.querySelectorAll("input, textarea, select"))
      .filter((element) => isVisible(element))
      .slice(0, Math.min(maxItems, 6))
      .map((element) => ({
        selector: buildSelectorHint(element) || null,
        tagName: String(element.tagName || "").toLowerCase(),
        type: element instanceof HTMLInputElement ? normalizeText(element.type) || null : null,
        role: getRole(element) || null,
        label: clipText(getLabelText(element), FIELD_PREVIEW_LIMIT) || null,
        name: clipText(element.getAttribute?.("name"), FIELD_PREVIEW_LIMIT) || null,
        placeholder: clipText(element.getAttribute?.("placeholder"), FIELD_PREVIEW_LIMIT) || null,
        valuePreview: getValuePreview(element) || null,
        required: typeof element.required === "boolean" ? element.required : false
      }));

    return {
      selector: buildSelectorHint(form) || null,
      name: clipText(form.getAttribute?.("name"), FIELD_PREVIEW_LIMIT) || null,
      method: normalizeText(form.getAttribute?.("method") || "get").toLowerCase() || "get",
      action: clipText(form.getAttribute?.("action"), FIELD_PREVIEW_LIMIT) || null,
      fieldCount: Array.from(form.querySelectorAll("input, textarea, select")).length,
      fields
    };
  }

  const selector = typeof task?.selector === "string" && task.selector.trim().length > 0 ? task.selector.trim() : null;
  const mode = task?.mode === "raw_html" ? "raw_html" : "navigation";
  const rawMaxChars = toPositiveInt(task?.maxChars, RAW_MAX_CHARS_DEFAULT, 1, RAW_MAX_CHARS_LIMIT);
  const navMaxChars = toPositiveInt(task?.maxChars, NAV_MAX_CHARS_DEFAULT, 250, NAV_MAX_CHARS_LIMIT);
  const maxItems = toPositiveInt(task?.maxItems, NAV_MAX_ITEMS_DEFAULT, 1, NAV_MAX_ITEMS_LIMIT);
  const node = selector ? document.querySelector(selector) : document.documentElement;
  const title = clipText(document.title, 200);
  const finalUrl = String(window.location.href || "");

  if (!node) {
    if (mode === "raw_html") {
      return {
        mode,
        html: "",
        fullLength: 0,
        truncated: false,
        finalUrl,
        title,
        selector,
        selectorMatched: false
      };
    }
    return {
      mode,
      title,
      finalUrl,
      selector,
      selectorMatched: false,
      textPreview: "",
      headings: [],
      interactive: [],
      forms: [],
      counts: {
        headingCount: 0,
        interactiveCount: 0,
        formCount: 0
      },
      truncated: {
        textPreview: false,
        headings: false,
        interactive: false,
        forms: false
      }
    };
  }

  if (mode === "raw_html") {
    const html = typeof node.outerHTML === "string" ? node.outerHTML : String(node.textContent ?? "");
    return {
      mode,
      html: html.slice(0, rawMaxChars),
      fullLength: html.length,
      truncated: html.length > rawMaxChars,
      finalUrl,
      title,
      selector,
      selectorMatched: selector ? true : null
    };
  }

  const root = node;
  const rootText =
    typeof root.innerText === "string"
      ? root.innerText
      : typeof root.textContent === "string"
        ? root.textContent
        : "";
  const normalizedText = normalizeText(rootText);
  const textPreview = clipText(normalizedText, navMaxChars);

  const headingNodes = [];
  if (root instanceof Element && /^h[1-6]$/i.test(root.tagName || "")) {
    headingNodes.push(root);
  }
  headingNodes.push(...Array.from(root.querySelectorAll?.("h1, h2, h3, h4, h5, h6") || []));
  const visibleHeadings = dedupeBySignature(
    headingNodes
      .filter((element) => element instanceof Element && isVisible(element))
      .map((element) => ({
        selector: buildSelectorHint(element) || null,
        level: Number.parseInt(String(element.tagName || "").replace(/^h/i, ""), 10) || null,
        text: clipText(getElementText(element), HEADING_PREVIEW_LIMIT) || null
      }))
      .filter((item) => item.text)
  );
  const headings = visibleHeadings.slice(0, Math.min(maxItems, 8));

  const interactiveSelector = [
    "a[href]",
    "button",
    "input",
    "select",
    "textarea",
    "[role='button']",
    "[role='link']",
    "[role='textbox']",
    "[role='combobox']",
    "[contenteditable='true']",
    "[contenteditable='']"
  ].join(", ");
  const interactiveNodes = [];
  if (root instanceof Element && root.matches?.(interactiveSelector)) {
    interactiveNodes.push(root);
  }
  interactiveNodes.push(...Array.from(root.querySelectorAll?.(interactiveSelector) || []));
  interactiveNodes.sort((left, right) => {
    const [leftTop, leftLeft] = interactiveSortKey(left);
    const [rightTop, rightLeft] = interactiveSortKey(right);
    if (leftTop !== rightTop) {
      return leftTop - rightTop;
    }
    return leftLeft - rightLeft;
  });
  const interactiveItems = dedupeBySignature(
    interactiveNodes
      .filter((element) => element instanceof Element && isVisible(element))
      .map((element) => buildInteractiveItem(element))
      .filter((item) => item.selector || item.textPreview || item.label || item.name || item.placeholder)
  );
  const interactive = interactiveItems.slice(0, maxItems);

  const formNodes = [];
  if (root instanceof HTMLFormElement) {
    formNodes.push(root);
  }
  formNodes.push(...Array.from(root.querySelectorAll?.("form") || []));
  const visibleForms = formNodes.filter((form) => form instanceof HTMLFormElement && isVisible(form));
  const forms = visibleForms.slice(0, Math.min(maxItems, 4)).map((form) => buildFormSummary(form, maxItems));

  return {
    mode,
    title,
    finalUrl,
    selector,
    selectorMatched: selector ? true : null,
    textPreview,
    headings,
    interactive,
    forms,
    counts: {
      headingCount: visibleHeadings.length,
      interactiveCount: interactiveItems.length,
      formCount: visibleForms.length
    },
    truncated: {
      textPreview: normalizedText.length > textPreview.length,
      headings: visibleHeadings.length > headings.length,
      interactive: interactiveItems.length > interactive.length,
      forms: visibleForms.length > forms.length
    }
  };
}


function runHighlightTaskInPage(task) {
  const CLEANUP_KEY = "__assistReadAssistantHighlightCleanup";
  const BLOCK_SELECTOR = "article, main, section, p, li, blockquote, pre, div, h1, h2, h3, h4, h5, h6";
  const normalizeText = (value) => String(value || "").replace(/\s+/g, " ").trim();
  const preview = (value, limit = 200) => normalizeText(value).slice(0, limit);
  const isVisible = (element) => {
    if (!(element instanceof Element)) {
      return false;
    }
    const style = window.getComputedStyle(element);
    if (!style || style.display === "none" || style.visibility === "hidden" || Number(style.opacity) === 0) {
      return false;
    }
    const rect = element.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  };
  const getText = (element) => preview(element?.innerText || element?.textContent || "", 5000);
  const getRole = (element) => normalizeText(element.getAttribute?.("role")).toLowerCase();
  const getLabel = (element) => normalizeText([
    element.getAttribute?.("aria-label"),
    element.getAttribute?.("name"),
    element.getAttribute?.("placeholder")
  ].join(" "));
  const clearExisting = () => {
    const cleanup = window[CLEANUP_KEY];
    if (typeof cleanup === "function") {
      cleanup();
    }
  };
  const selectByText = (query) => {
    const needle = normalizeText(query).toLowerCase();
    if (!needle) {
      return null;
    }
    const root = document.querySelector("article, main, [role='main']") || document.body || document.documentElement;
    const candidates = Array.from(root.querySelectorAll(BLOCK_SELECTOR)).filter(isVisible);
    return candidates.find((element) => getText(element).toLowerCase().includes(needle)) || null;
  };
  const selectByLocator = (locator) => {
    if (!locator || typeof locator !== "object") {
      return null;
    }
    if (typeof locator.selector === "string" && locator.selector.trim()) {
      try {
        const selectorMatch = Array.from(document.querySelectorAll(locator.selector.trim())).find((element) => {
          if (!isVisible(element)) {
            return false;
          }
          if (locator.text && !getText(element).toLowerCase().includes(normalizeText(locator.text).toLowerCase())) {
            return false;
          }
          return true;
        });
        if (selectorMatch) {
          return selectorMatch;
        }
      } catch {
        // Ignore invalid selectors from model input.
      }
    }
    const all = Array.from(document.querySelectorAll("*")).filter(isVisible);
    return all.find((element) => {
      if (locator.text && !getText(element).toLowerCase().includes(normalizeText(locator.text).toLowerCase())) {
        return false;
      }
      if (locator.label && !getLabel(element).toLowerCase().includes(normalizeText(locator.label).toLowerCase())) {
        return false;
      }
      if (locator.role && getRole(element) !== normalizeText(locator.role).toLowerCase()) {
        return false;
      }
      if (locator.name && !normalizeText(element.getAttribute?.("name")).toLowerCase().includes(normalizeText(locator.name).toLowerCase())) {
        return false;
      }
      if (locator.placeholder && !normalizeText(element.getAttribute?.("placeholder")).toLowerCase().includes(normalizeText(locator.placeholder).toLowerCase())) {
        return false;
      }
      return Boolean(locator.text || locator.label || locator.role || locator.name || locator.placeholder);
    }) || null;
  };

  const durationMs = Number.isInteger(task?.durationMs) ? Math.max(500, Math.min(task.durationMs, 20000)) : 6000;
  const strategy = task?.locator ? "locator" : "text";
  const target = selectByLocator(task?.locator) || selectByText(task?.text || "");
  clearExisting();
  if (!target) {
    return {
      ok: true,
      highlighted: false,
      strategy,
      preview_text: "",
      duration_ms: durationMs
    };
  }
  if (task?.scroll !== false) {
    target.scrollIntoView({ behavior: "instant", block: "center", inline: "nearest" });
  }
  const previous = {
    outline: target.style.outline,
    outlineOffset: target.style.outlineOffset,
    boxShadow: target.style.boxShadow,
    transition: target.style.transition
  };
  target.setAttribute("data-assist-read-highlight", "true");
  target.style.outline = "3px solid #ffb300";
  target.style.outlineOffset = "4px";
  target.style.boxShadow = "0 0 0 8px rgba(255, 179, 0, 0.22)";
  target.style.transition = "outline-color 120ms ease, box-shadow 120ms ease";
  window[CLEANUP_KEY] = () => {
    target.style.outline = previous.outline || "";
    target.style.outlineOffset = previous.outlineOffset || "";
    target.style.boxShadow = previous.boxShadow || "";
    target.style.transition = previous.transition || "";
    target.removeAttribute("data-assist-read-highlight");
    window[CLEANUP_KEY] = null;
  };
  window.setTimeout(() => {
    if (typeof window[CLEANUP_KEY] === "function") {
      window[CLEANUP_KEY]();
    }
  }, durationMs);
  return {
    ok: true,
    highlighted: true,
    strategy,
    preview_text: preview(getText(target), 200),
    duration_ms: durationMs
  };
}

async function commandHighlight(args, allowedHosts) {
  const tabId = await resolveTabId(args.tabId);
  await getAllowedTab(tabId, allowedHosts);
  const locator = args.locator && typeof args.locator === "object"
    ? normalizeLocator(args.locator, { defaultVisible: true })
    : null;
  const result = await runInTab(tabId, runHighlightTaskInPage, [{
    locator,
    text: typeof args.text === "string" ? args.text : "",
    scroll: args.scroll !== false,
    durationMs: Number.isInteger(args.durationMs) ? args.durationMs : 6000
  }]);
  return {
    ok: Boolean(result?.ok),
    tabId,
    highlighted: Boolean(result?.highlighted),
    strategy: result?.strategy || (locator ? "locator" : "text"),
    preview_text: typeof result?.preview_text === "string" ? result.preview_text : "",
    duration_ms: Number.isInteger(result?.duration_ms) ? result.duration_ms : 6000
  };
}

async function commandGetContent(args, allowedHosts) {
  const tabId = await resolveTabId(args.tabId);
  await getAllowedTab(tabId, allowedHosts);
  const selector = typeof args.selector === "string" && args.selector.trim().length > 0 ? args.selector.trim() : null;
  const mode = typeof args.mode === "string" ? args.mode.trim().toLowerCase() : "navigation";
  const requestedMaxChars = Number.parseInt(String(args.maxChars ?? (mode === "raw_html" ? 6000 : 1200)), 10);
  const requestedMaxItems = Number.parseInt(String(args.maxItems ?? 10), 10);

  return await runInTab(tabId, runGetContentTaskInPage, [
    {
      selector,
      mode,
      maxChars: requestedMaxChars,
      maxItems: requestedMaxItems
    }
  ]);
}

async function commandFindOne(args, allowedHosts) {
  const tabId = await resolveTabId(args.tabId);
  await getAllowedTab(tabId, allowedHosts);
  const locator = normalizeLocator(args.locator, { defaultVisible: true });
  return await runInTab(tabId, runLocatorTaskInPage, [{ kind: "find_one", locator }]);
}

async function commandFindElements(args, allowedHosts) {
  const tabId = await resolveTabId(args.tabId);
  await getAllowedTab(tabId, allowedHosts);
  const locator = normalizeLocator(args.locator, { defaultVisible: true });
  const limit = normalizePositiveInt(args.limit, 10, { min: 1, max: 20 });
  return await runInTab(tabId, runLocatorTaskInPage, [{ kind: "find_elements", locator, limit }]);
}

async function commandWaitFor(args, allowedHosts) {
  const tabId = await resolveTabId(args.tabId);
  await getAllowedTab(tabId, allowedHosts);
  const locator = normalizeLocator(args.locator, { defaultVisible: null, allowVisibility: false });
  const condition = normalizeWaitCondition(args.condition);
  const timeoutMs = normalizePositiveInt(args.timeoutMs, 10_000, { min: 100, max: 60_000 });
  const pollMs = normalizePositiveInt(args.pollMs, 250, { min: 50, max: 5_000 });
  const startedAt = Date.now();
  let lastProbe = null;

  while (Date.now() - startedAt <= timeoutMs) {
    lastProbe = await runInTab(tabId, runLocatorTaskInPage, [{ kind: "probe", locator }]);
    const totalCount = Number(lastProbe?.totalCount ?? 0);
    const visibleCount = Number(lastProbe?.visibleCount ?? 0);
    const satisfied =
      (condition === "present" && totalCount > 0) ||
      (condition === "visible" && visibleCount > 0) ||
      (condition === "hidden" && totalCount > 0 && visibleCount === 0) ||
      (condition === "gone" && totalCount === 0);

    if (satisfied) {
      const element =
        condition === "visible"
          ? lastProbe?.firstVisible || lastProbe?.firstMatch || null
          : condition === "gone"
            ? null
            : lastProbe?.firstMatch || lastProbe?.firstVisible || null;
      return {
        condition,
        satisfied: true,
        elapsedMs: Date.now() - startedAt,
        matchCount: totalCount,
        visibleCount,
        element
      };
    }

    await delay(pollMs);
  }

  throw new Error(`wait_for timed out: ${condition}`);
}

async function commandGetElementState(args, allowedHosts) {
  const tabId = await resolveTabId(args.tabId);
  await getAllowedTab(tabId, allowedHosts);
  const locator = normalizeLocator(args.locator, { defaultVisible: true });
  return await runInTab(tabId, runLocatorTaskInPage, [{ kind: "get_state", locator }]);
}

async function commandSelectOption(args, allowedHosts) {
  const tabId = await resolveTabId(args.tabId);
  await getAllowedTab(tabId, allowedHosts);
  const locator = normalizeLocator(args.locator, { defaultVisible: true });
  const selection = normalizeSelectOptionRequest(args);
  const result = await runInTab(tabId, runLocatorTaskInPage, [
    {
      kind: "select_option",
      locator,
      ...selection
    }
  ]);

  if (!result?.ok) {
    throw new Error(`select_option failed: ${result?.error || "unknown_error"}`);
  }

  return {
    matchCount: Number(result.matchCount ?? 1),
    selectedValue: result.selectedValue ?? null,
    selectedText: result.selectedText ?? null,
    selectedIndex: Number.isInteger(result.selectedIndex) ? result.selectedIndex : null,
    element: result.element ?? null
  };
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
