const $ = (id) => document.getElementById(id);
const SAFE_LINK_PROTOCOLS = new Set(["http:", "https:"]);

const state = {
  sessionId: crypto.randomUUID(),
  pendingConfirmation: false,
  pendingRequest: null,
  conversationList: [],
  historyOpen: false,
  historyPinned: false,
  brokerHealth: null,
  busy: false,
  stopping: false,
  activeCodexRunId: "",
  activeLegacyRequestId: "",
  activeLegacyPendingNode: null,
  stoppedLegacyRequests: new Set(),
  actionConfirmResolver: null,
  codexRunUi: new Map(),
  codexPollingRuns: new Set(),
  rewriteTargetIndex: null,
  activeMainTab: "chat",
  modelsBusy: false,
  toolsBusy: false,
  toolsPolicy: null,
  toolsActiveTab: null
};

const appEl = document.querySelector(".app");
const brokerStatusEl = $("broker-status");
const contextUsageEl = $("context-usage");
const messagesEl = $("messages");
const emptyStateEl = $("empty-state");
const historyToggleBtn = $("history-toggle-btn");
const historyCloseBtn = $("history-close-btn");
const historyPinBtn = $("history-pin-btn");
const historyBackdropEl = $("history-backdrop");
const historyPanelEl = $("history-panel");
const historyListEl = $("history-list");
const promptEl = $("prompt");
const backendEl = $("backend");
const includePageContextEl = $("include-page-context");
const forceBrowserActionEl = $("force-browser-action");
const stopBtn = $("stop-btn");
const sendBtn = $("send-btn");
const newSessionBtn = $("new-session-btn");
const deleteChatBtn = $("delete-chat-btn");
const refreshHistoryBtn = $("refresh-history-btn");
const confirmWrap = $("risk-confirm");
const riskText = $("risk-text");
const confirmBtn = $("confirm-btn");
const cancelBtn = $("cancel-btn");
const actionConfirmEl = $("action-confirm");
const actionConfirmTitleEl = $("action-confirm-title");
const actionConfirmTextEl = $("action-confirm-text");
const actionConfirmBtn = $("action-confirm-btn");
const actionCancelBtn = $("action-cancel-btn");
const chatTabBtn = $("chat-tab-btn");
const modelsTabBtn = $("models-tab-btn");
const toolsTabBtn = $("tools-tab-btn");
const chatViewEl = $("chat-view");
const modelsViewEl = $("models-view");
const toolsViewEl = $("tools-view");
const mlxRefreshBtn = $("mlx-refresh-btn");
const modelsBackendEl = $("models-backend");
const mlxModelPathEl = $("mlx-model-path");
const mlxRuntimeStatusEl = $("mlx-runtime-status");
const mlxStartBtn = $("mlx-start-btn");
const mlxStopBtn = $("mlx-stop-btn");
const mlxRestartBtn = $("mlx-restart-btn");
const mlxTemperatureEl = $("mlx-temperature");
const mlxTopPEl = $("mlx-top-p");
const mlxTopKEl = $("mlx-top-k");
const mlxMaxTokensEl = $("mlx-max-tokens");
const mlxRepetitionPenaltyEl = $("mlx-repetition-penalty");
const mlxSeedEl = $("mlx-seed");
const mlxEnableThinkingEl = $("mlx-enable-thinking");
const mlxSystemPromptEl = $("mlx-system-prompt");
const mlxApplyBtn = $("mlx-apply-btn");
const mlxAdapterListEl = $("mlx-adapter-list");
const mlxAdapterPathEl = $("mlx-adapter-path");
const mlxAdapterNameEl = $("mlx-adapter-name");
const mlxLoadAdapterBtn = $("mlx-load-adapter-btn");
const mlxUnloadAdapterBtn = $("mlx-unload-adapter-btn");
const mlxLatencyTrendEl = $("mlx-latency-trend");
const mlxTpsTrendEl = $("mlx-tps-trend");
const mlxRestartTrendEl = $("mlx-restart-trend");
const mlxContractEl = $("mlx-contract");
const toolsRefreshBtn = $("tools-refresh-btn");
const toolsPolicyStatusEl = $("tools-policy-status");
const toolsHostInputEl = $("tools-host-input");
const toolsAllowBtn = $("tools-allow-btn");
const toolsBlockBtn = $("tools-block-btn");
const toolsAllowActiveBtn = $("tools-allow-active-btn");
const toolsAllowedListEl = $("tools-allowed-list");
const toolsBlockedListEl = $("tools-blocked-list");

void initializeApp();

function setContextUsageDisplay(contextUsage) {
  if (!contextUsageEl) {
    return;
  }
  if (!contextUsage || typeof contextUsage !== "object") {
    contextUsageEl.classList.add("hidden");
    contextUsageEl.classList.remove("warning", "critical");
    contextUsageEl.textContent = "Context: --/--";
    return;
  }

  const usedChars = Number(contextUsage.used_chars || 0);
  const limitChars = Number(contextUsage.limit_chars || 0);
  const messagesUsed = Number(contextUsage.messages_used || 0);
  const maxMessages = Number(contextUsage.max_messages || 0);
  const truncated = Boolean(contextUsage.truncated);
  const utilization = limitChars > 0 ? Math.round((usedChars / limitChars) * 100) : 0;
  const utilizationText = limitChars > 0 ? ` (${utilization}%)` : "";
  const droppedText = truncated ? " · truncated" : "";
  contextUsageEl.textContent = `Context: ${usedChars.toLocaleString()}/${limitChars.toLocaleString()}${utilizationText} · ${messagesUsed}/${maxMessages} msgs${droppedText}`;
  contextUsageEl.classList.remove("hidden", "warning", "critical");
  contextUsageEl.classList.toggle("critical", utilization >= 98);
  contextUsageEl.classList.toggle("warning", utilization >= 85 && utilization < 98);
}

function clearContextUsageDisplay() {
  if (!contextUsageEl) {
    return;
  }
  contextUsageEl.textContent = "Context: --/--";
  contextUsageEl.classList.add("hidden");
  contextUsageEl.classList.remove("warning", "critical");
}

function setContextUsagePending() {
  if (!contextUsageEl) {
    return;
  }
  contextUsageEl.textContent = "Context: calculating...";
  contextUsageEl.classList.remove("hidden", "warning", "critical");
}

sendBtn.addEventListener("click", async () => {
  await submitPrompt(false);
});

stopBtn.addEventListener("click", async () => {
  await stopActiveRequest();
});

promptEl.addEventListener("keydown", async (event) => {
  if (event.isComposing || event.keyCode === 229) {
    return;
  }
  if (event.key === "Escape" && hasRewriteTarget()) {
    event.preventDefault();
    clearRewriteTarget();
    return;
  }
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    await submitPrompt(false);
  }
});

newSessionBtn.addEventListener("click", () => {
  startNewSession();
});

deleteChatBtn.addEventListener("click", async () => {
  await deleteCurrentConversation();
});

refreshHistoryBtn.addEventListener("click", async () => {
  await refreshHistory(state.sessionId);
});

historyToggleBtn.addEventListener("click", () => {
  toggleHistoryPanel();
});

historyCloseBtn.addEventListener("click", () => {
  closeHistoryPanel();
});

historyPinBtn.addEventListener("click", () => {
  toggleHistoryPin();
});

historyBackdropEl.addEventListener("click", () => {
  closeHistoryPanel();
});

chatTabBtn?.addEventListener("click", () => {
  setMainTab("chat");
});

modelsTabBtn?.addEventListener("click", () => {
  setMainTab("models");
});

toolsTabBtn?.addEventListener("click", () => {
  setMainTab("tools");
});

confirmBtn.addEventListener("click", async () => {
  await submitPrompt(true);
});

cancelBtn.addEventListener("click", () => {
  hideRiskConfirm();
  state.pendingConfirmation = false;
  state.pendingRequest = null;
  appendMessage("system", "Action canceled.");
  updateComposerState();
});

actionConfirmBtn.addEventListener("click", () => {
  resolveActionConfirm(true);
});

actionCancelBtn.addEventListener("click", () => {
  resolveActionConfirm(false);
});

mlxRefreshBtn?.addEventListener("click", async () => {
  await refreshModelsState(true);
});

modelsBackendEl?.addEventListener("change", () => {
  const selected = String(modelsBackendEl.value || "").trim();
  if (selected && (selected === "llama" || selected === "codex" || selected === "mlx")) {
    backendEl.value = selected;
  }
  updateComposerState();
});

backendEl?.addEventListener("change", () => {
  const selected = String(backendEl.value || "").trim();
  if (modelsBackendEl && [...modelsBackendEl.options].some((option) => option.value === selected)) {
    modelsBackendEl.value = selected;
  }
  updateComposerState();
});

mlxStartBtn?.addEventListener("click", async () => {
  await runMlxSessionAction("assistant.mlx.session.start", "MLX started.");
});

mlxStopBtn?.addEventListener("click", async () => {
  await runMlxSessionAction("assistant.mlx.session.stop", "MLX stopped.");
});

mlxRestartBtn?.addEventListener("click", async () => {
  await runMlxSessionAction("assistant.mlx.session.restart", "MLX restarted.");
});

mlxApplyBtn?.addEventListener("click", async () => {
  await applyMlxGenerationSettings();
});

mlxLoadAdapterBtn?.addEventListener("click", async () => {
  await loadMlxAdapterFromInputs();
});

mlxUnloadAdapterBtn?.addEventListener("click", async () => {
  await unloadMlxAdapter();
});

toolsRefreshBtn?.addEventListener("click", async () => {
  await refreshToolsState(true);
});

toolsAllowBtn?.addEventListener("click", async () => {
  await allowHostFromInput();
});

toolsBlockBtn?.addEventListener("click", async () => {
  await blockHostFromInput();
});

toolsAllowActiveBtn?.addEventListener("click", async () => {
  await allowActiveTabHost();
});

toolsHostInputEl?.addEventListener("keydown", async (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    await allowHostFromInput();
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && state.actionConfirmResolver) {
    event.preventDefault();
    resolveActionConfirm(false);
    return;
  }
  if (event.key === "Escape" && (state.historyOpen || state.historyPinned)) {
    closeHistoryPanel();
  }
});

async function initializeApp() {
  setHistoryPanel(false);
  setMainTab("chat");
  await refreshBrokerHealth();
  await refreshModelsState(false);
  await refreshToolsState(false);
  await refreshHistory(state.sessionId);
  appendMessage("system", "Started a new chat. Conversations are saved on the local broker.");
  updateComposerState();
}

async function refreshBrokerHealth() {
  try {
    const result = await sendRuntimeMessage({ type: "assistant.health" });
    if (!result.ok || !result.health?.ok) {
      throw new Error(result.error || "Broker is unavailable.");
    }
    state.brokerHealth = result.health;

    const backendMode = result.health.codex_backend || "disabled";
    if (brokerStatusEl) {
      if (backendMode === "responses_ready") {
        brokerStatusEl.textContent = "Online (Codex ready)";
        brokerStatusEl.style.color = "#75f0bc";
      } else if (backendMode === "cli_ready") {
        brokerStatusEl.textContent = "Online (Codex CLI)";
        brokerStatusEl.style.color = "#ffd18c";
      } else if (backendMode === "legacy_command") {
        brokerStatusEl.textContent = "Online (Codex legacy)";
        brokerStatusEl.style.color = "#ffd18c";
      } else {
        brokerStatusEl.textContent = "Online (Llama only)";
        brokerStatusEl.style.color = "#75f0bc";
      }
    }
  } catch (error) {
    state.brokerHealth = null;
    if (brokerStatusEl) {
      brokerStatusEl.textContent = "Offline";
      brokerStatusEl.style.color = "#ff6d94";
    }
    appendMessage("system", `Broker health check failed: ${String(error.message || error)}`);
  } finally {
    updateComposerState();
  }
}

async function refreshHistory(selectedId = null) {
  try {
    const result = await sendRuntimeMessage({ type: "assistant.history.list" });
    if (!result.ok) {
      throw new Error(result.error || "Failed to load history.");
    }
    state.conversationList = Array.isArray(result.conversations) ? result.conversations : [];
    renderHistory(selectedId ?? state.sessionId);
  } catch (error) {
    appendMessage("system", `History load failed: ${String(error.message || error)}`);
  }
}

function setMainTab(tab) {
  const next = tab === "models" || tab === "tools" ? tab : "chat";
  state.activeMainTab = next;

  const tabConfig = [
    { key: "chat", button: chatTabBtn, view: chatViewEl },
    { key: "models", button: modelsTabBtn, view: modelsViewEl },
    { key: "tools", button: toolsTabBtn, view: toolsViewEl }
  ];

  for (const config of tabConfig) {
    const active = config.key === next;
    config.button?.classList.toggle("active", active);
    config.button?.setAttribute("aria-selected", String(active));
    config.view?.classList.toggle("hidden", !active);
    config.view?.setAttribute("aria-hidden", String(!active));
  }

  if (next !== "chat") {
    closeHistoryPanel();
  }
  if (next === "models") {
    void refreshModelsState(false);
  }
  if (next === "tools") {
    void refreshToolsState(false);
  }
}

function setModelsBusy(busy) {
  state.modelsBusy = busy;
  const controls = [
    mlxRefreshBtn,
    mlxStartBtn,
    mlxStopBtn,
    mlxRestartBtn,
    mlxApplyBtn,
    mlxLoadAdapterBtn,
    mlxUnloadAdapterBtn
  ];
  for (const control of controls) {
    if (control) {
      control.disabled = busy;
    }
  }
  if (mlxEnableThinkingEl) {
    mlxEnableThinkingEl.disabled = busy;
  }
}

function formatMlxStatus(mlx) {
  const status = String(mlx?.status || "unknown");
  const modelPath = String(mlx?.model_path || "");
  const activeAdapter = mlx?.active_adapter?.name ? ` | adapter: ${mlx.active_adapter.name}` : "";
  const errorText = mlx?.last_error ? ` | error: ${String(mlx.last_error)}` : "";
  return `Status: ${status}${modelPath ? ` | model: ${modelPath}` : ""}${activeAdapter}${errorText}`;
}

function formatMlxContract(contract) {
  if (!contract || typeof contract !== "object") {
    return "(no contract metadata)";
  }
  const fields = [
    "schema_version",
    "message_format",
    "tool_call_format",
    "chat_template_assumption",
    "tokenizer_template_mode",
    "max_context_behavior",
    "max_context_chars"
  ];
  const lines = [];
  for (const field of fields) {
    if (Object.prototype.hasOwnProperty.call(contract, field)) {
      lines.push(`${field}: ${String(contract[field])}`);
    }
  }
  return lines.length ? lines.join("\n") : "(no contract metadata)";
}

function renderModelsBackends(backends = []) {
  if (!modelsBackendEl) {
    return;
  }
  const current = String(modelsBackendEl.value || backendEl.value || "mlx");
  modelsBackendEl.textContent = "";
  const options = Array.isArray(backends) && backends.length
    ? backends
    : [{ id: "mlx", label: "MLX Local", available: true }];
  for (const backend of options) {
    const id = String(backend?.id || "").trim();
    if (!id) {
      continue;
    }
    const option = document.createElement("option");
    option.value = id;
    option.textContent = String(backend?.label || id);
    option.disabled = backend?.available === false;
    modelsBackendEl.appendChild(option);
  }
  const availableOptions = [...modelsBackendEl.options];
  const activeOption = availableOptions.find((option) => option.value === current && !option.disabled);
  const firstAvailable = availableOptions.find((option) => !option.disabled);
  if (activeOption) {
    modelsBackendEl.value = current;
  } else if (firstAvailable) {
    modelsBackendEl.value = firstAvailable.value;
  } else if (availableOptions.length > 0) {
    modelsBackendEl.value = modelsBackendEl.options[0].value;
  }
}

function toSparkline(values) {
  const points = Array.isArray(values) ? values.map((value) => Number(value)).filter((value) => Number.isFinite(value)) : [];
  if (!points.length) {
    return "(no data)";
  }
  const glyphs = ["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"];
  const min = Math.min(...points);
  const max = Math.max(...points);
  if (max === min) {
    return points.map(() => glyphs[3]).join("");
  }
  return points
    .map((value) => {
      const ratio = (value - min) / (max - min);
      const idx = Math.max(0, Math.min(glyphs.length - 1, Math.round(ratio * (glyphs.length - 1))));
      return glyphs[idx];
    })
    .join("");
}

function renderMlxTrends(mlx) {
  const metrics = mlx?.metrics && typeof mlx.metrics === "object" ? mlx.metrics : {};
  const latency = Array.isArray(metrics.latency_ms) ? metrics.latency_ms : [];
  const tps = Array.isArray(metrics.tokens_per_sec) ? metrics.tokens_per_sec : [];
  const restartSuccess = Number(metrics.restart_success_count || 0);
  const restartFailure = Number(metrics.restart_failure_count || 0);
  if (mlxLatencyTrendEl) {
    mlxLatencyTrendEl.textContent = `Latency (ms): ${toSparkline(latency)}`;
  }
  if (mlxTpsTrendEl) {
    mlxTpsTrendEl.textContent = `Tokens/sec: ${toSparkline(tps)}`;
  }
  if (mlxRestartTrendEl) {
    mlxRestartTrendEl.textContent = `Restarts: success ${restartSuccess} / failed ${restartFailure}`;
  }
}

function renderMlxAdapters(payload) {
  if (!mlxAdapterListEl) {
    return;
  }
  const adapters = Array.isArray(payload?.adapters) ? payload.adapters : [];
  const activeId = String(payload?.active_adapter?.id || "");
  if (!adapters.length) {
    mlxAdapterListEl.textContent = "No adapters registered.";
    return;
  }
  const lines = [];
  for (const adapter of adapters) {
    const id = String(adapter?.id || "");
    const name = String(adapter?.name || id || "adapter");
    const path = String(adapter?.path || "");
    lines.push(`${id === activeId ? "●" : "○"} ${name}${path ? ` — ${path}` : ""}`);
  }
  mlxAdapterListEl.textContent = lines.join("\n");
}

function readGenerationInputs() {
  const seedRaw = String(mlxSeedEl?.value || "").trim();
  const generation = {
    temperature: Number(mlxTemperatureEl?.value || 0),
    top_p: Number(mlxTopPEl?.value || 0),
    top_k: Number(mlxTopKEl?.value || 0),
    max_tokens: Number(mlxMaxTokensEl?.value || 0),
    repetition_penalty: Number(mlxRepetitionPenaltyEl?.value || 0),
    seed: seedRaw === "" ? null : Number(seedRaw),
    enable_thinking: Boolean(mlxEnableThinkingEl?.checked)
  };
  return generation;
}

function readMlxSystemPromptInput() {
  return String(mlxSystemPromptEl?.value || "").trim();
}

function fillGenerationInputs(config) {
  const generation = config && typeof config === "object" ? config : {};
  if (mlxTemperatureEl) {
    mlxTemperatureEl.value = String(generation.temperature ?? 0.2);
  }
  if (mlxTopPEl) {
    mlxTopPEl.value = String(generation.top_p ?? 0.95);
  }
  if (mlxTopKEl) {
    mlxTopKEl.value = String(generation.top_k ?? 50);
  }
  if (mlxMaxTokensEl) {
    mlxMaxTokensEl.value = String(generation.max_tokens ?? 512);
  }
  if (mlxRepetitionPenaltyEl) {
    mlxRepetitionPenaltyEl.value = String(generation.repetition_penalty ?? 1.0);
  }
  if (mlxSeedEl) {
    mlxSeedEl.value = generation.seed === null || generation.seed === undefined ? "" : String(generation.seed);
  }
  if (mlxEnableThinkingEl) {
    mlxEnableThinkingEl.checked = Boolean(generation.enable_thinking);
  }
}

function fillSystemPromptInput(value) {
  if (mlxSystemPromptEl) {
    mlxSystemPromptEl.value = String(value || "");
  }
}

async function refreshModelsState(showErrors = true) {
  setModelsBusy(true);
  try {
    const modelsResult = await sendRuntimeMessage({ type: "assistant.models.get" });
    if (!modelsResult.ok) {
      throw new Error(modelsResult.error || "Failed to load models metadata.");
    }
    const backends = Array.isArray(modelsResult.backends) ? modelsResult.backends : [];
    const mlx = modelsResult.mlx && typeof modelsResult.mlx === "object" ? modelsResult.mlx : {};
    renderModelsBackends(backends);
    if (modelsBackendEl?.value) {
      backendEl.value = modelsBackendEl.value;
    }
    updateComposerState();
    if (mlxModelPathEl) {
      mlxModelPathEl.value = String(mlx.model_path || "");
    }
    if (mlxRuntimeStatusEl) {
      mlxRuntimeStatusEl.textContent = formatMlxStatus(mlx);
    }
    if (mlxContractEl) {
      mlxContractEl.textContent = formatMlxContract(mlx.contract);
    }
    fillGenerationInputs(mlx.generation_config || {});
    fillSystemPromptInput(mlx.system_prompt);
    renderMlxTrends(mlx);

    const adaptersResult = await sendRuntimeMessage({ type: "assistant.mlx.adapters.list" });
    if (adaptersResult.ok) {
      renderMlxAdapters(adaptersResult);
    }
  } catch (error) {
    if (showErrors && mlxRuntimeStatusEl) {
      mlxRuntimeStatusEl.textContent = `MLX status error: ${String(error.message || error)}`;
    }
    if (showErrors && mlxContractEl) {
      mlxContractEl.textContent = "(contract unavailable)";
    }
  } finally {
    setModelsBusy(false);
  }
}

async function runMlxSessionAction(type, successText) {
  setModelsBusy(true);
  try {
    const result = await sendRuntimeMessage({ type });
    if (!result.ok) {
      throw new Error(result.error || "MLX session action failed.");
    }
    if (mlxRuntimeStatusEl) {
      mlxRuntimeStatusEl.textContent = successText;
    }
    await refreshModelsState(false);
  } catch (error) {
    if (mlxRuntimeStatusEl) {
      mlxRuntimeStatusEl.textContent = `MLX error: ${String(error.message || error)}`;
    }
  } finally {
    setModelsBusy(false);
  }
}

async function applyMlxGenerationSettings() {
  setModelsBusy(true);
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.mlx.config",
      generation: readGenerationInputs(),
      systemPrompt: readMlxSystemPromptInput()
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to update MLX settings.");
    }
    if (mlxRuntimeStatusEl) {
      mlxRuntimeStatusEl.textContent = "MLX settings updated.";
    }
    await refreshModelsState(false);
  } catch (error) {
    if (mlxRuntimeStatusEl) {
      mlxRuntimeStatusEl.textContent = `MLX config error: ${String(error.message || error)}`;
    }
  } finally {
    setModelsBusy(false);
  }
}

async function loadMlxAdapterFromInputs() {
  const path = String(mlxAdapterPathEl?.value || "").trim();
  const name = String(mlxAdapterNameEl?.value || "").trim();
  if (!path) {
    if (mlxRuntimeStatusEl) {
      mlxRuntimeStatusEl.textContent = "Enter an adapter path first.";
    }
    return;
  }
  setModelsBusy(true);
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.mlx.adapters.load",
      path,
      name
    });
    if (!result.ok) {
      throw new Error(result.error || "Adapter load failed.");
    }
    mlxAdapterPathEl.value = "";
    mlxAdapterNameEl.value = "";
    renderMlxAdapters(result);
    await refreshModelsState(false);
  } catch (error) {
    if (mlxRuntimeStatusEl) {
      mlxRuntimeStatusEl.textContent = `Adapter load error: ${String(error.message || error)}`;
    }
  } finally {
    setModelsBusy(false);
  }
}

async function unloadMlxAdapter() {
  setModelsBusy(true);
  try {
    const result = await sendRuntimeMessage({ type: "assistant.mlx.adapters.unload" });
    if (!result.ok) {
      throw new Error(result.error || "Adapter unload failed.");
    }
    renderMlxAdapters(result);
    await refreshModelsState(false);
  } catch (error) {
    if (mlxRuntimeStatusEl) {
      mlxRuntimeStatusEl.textContent = `Adapter unload error: ${String(error.message || error)}`;
    }
  } finally {
    setModelsBusy(false);
  }
}

function normalizeHostToken(rawValue) {
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

function hostPermissionOrigins(host) {
  const normalized = normalizeHostToken(host);
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

async function requestHostPermission(host) {
  const normalized = normalizeHostToken(host);
  if (!normalized) {
    return false;
  }
  if (!chrome.permissions?.contains || !chrome.permissions?.request) {
    return true;
  }

  const origins = hostPermissionOrigins(normalized);
  if (origins.length === 0) {
    return true;
  }

  try {
    const alreadyGranted = await chrome.permissions.contains({ origins });
    if (alreadyGranted) {
      return true;
    }
  } catch (error) {
    console.warn(`[secure-panel] host permission check failed for ${normalized}:`, String(error?.message || error));
    return false;
  }

  try {
    return await chrome.permissions.request({ origins });
  } catch (error) {
    console.warn(`[secure-panel] host permission request failed for ${normalized}:`, String(error?.message || error));
    return false;
  }
}

function normalizeHostArray(values) {
  const list = Array.isArray(values) ? values : [];
  const deduped = [];
  for (const value of list) {
    const host = normalizeHostToken(value);
    if (!host || deduped.includes(host)) {
      continue;
    }
    deduped.push(host);
  }
  return deduped;
}

function normalizeToolsPolicy(policy) {
  const raw = policy && typeof policy === "object" ? policy : {};
  return {
    default_hosts: normalizeHostArray(raw.default_hosts),
    custom_allowed_hosts: normalizeHostArray(raw.custom_allowed_hosts),
    blocked_hosts: normalizeHostArray(raw.blocked_hosts),
    effective_allowed_hosts: normalizeHostArray(raw.effective_allowed_hosts)
  };
}

function setToolsBusy(busy) {
  state.toolsBusy = busy;
  const controls = [
    toolsRefreshBtn,
    toolsHostInputEl,
    toolsAllowBtn,
    toolsBlockBtn,
    toolsAllowActiveBtn
  ];
  for (const control of controls) {
    if (control) {
      control.disabled = busy;
    }
  }
  for (const button of toolsAllowedListEl?.querySelectorAll("button") || []) {
    button.disabled = busy;
  }
  for (const button of toolsBlockedListEl?.querySelectorAll("button") || []) {
    button.disabled = busy;
  }
}

function updateToolsStatus(text) {
  if (toolsPolicyStatusEl) {
    toolsPolicyStatusEl.textContent = text;
  }
}

function renderToolsPolicy(policy) {
  const normalized = normalizeToolsPolicy(policy);
  state.toolsPolicy = normalized;
  const defaultHosts = new Set(normalized.default_hosts);
  const customHosts = new Set(normalized.custom_allowed_hosts);

  if (toolsAllowedListEl) {
    toolsAllowedListEl.textContent = "";
    if (!normalized.effective_allowed_hosts.length) {
      const empty = document.createElement("p");
      empty.className = "tools-empty";
      empty.textContent = "No allowlisted hosts.";
      toolsAllowedListEl.appendChild(empty);
    } else {
      for (const host of normalized.effective_allowed_hosts) {
        const row = document.createElement("div");
        row.className = "tools-item";

        const meta = document.createElement("div");
        meta.className = "tools-item-meta";

        const hostEl = document.createElement("div");
        hostEl.className = "tools-item-host";
        hostEl.textContent = host;
        meta.appendChild(hostEl);

        const tags = document.createElement("div");
        tags.className = "tools-item-tags";
        const tagsToRender = [];
        if (defaultHosts.has(host)) {
          tagsToRender.push({ label: "default", className: "default" });
        }
        if (customHosts.has(host)) {
          tagsToRender.push({ label: "custom", className: "custom" });
        }
        if (!tagsToRender.length) {
          tagsToRender.push({ label: "effective", className: "" });
        }
        for (const tag of tagsToRender) {
          const chip = document.createElement("span");
          chip.className = `tools-chip${tag.className ? ` ${tag.className}` : ""}`;
          chip.textContent = tag.label;
          tags.appendChild(chip);
        }
        meta.appendChild(tags);
        row.appendChild(meta);

        const actions = document.createElement("div");
        actions.className = "tools-item-actions";

        const disallowBtn = document.createElement("button");
        disallowBtn.className = "ghost small";
        disallowBtn.textContent = "Disallow";
        disallowBtn.addEventListener("click", async () => {
          await runHostPolicyAction(
            { type: "assistant.tools.page_hosts.block", host },
            `${host} moved to disallow list.`
          );
        });
        actions.appendChild(disallowBtn);

        if (customHosts.has(host)) {
          const removeBtn = document.createElement("button");
          removeBtn.className = "ghost small";
          removeBtn.textContent = "Remove";
          removeBtn.addEventListener("click", async () => {
            await runHostPolicyAction(
              { type: "assistant.tools.page_hosts.remove_allow", host },
              `${host} removed from custom allowlist.`
            );
          });
          actions.appendChild(removeBtn);
        }

        row.appendChild(actions);
        toolsAllowedListEl.appendChild(row);
      }
    }
  }

  if (toolsBlockedListEl) {
    toolsBlockedListEl.textContent = "";
    if (!normalized.blocked_hosts.length) {
      const empty = document.createElement("p");
      empty.className = "tools-empty";
      empty.textContent = "No disallowed hosts.";
      toolsBlockedListEl.appendChild(empty);
    } else {
      for (const host of normalized.blocked_hosts) {
        const row = document.createElement("div");
        row.className = "tools-item";

        const meta = document.createElement("div");
        meta.className = "tools-item-meta";

        const hostEl = document.createElement("div");
        hostEl.className = "tools-item-host";
        hostEl.textContent = host;
        meta.appendChild(hostEl);

        const tags = document.createElement("div");
        tags.className = "tools-item-tags";
        const chip = document.createElement("span");
        chip.className = "tools-chip blocked";
        chip.textContent = defaultHosts.has(host) ? "blocked default" : "blocked";
        tags.appendChild(chip);
        meta.appendChild(tags);
        row.appendChild(meta);

        const actions = document.createElement("div");
        actions.className = "tools-item-actions";

        const allowBtn = document.createElement("button");
        allowBtn.className = "small";
        allowBtn.textContent = "Allow";
        allowBtn.addEventListener("click", async () => {
          await runHostPolicyAction(
            { type: "assistant.tools.page_hosts.allow", host },
            `${host} added to allowlist.`
          );
        });
        actions.appendChild(allowBtn);

        const unblockBtn = document.createElement("button");
        unblockBtn.className = "ghost small";
        unblockBtn.textContent = "Unblock";
        unblockBtn.addEventListener("click", async () => {
          await runHostPolicyAction(
            { type: "assistant.tools.page_hosts.unblock", host },
            `${host} removed from disallow list.`
          );
        });
        actions.appendChild(unblockBtn);

        row.appendChild(actions);
        toolsBlockedListEl.appendChild(row);
      }
    }
  }
  setToolsBusy(state.toolsBusy);
}

async function refreshToolsState(showErrors = true) {
  setToolsBusy(true);
  try {
    const [policyResult, activeResult] = await Promise.all([
      sendRuntimeMessage({ type: "assistant.tools.page_hosts.get" }),
      sendRuntimeMessage({ type: "assistant.tools.page_hosts.active_tab" })
    ]);
    if (!policyResult.ok) {
      throw new Error(policyResult.error || "Failed to load tool policy.");
    }
    renderToolsPolicy(policyResult.policy);
    state.toolsActiveTab =
      activeResult.ok && activeResult.active_tab && typeof activeResult.active_tab === "object"
        ? activeResult.active_tab
        : null;

    if (state.toolsActiveTab?.host) {
      const marker = state.toolsActiveTab.blocked ? "blocked" : state.toolsActiveTab.allowed ? "allowed" : "not allowed";
      updateToolsStatus(`Active tab: ${state.toolsActiveTab.host} (${marker})`);
    } else {
      updateToolsStatus("Active tab host unavailable.");
    }
  } catch (error) {
    if (showErrors) {
      updateToolsStatus(`Tools error: ${String(error.message || error)}`);
    }
  } finally {
    setToolsBusy(false);
  }
}

async function runHostPolicyAction(message, successStatus) {
  setToolsBusy(true);
  try {
    const result = await sendRuntimeMessage(message);
    if (!result.ok) {
      throw new Error(result.error || "Host policy update failed.");
    }
    renderToolsPolicy(result.policy);
    updateToolsStatus(successStatus || "Tools policy updated.");
    return true;
  } catch (error) {
    updateToolsStatus(`Tools error: ${String(error.message || error)}`);
    return false;
  } finally {
    setToolsBusy(false);
  }
}

async function allowHostFromInput() {
  const rawHost = String(toolsHostInputEl?.value || "").trim();
  if (!rawHost) {
    updateToolsStatus("Enter a host to allow.");
    return;
  }
  const host = normalizeHostToken(rawHost);
  if (!host) {
    updateToolsStatus("Host must be a valid hostname like example.com.");
    return;
  }
  const ok = await runHostPolicyAction(
    { type: "assistant.tools.page_hosts.allow", host },
    `${host} added to allowlist.`
  );
  if (ok && toolsHostInputEl) {
    toolsHostInputEl.value = "";
  }
  if (!ok) {
    return;
  }
  const permissionGranted = await requestHostPermission(host);
  if (!permissionGranted) {
    updateToolsStatus(
      `Added ${host} to allowlist, but browser access permission is not enabled. `
      + "Click Allow when Chrome prompts, or enable it in extension settings."
    );
  } else {
    updateToolsStatus(`${host} added to allowlist and browser access is enabled.`);
  }
}

async function blockHostFromInput() {
  const rawHost = String(toolsHostInputEl?.value || "").trim();
  if (!rawHost) {
    updateToolsStatus("Enter a host to disallow.");
    return;
  }
  const host = normalizeHostToken(rawHost);
  if (!host) {
    updateToolsStatus("Host must be a valid hostname like example.com.");
    return;
  }
  const ok = await runHostPolicyAction(
    { type: "assistant.tools.page_hosts.block", host },
    `${host} moved to disallow list.`
  );
  if (ok && toolsHostInputEl) {
    toolsHostInputEl.value = "";
  }
}

async function allowActiveTabHost() {
  setToolsBusy(true);
  try {
    const result = await sendRuntimeMessage({ type: "assistant.tools.page_hosts.allow_active_tab" });
    if (!result.ok) {
      throw new Error(result.error || "Unable to allow active tab host.");
    }
    renderToolsPolicy(result.policy);
    const host = String(result.host || "").trim();
    const hostLabel = host || "Active tab host";
    updateToolsStatus(`${hostLabel} added to allowlist.`);
    if (host) {
      const permissionGranted = await requestHostPermission(host);
      if (!permissionGranted) {
        updateToolsStatus(
          `Added ${hostLabel} to allowlist, but browser access permission is not enabled. `
          + "Click Allow when Chrome prompts, or enable it in extension settings."
        );
      } else {
        updateToolsStatus(`${hostLabel} added to allowlist and browser access is enabled.`);
      }
    }
  } catch (error) {
    updateToolsStatus(`Tools error: ${String(error.message || error)}`);
  } finally {
    setToolsBusy(false);
  }
}

function renderHistory(selectedId) {
  historyListEl.textContent = "";
  if (!state.conversationList.length) {
    const empty = document.createElement("p");
    empty.className = "empty-state";
    empty.textContent = "No saved conversations yet.";
    historyListEl.appendChild(empty);
    return;
  }

  for (const conversation of state.conversationList) {
    const button = document.createElement("button");
    button.className = `history-item${conversation.id === selectedId ? " active" : ""}`;
    button.addEventListener("click", async () => {
      await loadConversation(conversation.id);
      if (!state.historyPinned) {
        closeHistoryPanel();
      }
    });

    const title = document.createElement("span");
    title.className = "history-title";
    title.textContent = conversation.title || "Untitled";

    const meta = document.createElement("span");
    meta.className = "history-meta";
    const count = Number(conversation.message_count || 0);
    meta.textContent = `${count} msg • ${formatTime(conversation.updated_at)}`;

    button.appendChild(title);
    button.appendChild(meta);
    historyListEl.appendChild(button);
  }
}

async function loadConversation(sessionId) {
  resolveActionConfirm(false);
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.history.get",
      sessionId
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to load conversation.");
    }
    const conversation = result.conversation;
    if (!conversation || !Array.isArray(conversation.messages)) {
      throw new Error("Conversation payload is invalid.");
    }

    state.sessionId = conversation.id;
    state.pendingConfirmation = false;
    state.pendingRequest = null;
    state.stopping = false;
    state.activeCodexRunId = "";
    state.activeLegacyRequestId = "";
    state.activeLegacyPendingNode = null;
    state.stoppedLegacyRequests = new Set();
    state.codexRunUi = new Map();
    state.rewriteTargetIndex = null;
    hideRiskConfirm();
    renderConversationMessages(conversation.messages);

    renderHistory(state.sessionId);
    await restoreCodexRun(conversation);
  } catch (error) {
    appendMessage("system", `Conversation load failed: ${String(error.message || error)}`);
  } finally {
    updateComposerState();
  }
}

async function deleteCurrentConversation() {
  const id = state.sessionId;
  const exists = state.conversationList.some((item) => item.id === id);
  if (!exists) {
    startNewSession("Current chat is unsaved. Started a new chat.");
    return;
  }
  const accepted = await requestActionConfirm({
    title: "Delete Chat?",
    text: "Delete this conversation permanently? This cannot be undone.",
    confirmLabel: "Delete"
  });
  if (!accepted) {
    return;
  }
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.history.delete",
      sessionId: id
    });
    if (!result.ok) {
      throw new Error(result.error || "Delete failed.");
    }
    await refreshHistory();
    if (state.conversationList.length > 0) {
      await loadConversation(state.conversationList[0].id);
    } else {
      startNewSession("Conversation deleted.");
    }
  } catch (error) {
    appendMessage("system", `Delete failed: ${String(error.message || error)}`);
  }
}

function startNewSession(message = "Started a new chat.") {
  resolveActionConfirm(false);
  state.sessionId = crypto.randomUUID();
  state.pendingConfirmation = false;
  state.pendingRequest = null;
  state.stopping = false;
  state.activeCodexRunId = "";
  state.activeLegacyRequestId = "";
  state.activeLegacyPendingNode = null;
  state.stoppedLegacyRequests = new Set();
  state.codexRunUi = new Map();
  state.rewriteTargetIndex = null;
  hideRiskConfirm();
  clearContextUsageDisplay();
  clearMessages();
  appendMessage("system", message);
  renderHistory(state.sessionId);
  updateComposerState();
}

function requestActionConfirm(options = {}) {
  const title = String(options.title || "Confirm Action");
  const text = String(options.text || "");
  const confirmLabel = String(options.confirmLabel || "Confirm");

  if (!actionConfirmEl) {
    return Promise.resolve(false);
  }

  if (state.actionConfirmResolver) {
    resolveActionConfirm(false);
  }

  actionConfirmTitleEl.textContent = title;
  actionConfirmTextEl.textContent = text;
  actionConfirmBtn.textContent = confirmLabel;
  actionConfirmEl.classList.remove("hidden");
  actionConfirmEl.setAttribute("aria-hidden", "false");
  actionConfirmBtn.focus({ preventScroll: true });

  return new Promise((resolve) => {
    state.actionConfirmResolver = resolve;
  });
}

function resolveActionConfirm(accepted) {
  const resolver = state.actionConfirmResolver;
  if (!resolver) {
    hideActionConfirm();
    return;
  }
  state.actionConfirmResolver = null;
  hideActionConfirm();
  resolver(Boolean(accepted));
}

function hideActionConfirm() {
  if (!actionConfirmEl) {
    return;
  }
  actionConfirmEl.classList.add("hidden");
  actionConfirmEl.setAttribute("aria-hidden", "true");
}

async function restoreCodexRun(conversation) {
  const codex = conversation?.codex && typeof conversation.codex === "object" ? conversation.codex : null;
  if (!codex) {
    return;
  }
  const activeRunId = typeof codex.active_run_id === "string" ? codex.active_run_id : "";
  const lastRunId = typeof codex.last_run_id === "string" ? codex.last_run_id : "";
  const runId = activeRunId || lastRunId;
  if (!runId) {
    return;
  }

  const allowAssistantBubble = Boolean(activeRunId);
  const runUi = ensureCodexRunUi(runId, conversation.id, allowAssistantBubble, "codex");
  runUi.lastSeq = 0;
  runUi.renderedSeqs.clear();
  runUi.waitingNode = null;
  runUi.approvalCards = new Map();
  runUi.assistantNode = null;
  runUi.thinkingNode = null;

  try {
    const result = await sendRuntimeMessage({
      type: "assistant.codex.run.events",
      runId,
      after: 0,
      timeoutMs: 0
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to restore Codex run.");
    }
    renderCodexEvents(runId, Array.isArray(result.events) ? result.events : []);
    runUi.lastSeq = getLastRenderedSeq(runUi);
    if (activeRunId && !isTerminalRunStatus(result.status)) {
      state.activeCodexRunId = runId;
      showRunWaitingIndicator(runUi);
      void pollCodexRun(runId, conversation.id);
    }
  } catch (error) {
    appendMessage("system", `Codex run restore failed: ${String(error.message || error)}`);
  }
}

async function submitPrompt(confirmed) {
  if (state.busy) {
    return;
  }
  if (state.pendingConfirmation && !confirmed) {
    appendMessage("system", "Confirm or cancel the pending high-risk request.");
    return;
  }
  if (state.activeCodexRunId) {
    appendMessage("system", "Wait for the active Codex run to finish or cancel it first.");
    return;
  }

  let request = null;
  let backend = backendEl.value;

  if (confirmed) {
    if (!state.pendingRequest) {
      appendMessage("system", "No pending action to confirm.");
      return;
    }
    request = { ...state.pendingRequest, confirmed: true };
    if ((request.type === "assistant.query" || request.type === "assistant.history.rewrite") && !request.requestId) {
      request.requestId = crypto.randomUUID();
    }
    backend = request.backend || "codex";
  } else {
    const prompt = promptEl.value.trim();
    if (!prompt) {
      appendMessage("system", "Enter a prompt first.");
      return;
    }
    const rewriteIndex = hasRewriteTarget() ? Number(state.rewriteTargetIndex) : null;
    const isRewrite = Number.isInteger(rewriteIndex);
    const forceBrowserAction = forceBrowserActionEl?.checked === true;
    if (!isRewrite) {
      appendMessage("user", prompt);
    }
    promptEl.value = "";

    if (usesCodexRunProtocol()) {
      request = {
        type: "assistant.codex.run.start",
        backend,
        sessionId: state.sessionId,
        prompt,
        includePageContext: includePageContextEl.checked,
        forceBrowserAction,
        confirmed: false
      };
      if (isRewrite) {
        request.rewriteMessageIndex = rewriteIndex;
      }
    } else if (isRewrite) {
      request = {
        type: "assistant.history.rewrite",
        backend,
        sessionId: state.sessionId,
        requestId: crypto.randomUUID(),
        messageIndex: rewriteIndex,
        prompt,
        includePageContext: includePageContextEl.checked,
        forceBrowserAction,
        confirmed: false
      };
    } else {
      request = {
        type: "assistant.query",
        backend,
        sessionId: state.sessionId,
        requestId: crypto.randomUUID(),
        prompt,
        includePageContext: includePageContextEl.checked,
        forceBrowserAction,
        confirmed: false
      };
    }
  }

  if (request.type === "assistant.codex.run.start") {
    clearContextUsageDisplay();
    setBusy(true);
    await submitCodexRun(request);
    return;
  }
  setContextUsagePending();
  setBusy(true);
  await submitLegacyQuery(request);
}

function extractBlockedHostFromResult(result) {
  if (!result || result.ok !== false) {
    return "";
  }
  const code = String(result.error_code || "").trim().toLowerCase();
  const host = normalizeHostToken(result?.error_data?.host || "");
  if (code === "host_not_allowlisted" && host) {
    return host;
  }
  if (host && /allowlist|allowlisted|not allow/i.test(String(result.error || ""))) {
    return host;
  }
  return "";
}

async function maybeAllowBlockedHostAndRetry(result, pendingNode = null) {
  const host = extractBlockedHostFromResult(result);
  if (!host) {
    return { handled: false, retry: false };
  }

  const accepted = await requestActionConfirm({
    title: "Page Host Blocked",
    text: `${host} is blocked by your page policy. Add it to the allowlist and retry?`,
    confirmLabel: "Allow + Retry"
  });

  if (!accepted) {
    const text = `Blocked by page policy: ${host}. Request canceled.`;
    if (pendingNode) {
      updateMessage(pendingNode, "assistant", text);
    } else {
      appendMessage("system", text);
    }
    return { handled: true, retry: false };
  }

  const allowResult = await sendRuntimeMessage({
    type: "assistant.tools.page_hosts.allow",
    host
  });
  if (!allowResult.ok) {
    throw new Error(allowResult.error || `Failed to allow host ${host}.`);
  }

  renderToolsPolicy(allowResult.policy);
  updateToolsStatus(`${host} added to allowlist.`);
  if (pendingNode) {
    updateMessage(pendingNode, "assistant", `Allowed ${host}. Retrying...`, true);
  } else {
    appendMessage("system", `Allowed ${host}. Retrying request.`);
  }
  return { handled: true, retry: true };
}

async function submitLegacyQuery(message) {
  const requestId = String(message?.requestId || "").trim();
  if (!requestId) {
    appendMessage("assistant", "Error: Missing request id for cancellation.");
    setBusy(false);
    return;
  }
  const isRewrite = message?.type === "assistant.history.rewrite";
  const pendingMessage = appendMessage(
    "assistant",
    isRewrite ? "Regenerating from edited prompt..." : "Thinking...",
    true
  );
  hideRiskConfirm();
  state.activeLegacyRequestId = requestId;
  state.activeLegacyPendingNode = pendingMessage;
  state.stoppedLegacyRequests.delete(requestId);

  try {
    let result = null;
    let retriedAfterAllow = false;
    while (true) {
      result = await sendRuntimeMessage(message);
      if (result.ok) {
        break;
      }
      const recovery = retriedAfterAllow
        ? { handled: false, retry: false }
        : await maybeAllowBlockedHostAndRetry(result, pendingMessage);
      if (recovery.retry && !retriedAfterAllow) {
        retriedAfterAllow = true;
        continue;
      }
      if (recovery.handled) {
        clearContextUsageDisplay();
        return;
      }
      throw new Error(result.error || "Unknown failure.");
    }

    if (state.stoppedLegacyRequests.has(requestId) || result.cancelled) {
      state.pendingConfirmation = false;
      state.pendingRequest = null;
      hideRiskConfirm();
      updateMessage(pendingMessage, "assistant", "Request stopped.");
      clearContextUsageDisplay();
      return;
    }

    if (result.requires_confirmation) {
      state.pendingConfirmation = true;
      state.pendingRequest = message;
      clearContextUsageDisplay();
      showRiskConfirm(result.risk_flags || ["high_risk_request"]);
      updateMessage(pendingMessage, "assistant", "High-risk request detected. Confirmation required.");
      return;
    }

    state.pendingConfirmation = false;
    state.pendingRequest = null;
    hideRiskConfirm();
    setContextUsageDisplay(result.context_usage);
    updateMessage(
      pendingMessage,
      "assistant",
      result.answer || "(No answer returned)",
      false,
      "",
      result.reasoning_blocks
    );
    clearRewriteTarget();
    await refreshHistory(state.sessionId);
    await loadConversation(state.sessionId);
  } catch (error) {
    if (state.stoppedLegacyRequests.has(requestId)) {
      state.pendingConfirmation = false;
      state.pendingRequest = null;
      hideRiskConfirm();
      updateMessage(pendingMessage, "assistant", "Request stopped.");
    } else {
      clearContextUsageDisplay();
      updateMessage(pendingMessage, "assistant", `Error: ${String(error.message || error)}`);
    }
  } finally {
    if (state.activeLegacyRequestId === requestId) {
      state.activeLegacyRequestId = "";
      state.activeLegacyPendingNode = null;
    }
    state.stoppedLegacyRequests.delete(requestId);
    state.stopping = false;
    setBusy(false);
  }
}

async function stopActiveRequest() {
  if (state.stopping) {
    return;
  }

  if (state.activeCodexRunId) {
    state.stopping = true;
    updateComposerState();
    try {
      await cancelCodexRun(state.activeCodexRunId);
    } finally {
      state.stopping = false;
      updateComposerState();
    }
    return;
  }

  const requestId = String(state.activeLegacyRequestId || "").trim();
  if (!requestId) {
    return;
  }

  state.stopping = true;
  state.stoppedLegacyRequests.add(requestId);
  if (state.activeLegacyPendingNode) {
    updateMessage(state.activeLegacyPendingNode, "assistant", "Stopping...", true);
  }
  updateComposerState();

  try {
    const result = await sendRuntimeMessage({
      type: "assistant.query.cancel",
      sessionId: state.sessionId,
      requestId
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to stop request.");
    }
  } catch (error) {
    state.stoppedLegacyRequests.delete(requestId);
    state.stopping = false;
    if (state.activeLegacyPendingNode) {
      updateMessage(state.activeLegacyPendingNode, "assistant", `Stop failed: ${String(error.message || error)}`);
    } else {
      appendMessage("system", `Stop failed: ${String(error.message || error)}`);
    }
    updateComposerState();
  }
}

async function submitCodexRun(message) {
  hideRiskConfirm();

  try {
    let result = null;
    let retriedAfterAllow = false;
    while (true) {
      result = await sendRuntimeMessage(message);
      if (result.ok) {
        break;
      }
      const recovery = retriedAfterAllow
        ? { handled: false, retry: false }
        : await maybeAllowBlockedHostAndRetry(result);
      if (recovery.retry && !retriedAfterAllow) {
        retriedAfterAllow = true;
        continue;
      }
      if (recovery.handled) {
        return;
      }
      throw new Error(result.error || "Failed to start Codex run.");
    }

    if (result.requires_confirmation) {
      state.pendingConfirmation = true;
      state.pendingRequest = message;
      showRiskConfirm(result.risk_flags || ["high_risk_request"]);
      appendMessage("system", "High-risk request detected. Confirmation required.");
      return;
    }

    const runId = typeof result.run_id === "string" ? result.run_id : "";
    if (!runId) {
      throw new Error("Broker did not return a Codex run id.");
    }

    state.pendingConfirmation = false;
    state.pendingRequest = null;
    const rewriteIndex = Number.isInteger(message?.rewriteMessageIndex) ? Number(message.rewriteMessageIndex) : null;
    if (rewriteIndex !== null) {
      applyLocalRewritePreview(rewriteIndex, message.prompt);
      clearRewriteTarget();
    }
    state.activeCodexRunId = runId;
    const runBackend = typeof result.backend === "string" ? result.backend : message.backend || "codex";
    const runUi = ensureCodexRunUi(runId, state.sessionId, true, runBackend);
    showRunWaitingIndicator(runUi);
    await refreshHistory(state.sessionId);
    void pollCodexRun(runId, state.sessionId);
  } catch (error) {
    appendMessage("assistant", `Error: ${String(error.message || error)}`);
  } finally {
    setBusy(false);
    updateComposerState();
  }
}

function ensureCodexRunUi(runId, conversationId, allowAssistantBubble, backend = null) {
  let runUi = state.codexRunUi.get(runId);
  if (!runUi) {
    runUi = {
      runId,
      conversationId,
      lastSeq: 0,
      renderedSeqs: new Set(),
      waitingNode: null,
      assistantNode: null,
      thinkingNode: null,
      approvalCards: new Map(),
      allowAssistantBubble,
      backend: typeof backend === "string" ? backend : "codex"
    };
    state.codexRunUi.set(runId, runUi);
    return runUi;
  }
  runUi.conversationId = conversationId;
  runUi.allowAssistantBubble = allowAssistantBubble;
  if (typeof backend === "string" && backend) {
    runUi.backend = backend;
  }
  return runUi;
}

async function pollCodexRun(runId, conversationId) {
  if (state.codexPollingRuns.has(runId)) {
    return;
  }
  state.codexPollingRuns.add(runId);

  try {
    while (state.sessionId === conversationId) {
      const runUi = ensureCodexRunUi(runId, conversationId, true);
      const result = await sendRuntimeMessage({
        type: "assistant.codex.run.events",
        runId,
        after: runUi.lastSeq,
        timeoutMs: 20_000
      });
      if (!result.ok) {
        throw new Error(result.error || "Failed to poll Codex run events.");
      }
      if (state.sessionId !== conversationId) {
        break;
      }

      renderCodexEvents(runId, Array.isArray(result.events) ? result.events : []);
      runUi.lastSeq = getLastRenderedSeq(runUi);

      if (isTerminalRunStatus(result.status)) {
        state.activeCodexRunId = "";
        clearRewriteTarget();
        await refreshHistory(state.sessionId);
        await loadConversation(state.sessionId);
        break;
      }
    }
  } catch (error) {
    const runUi = state.codexRunUi.get(runId);
    if (runUi) {
      clearRunWaitingIndicator(runUi);
    }
    appendMessage("system", `Codex event polling failed: ${String(error.message || error)}`);
    state.activeCodexRunId = "";
  } finally {
    state.codexPollingRuns.delete(runId);
    updateComposerState();
  }
}

function renderCodexEvents(runId, events) {
  const runUi =
    state.codexRunUi.get(runId) || ensureCodexRunUi(runId, state.sessionId, true);
  for (const event of events) {
    const seq = Number(event?.seq || 0);
    if (!seq || runUi.renderedSeqs.has(seq)) {
      continue;
    }
    runUi.renderedSeqs.add(seq);

    if (event.type === "partial_text" || event.type === "partial_answer_text") {
      clearRunWaitingIndicator(runUi);
      if (runUi.allowAssistantBubble) {
        upsertCodexAssistantBubble(runUi, String(event?.data?.text || ""));
      }
      continue;
    }

    if (event.type === "partial_reasoning_text") {
      clearRunWaitingIndicator(runUi);
      const reasoningText = String(event?.data?.text || "");
      if (reasoningText || runUi.thinkingNode) {
        upsertCodexThinkingPanel(runUi, reasoningText, true);
      }
      continue;
    }

    if (event.type === "waiting_approval") {
      renderApprovalCard(runUi, event?.data || {});
      appendCodexStatusCard(describeCodexEvent(event));
      continue;
    }

    if (event.type === "approval_decision" || event.type === "approval_granted") {
      updateApprovalCard(runUi, event?.data || {}, event.message || "");
      appendCodexStatusCard(describeCodexEvent(event));
      continue;
    }

    if (event.type === "completed" || event.type === "failed" || event.type === "cancelled" || event.type === "blocked_for_review") {
      clearRunWaitingIndicator(runUi);
      const assistantText = String(event?.data?.assistant_text || "");
      const reasoningText = String(event?.data?.reasoning_text || "");
      const finalAssistantText = assistantText || getMessageText(runUi.assistantNode);
      const finalReasoningText = reasoningText || getMessageText(runUi.thinkingNode);
      const finalReasoningBlocks = splitReasoningText(finalReasoningText);
      if (runUi.allowAssistantBubble && (finalAssistantText || runUi.assistantNode || finalReasoningBlocks.length)) {
        upsertCodexAssistantBubble(
          runUi,
          finalAssistantText,
          false,
          finalReasoningBlocks
        );
        runUi.allowAssistantBubble = false;
      }
      if (runUi.thinkingNode) {
        runUi.thinkingNode.remove();
        runUi.thinkingNode = null;
      }
      disableApprovalCards(runUi);
      if (runUi.backend === "codex" && event.type === "completed" && !assistantText && !reasoningText) {
        continue;
      }
      appendCodexStatusCard(describeCodexEvent(event));
      continue;
    }

    if (event.type === "thinking") {
      if (runUi.thinkingNode) {
        upsertCodexThinkingPanel(runUi, getMessageText(runUi.thinkingNode), true);
      }
      continue;
    }

    if (event.type === "tool_result" || event.type === "calling_tool" || event.type === "thinking" || event.type === "cancel_requested") {
      appendCodexStatusCard(describeCodexEvent(event));
      continue;
    }

    appendCodexStatusCard(describeCodexEvent(event));
  }
}

function describeCodexEvent(event) {
  const type = String(event?.type || "codex");
  const status = String(event?.status || type);
  const data = event?.data && typeof event.data === "object" ? event.data : {};
  const toolName = typeof data.tool_name === "string" ? data.tool_name : "";
  const timestamp = formatTraceTime(event?.created_at);
  const base = {
    status,
    timestamp,
    detail: toolName ? `Tool: ${toolName}` : ""
  };

  switch (type) {
    case "thinking":
      return { ...base, label: "Thinking", text: event.message || "Codex run started." };
    case "calling_tool":
      return { ...base, label: "Browser Action", text: event.message || "Running a browser action." };
    case "tool_result":
      return {
        ...base,
        label: data.success === false ? "Tool Error" : "Tool Result",
        text: event.message || "Browser action returned a result."
      };
    case "waiting_approval":
      return { ...base, label: "Approval Needed", text: event.message || "Approval required before continuing." };
    case "approval_decision":
      return { ...base, label: "Approval Updated", text: event.message || "Approval decision recorded." };
    case "approval_granted":
      return { ...base, label: "Approval Granted", text: event.message || "Approval granted." };
    case "completed":
      return { ...base, label: "Completed", text: event.message || "Codex run completed." };
    case "failed":
      return { ...base, label: "Failed", text: event.message || "Codex run failed." };
    case "cancel_requested":
      return { ...base, label: "Cancel Requested", text: event.message || "Cancellation requested." };
    case "cancelled":
      return { ...base, label: "Cancelled", text: event.message || "Codex run cancelled." };
    case "blocked_for_review":
      return { ...base, label: "Blocked", text: event.message || "Codex run blocked for review." };
    default:
      return { ...base, label: "Codex", text: event.message || type || "Codex event" };
  }
}

function renderApprovalCard(runUi, approval) {
  const approvalId = typeof approval?.approval_id === "string" ? approval.approval_id : "";
  if (!approvalId || runUi.approvalCards.has(approvalId)) {
    return;
  }

  if (emptyStateEl) {
    emptyStateEl.classList.add("hidden");
  }

  const card = document.createElement("div");
  card.className = "message system codex-event codex-approval";
  card.dataset.runId = runUi.runId;
  card.dataset.approvalId = approvalId;

  const title = document.createElement("div");
  title.className = "codex-card-title";
  title.textContent = "Approval Required";

  const summary = document.createElement("div");
  summary.className = "codex-card-summary";
  summary.textContent = approval.summary || "Browser action requires approval.";

  card.appendChild(title);
  card.appendChild(summary);

  if (approval.host) {
    const host = document.createElement("div");
    host.className = "codex-card-detail";
    host.textContent = `Host: ${approval.host}`;
    card.appendChild(host);
  }
  if (approval.selector) {
    const selector = document.createElement("div");
    selector.className = "codex-card-detail";
    selector.textContent = `Selector: ${approval.selector}`;
    card.appendChild(selector);
  }
  if (approval.text_preview) {
    const preview = document.createElement("div");
    preview.className = "codex-card-detail";
    preview.textContent = `Text: ${approval.text_preview}`;
    card.appendChild(preview);
  }

  const status = document.createElement("div");
  status.className = "codex-card-detail codex-card-status";
  status.textContent = "Waiting for your decision.";
  card.appendChild(status);

  const buttonRow = document.createElement("div");
  buttonRow.className = "button-row codex-card-actions";

  const approveBtn = document.createElement("button");
  approveBtn.textContent = "Approve";
  approveBtn.addEventListener("click", async () => {
    await submitApprovalDecision(runUi, approvalId, "approve", card);
  });

  const denyBtn = document.createElement("button");
  denyBtn.className = "ghost";
  denyBtn.textContent = "Deny";
  denyBtn.addEventListener("click", async () => {
    await submitApprovalDecision(runUi, approvalId, "deny", card);
  });

  const cancelRunBtn = document.createElement("button");
  cancelRunBtn.className = "ghost";
  cancelRunBtn.textContent = "Cancel Run";
  cancelRunBtn.addEventListener("click", async () => {
    await cancelCodexRun(runUi.runId, card);
  });

  buttonRow.appendChild(approveBtn);
  buttonRow.appendChild(denyBtn);
  buttonRow.appendChild(cancelRunBtn);
  card.appendChild(buttonRow);

  messagesEl.appendChild(card);
  messagesEl.scrollTop = messagesEl.scrollHeight;
  runUi.approvalCards.set(approvalId, card);
}

function updateApprovalCard(runUi, data, message) {
  const approvalId = typeof data?.approval_id === "string" ? data.approval_id : "";
  const card = approvalId ? runUi.approvalCards.get(approvalId) : null;
  if (!card) {
    return;
  }
  const status = card.querySelector(".codex-card-status");
  if (status) {
    status.textContent = message || "Approval updated.";
  }
  for (const button of card.querySelectorAll("button")) {
    button.disabled = true;
  }
}

function disableApprovalCards(runUi) {
  for (const card of runUi.approvalCards.values()) {
    const status = card.querySelector(".codex-card-status");
    if (status && !status.textContent) {
      status.textContent = "Run finished.";
    }
    for (const button of card.querySelectorAll("button")) {
      button.disabled = true;
    }
  }
}

async function submitApprovalDecision(runUi, approvalId, decision, card) {
  try {
    setApprovalCardBusy(card, true, decision === "approve" ? "Submitting approval..." : "Submitting denial...");
    const result = await sendRuntimeMessage({
      type: "assistant.codex.run.approval",
      runId: runUi.runId,
      approvalId,
      decision
    });
    if (!result.ok) {
      throw new Error(result.error || "Approval request failed.");
    }
    updateApprovalCard(runUi, { approval_id: approvalId }, decision === "approve" ? "Approved." : "Denied.");
  } catch (error) {
    setApprovalCardBusy(card, false, `Approval failed: ${String(error.message || error)}`);
  }
}

async function cancelCodexRun(runId, card = null) {
  try {
    if (card) {
      setApprovalCardBusy(card, true, "Canceling run...");
    }
    const result = await sendRuntimeMessage({
      type: "assistant.codex.run.cancel",
      runId
    });
    if (!result.ok) {
      throw new Error(result.error || "Run cancel failed.");
    }
    state.activeCodexRunId = "";
    if (card) {
      setApprovalCardBusy(card, true, "Run canceled.");
    }
  } catch (error) {
    if (card) {
      setApprovalCardBusy(card, false, `Cancel failed: ${String(error.message || error)}`);
    } else {
      appendMessage("system", `Run cancel failed: ${String(error.message || error)}`);
    }
  } finally {
    updateComposerState();
  }
}

function setApprovalCardBusy(card, disabled, statusText) {
  const status = card?.querySelector(".codex-card-status");
  if (status && statusText) {
    status.textContent = statusText;
  }
  if (!card) {
    return;
  }
  for (const button of card.querySelectorAll("button")) {
    button.disabled = disabled;
  }
}

function upsertCodexAssistantBubble(runUi, text, pending = true, reasoningBlocks = null) {
  if (!text && !(Array.isArray(reasoningBlocks) && reasoningBlocks.length)) {
    return;
  }
  if (!runUi.assistantNode) {
    runUi.assistantNode = appendMessage(
      "assistant",
      text,
      pending,
      "codex-assistant",
      null,
      reasoningBlocks
    );
    return;
  }
  updateMessage(runUi.assistantNode, "assistant", text, pending, "codex-assistant", reasoningBlocks);
}

function showRunWaitingIndicator(runUi) {
  if (runUi.waitingNode) {
    return;
  }
  runUi.waitingNode = appendMessage("assistant", "", true, "run-waiting");
  const body = runUi.waitingNode.querySelector(".message-body") || document.createElement("div");
  if (!body.parentNode) {
    body.className = "message-body";
    runUi.waitingNode.appendChild(body);
  }
  body.textContent = "";

  const label = document.createElement("span");
  label.className = "run-waiting-label";
  label.textContent = "Waiting";
  body.appendChild(label);

  const dots = document.createElement("span");
  dots.className = "run-waiting-dots";
  dots.setAttribute("aria-label", "Loading");
  dots.setAttribute("role", "status");
  for (let index = 0; index < 3; index += 1) {
    const dot = document.createElement("span");
    dot.className = "run-waiting-dot";
    dot.textContent = ".";
    dots.appendChild(dot);
  }
  body.appendChild(dots);
}

function clearRunWaitingIndicator(runUi) {
  if (!runUi?.waitingNode) {
    return;
  }
  runUi.waitingNode.remove();
  runUi.waitingNode = null;
}

function upsertCodexThinkingPanel(runUi, text, pending = true) {
  const trace = String(text || "");
  if (!trace.trim() && !pending && !runUi.thinkingNode) {
    return;
  }
  if (!runUi.thinkingNode) {
    runUi.thinkingNode = appendMessage("assistant", "", pending, "codex-thinking");
  }
  runUi.thinkingNode.className = `message assistant codex-thinking${pending ? " pending" : ""}`;
  runUi.thinkingNode.dataset.rawText = trace;
  const body = runUi.thinkingNode.querySelector(".message-body") || document.createElement("div");
  if (!body.parentNode) {
    body.className = "message-body";
    runUi.thinkingNode.appendChild(body);
  }

  const existing = body.querySelector("details.thinking-disclosure");
  const wasOpen = Boolean(existing?.open);
  body.textContent = "";

  const details = document.createElement("details");
  details.className = "thinking-disclosure";
  details.open = wasOpen;

  const summary = document.createElement("summary");
  summary.textContent = pending ? "Thinking..." : "Thought";
  details.appendChild(summary);

  const content = document.createElement("div");
  content.className = "reasoning-content";
  if (trace.trim()) {
    content.appendChild(renderMarkdownFragment(trace));
  }
  details.appendChild(content);
  body.appendChild(details);
  messagesEl.scrollTop = messagesEl.scrollHeight;
}

function splitReasoningText(text) {
  return String(text || "")
    .split(/\n{2,}/)
    .map((part) => part.trim())
    .filter(Boolean);
}

function appendCodexStatusCard(eventSummary) {
  if (!eventSummary?.text) {
    return null;
  }
  if (emptyStateEl) {
    emptyStateEl.classList.add("hidden");
  }
  const card = document.createElement("div");
  card.className = `message system codex-event status-${String(eventSummary.status || "codex").replace(/\s+/g, "-")}`;

  const head = document.createElement("div");
  head.className = "codex-status-head";

  const label = document.createElement("div");
  label.className = "codex-status-label";
  label.textContent = eventSummary.label || "Codex";
  head.appendChild(label);

  if (eventSummary.timestamp) {
    const time = document.createElement("div");
    time.className = "codex-status-time";
    time.textContent = eventSummary.timestamp;
    head.appendChild(time);
  }

  const body = document.createElement("div");
  body.className = "codex-status-text";
  body.textContent = eventSummary.text;

  card.appendChild(head);
  card.appendChild(body);

  if (eventSummary.detail) {
    const detail = document.createElement("div");
    detail.className = "codex-status-detail";
    detail.textContent = eventSummary.detail;
    card.appendChild(detail);
  }

  messagesEl.appendChild(card);
  messagesEl.scrollTop = messagesEl.scrollHeight;
  return card;
}

function getLastRenderedSeq(runUi) {
  let maxSeq = 0;
  for (const seq of runUi.renderedSeqs) {
    if (seq > maxSeq) {
      maxSeq = seq;
    }
  }
  return maxSeq;
}

function usesCodexRunProtocol() {
  return true;
}

function isTerminalRunStatus(status) {
  return ["completed", "failed", "cancelled", "blocked_for_review"].includes(String(status || ""));
}

function setBusy(busy) {
  state.busy = busy;
  updateComposerState();
}

function updateComposerState() {
  const locked = state.busy || Boolean(state.activeCodexRunId);
  const hasActiveCodex = Boolean(state.activeCodexRunId);
  const hasActiveLegacy = Boolean(state.activeLegacyRequestId);
  const showStop = hasActiveCodex || hasActiveLegacy;
  const browserActionSupported = ["codex", "llama", "mlx"].includes(String(backendEl.value || ""));
  sendBtn.disabled = locked;
  promptEl.disabled = locked;
  confirmBtn.disabled = state.busy;
  if (forceBrowserActionEl) {
    if (!browserActionSupported && forceBrowserActionEl.checked) {
      forceBrowserActionEl.checked = false;
    }
    forceBrowserActionEl.disabled = locked || !browserActionSupported;
  }
  stopBtn.classList.toggle("hidden", !showStop);
  stopBtn.disabled = !showStop || state.stopping;
  stopBtn.textContent = state.stopping ? "Stopping..." : hasActiveCodex ? "Stop Run" : "Stop";
  if (state.busy) {
    sendBtn.textContent = "Sending...";
  } else if (state.activeCodexRunId) {
    sendBtn.textContent = "Codex Running";
  } else if (hasRewriteTarget()) {
    sendBtn.textContent = "Resend Edit";
  } else {
    sendBtn.textContent = "Send";
  }

  const disableEditButtons = locked || state.pendingConfirmation;
  for (const button of messagesEl.querySelectorAll(".message-edit-btn")) {
    button.disabled = disableEditButtons;
  }
}

function showRiskConfirm(flags) {
  riskText.textContent = `High-risk action detected (${flags.join(", ")}). Confirm to continue.`;
  confirmWrap.classList.remove("hidden");
}

function hideRiskConfirm() {
  confirmWrap.classList.add("hidden");
}

function toggleHistoryPanel() {
  if (state.historyPinned) {
    state.historyPinned = false;
    setHistoryPanel(false);
    return;
  }
  setHistoryPanel(!state.historyOpen);
}

function closeHistoryPanel() {
  if (state.historyPinned) {
    state.historyPinned = false;
  }
  setHistoryPanel(false);
}

function toggleHistoryPin() {
  state.historyPinned = !state.historyPinned;
  if (state.historyPinned) {
    state.historyOpen = true;
  }
  syncHistoryPanelState();
}

function setHistoryPanel(open) {
  state.historyOpen = open;
  if (!open) {
    state.historyPinned = false;
  }
  syncHistoryPanelState();
}

function syncHistoryPanelState() {
  const visible = state.historyOpen || state.historyPinned;
  appEl?.classList.toggle("history-open", visible);
  appEl?.classList.toggle("history-pinned", state.historyPinned);
  historyPanelEl?.setAttribute("aria-hidden", String(!visible));
  historyBackdropEl?.setAttribute("aria-hidden", String(!visible || state.historyPinned));
  historyToggleBtn?.setAttribute("aria-expanded", String(visible));
  historyPinBtn?.setAttribute("aria-pressed", String(state.historyPinned));
  historyPinBtn?.setAttribute("title", state.historyPinned ? "Unpin history" : "Pin history");
}

function hasRewriteTarget() {
  return Number.isInteger(state.rewriteTargetIndex) && Number(state.rewriteTargetIndex) >= 0;
}

function clearRewriteTarget(skipComposerUpdate = false) {
  state.rewriteTargetIndex = null;
  syncRewriteTargetHighlight();
  if (!skipComposerUpdate) {
    updateComposerState();
  }
}

function startRewriteFromMessage(messageIndex, text) {
  if (!Number.isInteger(messageIndex) || messageIndex < 0) {
    return;
  }
  if (state.busy || state.activeCodexRunId || state.activeLegacyRequestId) {
    appendMessage("system", "Finish the active request before editing a prior prompt.");
    return;
  }
  state.pendingConfirmation = false;
  state.pendingRequest = null;
  hideRiskConfirm();
  state.rewriteTargetIndex = messageIndex;
  promptEl.value = String(text || "");
  promptEl.focus({ preventScroll: true });
  promptEl.setSelectionRange(promptEl.value.length, promptEl.value.length);
  syncRewriteTargetHighlight();
  updateComposerState();
}

function syncRewriteTargetHighlight() {
  const activeIndex = hasRewriteTarget() ? Number(state.rewriteTargetIndex) : -1;
  for (const node of messagesEl.querySelectorAll(".message[data-message-index]")) {
    const nodeIndex = Number(node.dataset.messageIndex);
    node.classList.toggle("rewrite-target", Number.isInteger(nodeIndex) && nodeIndex === activeIndex);
  }
}

function applyLocalRewritePreview(messageIndex, prompt) {
  const indexedMessages = [...messagesEl.querySelectorAll(".message[data-message-index]")];
  const targetNode = indexedMessages.find((node) => Number(node.dataset.messageIndex) === messageIndex) || null;

  if (targetNode) {
    let cursor = targetNode;
    while (cursor) {
      const next = cursor.nextSibling;
      cursor.remove();
      cursor = next;
    }
  } else {
    clearMessages();
  }

  state.codexRunUi = new Map();
  appendMessage("user", prompt, false, "", messageIndex);
}

function renderConversationMessages(messages) {
  clearMessages();
  const normalizedMessages = Array.isArray(messages) ? messages : [];
  normalizedMessages.forEach((message, index) => {
    if (message.role === "user" || message.role === "assistant") {
      appendMessage(
        message.role,
        String(message.content || ""),
        false,
        "",
        index,
        message.reasoning_blocks || message.reasoningBlocks
      );
    }
  });
  if (!normalizedMessages.length) {
    appendMessage("system", "Conversation is empty.");
  }
  syncRewriteTargetHighlight();
}

function formatTime(raw) {
  if (!raw) {
    return "unknown";
  }
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) {
    return "unknown";
  }
  return date.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit"
  });
}

function formatTraceTime(raw) {
  if (!raw) {
    return "";
  }
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return date.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit"
  });
}

function appendMessage(
  role,
  text,
  pending = false,
  extraClass = "",
  messageIndex = null,
  reasoningBlocks = null
) {
  if (emptyStateEl) {
    emptyStateEl.classList.add("hidden");
  }
  const item = document.createElement("div");
  const body = document.createElement("div");
  body.className = "message-body";
  item.appendChild(body);
  if (Number.isInteger(messageIndex)) {
    item.dataset.messageIndex = String(messageIndex);
  }
  updateMessage(item, role, text, pending, extraClass, reasoningBlocks);
  if (role === "user" && Number.isInteger(messageIndex) && !pending) {
    attachUserMessageActions(item, messageIndex);
  }
  messagesEl.appendChild(item);
  messagesEl.scrollTop = messagesEl.scrollHeight;
  return item;
}

function collapseThinkBlocks(text) {
  const raw = String(text || "");
  if (!raw) {
    return { visible: "", hiddenChars: 0, reasoningBlocks: [] };
  }

  let source = raw;
  let visible = "";
  let hiddenChars = 0;
  const reasoningBlocks = [];
  const openTagPattern = /<(?:think|thinking)\b[^>]*>/i;
  const closeTagPattern = /<\/(?:think|thinking)\b[^>]*>/i;

  while (source.length > 0) {
    const openMatch = openTagPattern.exec(source);
    if (!openMatch) {
      visible += source;
      break;
    }

    const openStart = openMatch.index;
    visible += source.slice(0, openStart);
    const openEnd = openStart + openMatch[0].length;
    if (openEnd <= openStart) {
      break;
    }

    const remaining = source.slice(openEnd);
    const closeMatch = closeTagPattern.exec(remaining);
    if (!closeMatch) {
      // If a reasoning block is not closed, keep it hidden instead of rendering it.
      hiddenChars += Math.max(0, source.length - openStart);
      source = source.slice(0, openStart);
      break;
    }

    const closeStart = openEnd + closeMatch.index;
    const closeEnd = closeStart + closeMatch[0].length;
    const reasoningText = source.slice(openEnd, closeStart).trim();
    if (reasoningText) {
      reasoningBlocks.push(reasoningText);
    }
    hiddenChars += Math.max(0, closeStart - openEnd);
    source = source.slice(closeEnd);
  }

  visible = visible.replace(/<\/(?:think|thinking)\b[^>]*>/gi, "");
  visible = visible.replace(/\n{3,}/g, "\n\n").trim();
  return { visible, hiddenChars, reasoningBlocks };
}

function createReasoningDisclosure(reasoningBlocks) {
  const blocks = Array.isArray(reasoningBlocks)
    ? reasoningBlocks.map((block) => String(block || "").trim()).filter(Boolean)
    : [];
  if (!blocks.length) {
    return null;
  }

  const details = document.createElement("details");
  details.className = "reasoning-disclosure";

  const summary = document.createElement("summary");
  const totalChars = blocks.reduce((sum, block) => sum + block.length, 0);
  summary.textContent = `Reasoning (${blocks.length} block${blocks.length === 1 ? "" : "s"}, ${totalChars} chars)`;
  details.appendChild(summary);

  const content = document.createElement("div");
  content.className = "reasoning-content";
  const combined = blocks
    .map((block, index) => (blocks.length > 1 ? `Block ${index + 1}\n\n${block}` : block))
    .join("\n\n");
  content.appendChild(renderMarkdownFragment(combined));
  details.appendChild(content);
  return details;
}

function attachUserMessageActions(node, messageIndex) {
  const actions = document.createElement("div");
  actions.className = "message-actions";

  const button = document.createElement("button");
  button.type = "button";
  button.className = "ghost message-edit-btn";
  button.setAttribute("aria-label", "Edit prompt");
  button.title = "Edit prompt";
  button.textContent = "✎";
  button.addEventListener("click", () => {
    startRewriteFromMessage(messageIndex, getMessageText(node));
  });

  actions.appendChild(button);
  node.appendChild(actions);
}

function updateMessage(node, role, text, pending = false, extraClass = "", reasoningBlocks = null) {
  node.className = `message ${role}${pending ? " pending" : ""}${extraClass ? ` ${extraClass}` : ""}`;
  const rawText = String(text || "");
  node.dataset.rawText = rawText;
  const body = node.querySelector(".message-body") || document.createElement("div");
  if (!body.parentNode) {
    body.className = "message-body";
    node.appendChild(body);
  }
  let displayText = rawText;
  let reasoningDisclosure = null;
  if (role === "assistant") {
    const explicitReasoning = Array.isArray(reasoningBlocks)
      ? reasoningBlocks.map((block) => String(block || "").trim()).filter(Boolean)
      : [];
    if (explicitReasoning.length) {
      displayText = rawText;
      reasoningDisclosure = createReasoningDisclosure(explicitReasoning);
    } else {
      const collapsed = collapseThinkBlocks(rawText);
      displayText = collapsed.visible;
      reasoningDisclosure = createReasoningDisclosure(collapsed.reasoningBlocks);
    }
  }
  body.textContent = "";
  if (displayText.trim()) {
    body.appendChild(renderMarkdownFragment(displayText));
  } else if (role === "assistant" && reasoningDisclosure) {
    const note = document.createElement("p");
    note.className = "reasoning-note";
    note.textContent = "No final answer text. Expand reasoning below.";
    body.appendChild(note);
  }
  if (role === "assistant" && reasoningDisclosure) {
    body.appendChild(reasoningDisclosure);
  }
  messagesEl.scrollTop = messagesEl.scrollHeight;
}

function clearMessages() {
  messagesEl.textContent = "";
  if (emptyStateEl) {
    emptyStateEl.classList.remove("hidden");
    messagesEl.appendChild(emptyStateEl);
  }
}

function renderMarkdownFragment(source) {
  const text = String(source || "").replace(/\r\n?/g, "\n");
  const lines = text.split("\n");
  const fragment = document.createDocumentFragment();
  let index = 0;

  while (index < lines.length) {
    const line = lines[index];

    if (!line.trim()) {
      index += 1;
      continue;
    }

    const fenceMatch = line.match(/^```([\w-]+)?\s*$/);
    if (fenceMatch) {
      index += 1;
      const codeLines = [];
      while (index < lines.length && !/^```\s*$/.test(lines[index])) {
        codeLines.push(lines[index]);
        index += 1;
      }
      if (index < lines.length) {
        index += 1;
      }
      const pre = document.createElement("pre");
      const code = document.createElement("code");
      if (fenceMatch[1]) {
        code.dataset.lang = fenceMatch[1];
      }
      code.textContent = codeLines.join("\n");
      pre.appendChild(code);
      fragment.appendChild(pre);
      continue;
    }

    if (/^\s*>\s?/.test(line)) {
      const quoteLines = [];
      while (index < lines.length && /^\s*>\s?/.test(lines[index])) {
        quoteLines.push(lines[index].replace(/^\s*>\s?/, ""));
        index += 1;
      }
      const blockquote = document.createElement("blockquote");
      appendParagraphBlocks(blockquote, quoteLines.join("\n"));
      fragment.appendChild(blockquote);
      continue;
    }

    if (/^\s*[-*]\s+/.test(line)) {
      const list = document.createElement("ul");
      while (index < lines.length && /^\s*[-*]\s+/.test(lines[index])) {
        const item = document.createElement("li");
        appendInlineContent(item, lines[index].replace(/^\s*[-*]\s+/, ""));
        list.appendChild(item);
        index += 1;
      }
      fragment.appendChild(list);
      continue;
    }

    if (/^\s*\d+\.\s+/.test(line)) {
      const list = document.createElement("ol");
      while (index < lines.length && /^\s*\d+\.\s+/.test(lines[index])) {
        const item = document.createElement("li");
        appendInlineContent(item, lines[index].replace(/^\s*\d+\.\s+/, ""));
        list.appendChild(item);
        index += 1;
      }
      fragment.appendChild(list);
      continue;
    }

    const paragraphLines = [];
    while (
      index < lines.length &&
      lines[index].trim() &&
      !/^```/.test(lines[index]) &&
      !/^\s*>\s?/.test(lines[index]) &&
      !/^\s*[-*]\s+/.test(lines[index]) &&
      !/^\s*\d+\.\s+/.test(lines[index])
    ) {
      paragraphLines.push(lines[index]);
      index += 1;
    }
    appendParagraphBlocks(fragment, paragraphLines.join("\n"));
  }

  if (!fragment.childNodes.length) {
    fragment.appendChild(document.createTextNode(""));
  }
  return fragment;
}

function appendParagraphBlocks(container, text) {
  const chunks = text.split(/\n{2,}/).filter((chunk) => chunk.trim());
  for (const chunk of chunks) {
    const paragraph = document.createElement("p");
    appendInlineContent(paragraph, chunk);
    container.appendChild(paragraph);
  }
}

function appendInlineContent(container, text, depth = 0) {
  if (!text) {
    return;
  }
  if (depth > 8) {
    appendTextWithBreaks(container, text);
    return;
  }

  const nextToken = findNextInlineToken(text);
  if (!nextToken) {
    appendTextWithBreaks(container, text);
    return;
  }

  if (nextToken.index > 0) {
    appendTextWithBreaks(container, text.slice(0, nextToken.index));
  }

  const matchedText = text.slice(nextToken.index, nextToken.index + nextToken.length);
  switch (nextToken.type) {
    case "code": {
      const code = document.createElement("code");
      code.textContent = nextToken.content;
      container.appendChild(code);
      break;
    }
    case "link": {
      const href = normalizeLink(nextToken.url);
      if (href) {
        const link = document.createElement("a");
        link.href = href;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        appendInlineContent(link, nextToken.label, depth + 1);
        container.appendChild(link);
      } else {
        appendTextWithBreaks(container, matchedText);
      }
      break;
    }
    case "strong": {
      const strong = document.createElement("strong");
      appendInlineContent(strong, nextToken.content, depth + 1);
      container.appendChild(strong);
      break;
    }
    case "em": {
      const em = document.createElement("em");
      appendInlineContent(em, nextToken.content, depth + 1);
      container.appendChild(em);
      break;
    }
    case "url": {
      const href = normalizeLink(nextToken.url);
      if (href) {
        const link = document.createElement("a");
        link.href = href;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.textContent = nextToken.url;
        container.appendChild(link);
      } else {
        appendTextWithBreaks(container, matchedText);
      }
      break;
    }
    default:
      appendTextWithBreaks(container, matchedText);
      break;
  }

  const remaining = text.slice(nextToken.index + nextToken.length);
  if (remaining) {
    appendInlineContent(container, remaining, depth + 1);
  }
}

function appendTextWithBreaks(container, text) {
  const parts = String(text || "").split("\n");
  parts.forEach((part, index) => {
    if (index > 0) {
      container.appendChild(document.createElement("br"));
    }
    if (part) {
      container.appendChild(document.createTextNode(part));
    }
  });
}

function findNextInlineToken(text) {
  const patterns = [
    {
      type: "code",
      regex: /`([^`\n]+)`/g,
      build: (match) => ({ content: match[1] })
    },
    {
      type: "link",
      regex: /\[([^\]\n]+)\]\((https?:\/\/[^\s)]+)\)/g,
      build: (match) => ({ label: match[1], url: match[2] })
    },
    {
      type: "strong",
      regex: /\*\*([^*\n][\s\S]*?)\*\*/g,
      build: (match) => ({ content: match[1] })
    },
    {
      type: "em",
      regex: /\*([^*\n]+)\*/g,
      build: (match) => ({ content: match[1] })
    },
    {
      type: "url",
      regex: /https?:\/\/[^\s<]+[^\s<.,:;"')\]]/g,
      build: (match) => ({ url: match[0] })
    }
  ];

  let next = null;
  for (const pattern of patterns) {
    pattern.regex.lastIndex = 0;
    const match = pattern.regex.exec(text);
    if (!match) {
      continue;
    }
    const candidate = {
      type: pattern.type,
      index: match.index,
      length: match[0].length,
      ...pattern.build(match)
    };
    if (!next || candidate.index < next.index) {
      next = candidate;
    }
  }
  return next;
}

function normalizeLink(rawUrl) {
  try {
    const parsed = new URL(String(rawUrl || "").trim());
    if (!SAFE_LINK_PROTOCOLS.has(parsed.protocol)) {
      return "";
    }
    return parsed.toString();
  } catch {
    return "";
  }
}

function getMessageText(node) {
  return node?.dataset?.rawText || "";
}

function sendRuntimeMessage(message) {
  return new Promise((resolve, reject) => {
    chrome.runtime.sendMessage(message, (response) => {
      const lastError = chrome.runtime.lastError;
      if (lastError) {
        reject(new Error(lastError.message));
        return;
      }
      if (!response) {
        reject(new Error("No response from background worker."));
        return;
      }
      resolve(response);
    });
  });
}
