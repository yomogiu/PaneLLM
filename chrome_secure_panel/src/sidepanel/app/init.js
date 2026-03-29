async function initializeApp() {
  setHistoryPanel(false);
  setMainTab("chat");
  setPaperTab("chat");
  renderPaperWorkspace();
  renderBrowserCurrentPage();
  renderBrowserPickerPanel();
  await restoreBrowserProfileState();
  renderBrowserProfilePanel({ forceNameSync: true });
  renderBrowserAutomationPanel();
  renderBrowserNextActionIndicator();
  await refreshBrokerHealth();
  await refreshBackendState(false);
  await refreshToolsState(false);
  await refreshHistory(state.sessionId);
  appendMessage("system", "Started a new chat. Conversations are saved on the local broker.");
  updateComposerState();
}

function queueActiveTabRefresh() {
  if (state.pollTimers.activeTabRefresh) {
    window.clearTimeout(state.pollTimers.activeTabRefresh);
  }
  state.pollTimers.activeTabRefresh = window.setTimeout(() => {
    state.pollTimers.activeTabRefresh = 0;
    void refreshToolsState(false);
  }, 150);
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

async function refreshPaperMemory(force = false) {
  const paper = getEffectivePaper();
  const version = getPaperMemoryVersion(paper);
  const query = String(paperMemorySearchEl?.value ?? state.paperMemoryQuery ?? "").trim();
  const requestKey = paper ? `${getPaperKey(paper)}:${version}:${query}` : "";

  if (!paper) {
    resetPaperMemoryState();
    renderPaperMemoryPanel();
    return;
  }
  if (!version) {
    state.paperMemoryResults = [];
    state.paperMemoryQuery = query;
    state.paperMemoryVersion = "";
    state.paperMemoryLoading = false;
    state.paperMemoryError = "";
    state.paperMemoryRequestKey = requestKey;
    renderPaperMemoryPanel();
    return;
  }
  if (!force && requestKey && requestKey === state.paperMemoryRequestKey && !state.paperMemoryError) {
    renderPaperMemoryPanel();
    return;
  }

  state.paperMemoryLoading = true;
  state.paperMemoryError = "";
  state.paperMemoryResults = [];
  state.paperMemoryQuery = query;
  state.paperMemoryVersion = version;
  state.paperMemoryRequestKey = requestKey;
  renderPaperMemoryPanel();

  const requestPaper = {
    ...paper,
    paper_version: version,
    versioned_url: paper.versioned_url || (paper.paper_id ? `https://arxiv.org/abs/${paper.paper_id}${version}` : "")
  };

  try {
    const result = await sendRuntimeMessage({
      type: "assistant.paper.memory_query",
      paper: requestPaper,
      query,
      limit: 8,
      excludeConversationId: state.sessionId
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to load paper memory.");
    }
    if (state.paperMemoryRequestKey !== requestKey) {
      return;
    }
    state.paperMemoryVersion = normalizePaperVersionLabel(result.memory_version || version);
    state.paperMemoryResults = Array.isArray(result.results)
      ? result.results.map((item) => normalizePaperMemoryResultPayload(item)).filter(Boolean)
      : [];
  } catch (error) {
    if (state.paperMemoryRequestKey !== requestKey) {
      return;
    }
    state.paperMemoryError = `Paper memory load failed: ${String(error.message || error)}`;
  } finally {
    if (state.paperMemoryRequestKey === requestKey) {
      state.paperMemoryLoading = false;
      renderPaperMemoryPanel();
    }
  }
}

function setMainTab(tab) {
  const next = tab === "browser" ? tab : "chat";
  state.activeMainTab = next;

  const tabConfig = [
    { key: "chat", button: chatTabBtn, view: chatViewEl },
    { key: "browser", button: browserTabBtn, view: browserViewEl }
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
  if (next === "browser") {
    void refreshToolsState(false);
  }
}

function renderBackendOptions(selectEl, backends = [], current = "codex") {
  if (!selectEl) {
    return;
  }
  selectEl.textContent = "";
  const options = Array.isArray(backends) && backends.length
    ? backends
    : [
        { id: "codex", label: "Codex", available: true },
        { id: "llama", label: "llama.cpp", available: true },
        { id: "mlx", label: "MLX Local", available: true }
      ];
  for (const backend of options) {
    const id = String(backend?.id || "").trim();
    if (!id) {
      continue;
    }
    const option = document.createElement("option");
    option.value = id;
    option.disabled = backend?.available === false;
    option.textContent = option.disabled
      ? `${String(backend?.label || id)} (unavailable)`
      : String(backend?.label || id);
    selectEl.appendChild(option);
  }
  const availableOptions = [...selectEl.options];
  const activeOption = availableOptions.find((option) => option.value === current && !option.disabled);
  const firstAvailable = availableOptions.find((option) => !option.disabled);
  if (activeOption) {
    selectEl.value = current;
  } else if (firstAvailable) {
    selectEl.value = firstAvailable.value;
  } else if (availableOptions.length > 0) {
    selectEl.value = availableOptions[0].value;
  }
}

function renderAvailableBackends(backends = []) {
  const current = String(backendEl?.value || "codex");
  state.availableBackends = Array.isArray(backends) ? backends : [];
  renderBackendOptions(backendEl, state.availableBackends, current);
  updateComposerState();
}

async function refreshBackendState(showErrors = true) {
  try {
    const result = await sendRuntimeMessage({ type: "assistant.models.get" });
    if (!result.ok) {
      throw new Error(result.error || "Failed to load backend metadata.");
    }
    renderAvailableBackends(Array.isArray(result.backends) ? result.backends : []);
  } catch (error) {
    renderAvailableBackends(state.availableBackends);
    if (showErrors) {
      appendMessage("system", `Backend metadata load failed: ${String(error.message || error)}`);
    }
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
    effective_allowed_hosts: normalizeHostArray(raw.effective_allowed_hosts)
  };
}

function normalizeBrowserConfig(config) {
  const raw = config && typeof config === "object" ? config : {};
  const limitsRaw = raw.limits && typeof raw.limits === "object" ? raw.limits : {};
  const stepLimitsRaw =
    limitsRaw.agent_max_steps && typeof limitsRaw.agent_max_steps === "object"
      ? limitsRaw.agent_max_steps
      : {};
  const min = Number.isInteger(Number(stepLimitsRaw.min)) && Number(stepLimitsRaw.min) >= 1
    ? Number(stepLimitsRaw.min)
    : 1;
  const parsedMax = Number(stepLimitsRaw.max);
  const max = Number.isInteger(parsedMax) && parsedMax > 0 ? parsedMax : null;
  const configured = Number.parseInt(String(raw.agent_max_steps ?? 0), 10);
  const agentMaxSteps = Number.isInteger(configured) && configured >= 0 ? configured : 0;
  return {
    agent_max_steps: agentMaxSteps,
    limits: {
      agent_max_steps: {
        min,
        max
      }
    }
  };
}


function initializeSidepanelApp() {
  initializeThemeControls();
  initializeFontScaleControls();
  void initializeApp();
}

initializeSidepanelApp();
