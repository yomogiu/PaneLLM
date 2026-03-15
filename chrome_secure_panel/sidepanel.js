const $ = (id) => document.getElementById(id);
const SAFE_LINK_PROTOCOLS = new Set(["http:", "https:"]);
const ACTIVE_JOB_STATUSES = new Set(["queued", "running"]);
const JOB_POLL_INTERVAL_MS = 4_000;
const DETAIL_ITEM_LIMIT = 3;
const EXPLAIN_SELECTION_DEFAULT_PROMPT = "Explain the selected passage in plain language.";
const SHOW_ME_WHERE_FOLLOWUP =
  "After answering, use browser tools to scroll to and temporarily highlight the section of the current page that best answers the request above.";
const TRAINING_BALANCED_PROFILE = Object.freeze({
  rank: 8,
  scale: 20,
  dropout: 0,
  num_layers: 8,
  learning_rate: 0.00001,
  iters: 600,
  batch_size: 1,
  grad_accumulation_steps: 4,
  steps_per_report: 10,
  steps_per_eval: 100,
  save_every: 100,
  val_batches: 25,
  max_seq_length: 2048,
  grad_checkpoint: true,
  seed: 0
});

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
  composerExplainSelection: "",
  composerShowMeWhere: false,
  activeMainTab: "chat",
  modelsBusy: false,
  toolsBusy: false,
  toolsPolicy: null,
  toolsActiveTab: null,
  toolsBrowserConfig: null,
  readContext: null,
  experimentJobs: [],
  experiments: [],
  experimentDetail: null,
  experimentComparison: null,
  trainingDatasets: [],
  trainingJobs: [],
  trainingRuns: [],
  trainingRunDetail: null,
  activeMlxAdapterPath: "",
  mlxRuntime: null,
  pollTimers: {
    papers: 0,
    experiments: 0
  }
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
const llamaThinkingControlsEl = $("llama-thinking-controls");
const llamaThinkingToggleEl = $("llama-thinking-toggle");
const llamaEnableThinkingEl = $("llama-enable-thinking");
const llamaThinkingStateEl = $("llama-thinking-state");
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
const trainingRefreshBtn = $("training-refresh-btn");
const trainingStatusEl = $("training-status");
const trainingDatasetPathEl = $("training-dataset-path");
const trainingDatasetNameEl = $("training-dataset-name");
const trainingDatasetImportBtn = $("training-dataset-import-btn");
const trainingDatasetListEl = $("training-dataset-list");
const trainingDatasetSelectEl = $("training-dataset-select");
const trainingRunNameEl = $("training-run-name");
const trainingModelPathEl = $("training-model-path");
const trainingPresetEl = $("training-preset");
const trainingRankEl = $("training-rank");
const trainingScaleEl = $("training-scale");
const trainingDropoutEl = $("training-dropout");
const trainingNumLayersEl = $("training-num-layers");
const trainingLearningRateEl = $("training-learning-rate");
const trainingItersEl = $("training-iters");
const trainingBatchSizeEl = $("training-batch-size");
const trainingGradAccumulationEl = $("training-grad-accumulation");
const trainingStepsReportEl = $("training-steps-report");
const trainingStepsEvalEl = $("training-steps-eval");
const trainingSaveEveryEl = $("training-save-every");
const trainingValBatchesEl = $("training-val-batches");
const trainingMaxSeqLengthEl = $("training-max-seq-length");
const trainingSeedEl = $("training-seed");
const trainingGradCheckpointEl = $("training-grad-checkpoint");
const trainingStartBtn = $("training-start-btn");
const trainingStopStartBtn = $("training-stop-start-btn");
const trainingJobListEl = $("training-job-list");
const trainingRunListEl = $("training-run-list");
const trainingRunOutputEl = $("training-run-output");
const mlxLatencyTrendEl = $("mlx-latency-trend");
const mlxTpsTrendEl = $("mlx-tps-trend");
const mlxRestartTrendEl = $("mlx-restart-trend");
const mlxContractEl = $("mlx-contract");
const experimentsRefreshBtn = $("experiments-refresh-btn");
const experimentsStatusEl = $("experiments-status");
const experimentPromptsEl = $("experiment-prompts");
const experimentReferencesEl = $("experiment-references");
const experimentRunBtn = $("experiment-run-btn");
const experimentAdapterRunBtn = $("experiment-adapter-run-btn");
const experimentJobListEl = $("experiment-job-list");
const experimentListEl = $("experiment-list");
const experimentCompareOutputEl = $("experiment-compare-output");
const toolsRefreshBtn = $("tools-refresh-btn");
const toolsPolicyStatusEl = $("tools-policy-status");
const toolsHostInputEl = $("tools-host-input");
const toolsAllowBtn = $("tools-allow-btn");
const toolsAllowActiveBtn = $("tools-allow-active-btn");
const toolsAgentMaxStepsEl = $("tools-agent-max-steps");
const toolsBrowserApplyBtn = $("tools-browser-apply-btn");
const toolsAllowedListEl = $("tools-allowed-list");
const papersStatusEl = $("read-assistant-status");
const paperSourceInputEl = null;
const paperUseActiveBtn = $("read-context-refresh-btn");
const paperInspectBtn = $("read-explain-btn");
const paperAnalyzeBtn = $("read-guide-btn");
const readShowWhereBtn = $("read-show-btn");
const paperInspectOutputEl = $("read-assistant-preview");
const paperJobListEl = null;
const paperListEl = null;

function renderReadAssistantPreview(context) {
  if (!paperInspectOutputEl) {
    return;
  }
  if (!context || typeof context !== "object") {
    paperInspectOutputEl.textContent = "No active page context.";
    return;
  }
  const lines = [];
  if (context.title) {
    lines.push(`Title: ${String(context.title)}`);
  }
  if (context.url) {
    lines.push(`URL: ${String(context.url)}`);
  }
  if (Array.isArray(context.heading_path) && context.heading_path.length) {
    lines.push(`Section: ${context.heading_path.join(" > ")}`);
  }
  if (context.selection) {
    lines.push(`Selection:\n${String(context.selection)}`);
  }
  const local = context.selection_context && typeof context.selection_context === "object"
    ? context.selection_context
    : null;
  if (local?.focus) {
    const parts = [local.before, local.focus, local.after].filter(Boolean);
    lines.push(`Local context:\n${parts.join("\n")}`);
  } else if (context.text_excerpt) {
    lines.push(`Page excerpt:\n${truncatePreview(String(context.text_excerpt), 360)}`);
  }
  paperInspectOutputEl.textContent = lines.join("\n\n") || "No active page context.";
}

function setReadAssistantExplainEnabled() {
  if (paperInspectBtn) {
    paperInspectBtn.disabled = state.toolsBusy;
  }
  const composerExplainBtn = $("composer-read-explain-btn");
  if (composerExplainBtn) {
    composerExplainBtn.disabled = state.busy || state.toolsBusy;
  }
}

function getComposerExplainSelectionBlock(selection = state.composerExplainSelection) {
  const raw = String(selection || "").trim();
  if (!raw) {
    return "";
  }
  const quoted = raw
    .split("\n")
    .map((line) => `> ${line}`.trimEnd())
    .join("\n");
  return `Selected passage:\n${quoted}`;
}

function syncReadAssistantQuickActionState() {
  const explainBtn = $("composer-read-explain-btn");
  const showBtn = $("composer-read-show-btn");
  explainBtn?.classList.toggle("active", Boolean(state.composerExplainSelection));
  explainBtn?.setAttribute("aria-pressed", String(Boolean(state.composerExplainSelection)));
  showBtn?.classList.toggle("active", state.composerShowMeWhere);
  showBtn?.setAttribute("aria-pressed", String(state.composerShowMeWhere));
}

function clearComposerExplainSelection() {
  state.composerExplainSelection = "";
}

function clearComposerShowMeWhere() {
  state.composerShowMeWhere = false;
}

function resetComposerReadAssistantModes() {
  clearComposerExplainSelection();
  clearComposerShowMeWhere();
  syncReadAssistantQuickActionState();
}

function focusComposerToEnd() {
  if (!promptEl) {
    return;
  }
  promptEl.focus({ preventScroll: true });
  promptEl.setSelectionRange(promptEl.value.length, promptEl.value.length);
}

function armComposerExplainSelection(selection) {
  const cleanedSelection = String(selection || "").trim();
  if (!cleanedSelection) {
    return;
  }
  state.composerExplainSelection = cleanedSelection;
  includePageContextEl.checked = true;
  syncReadAssistantQuickActionState();
  focusComposerToEnd();
}

async function toggleComposerExplainSelectionMode() {
  if (state.composerExplainSelection) {
    clearComposerExplainSelection();
    syncReadAssistantQuickActionState();
    updatePapersStatus("Selection will no longer be included with your next message.");
    updateComposerState();
    focusComposerToEnd();
    return;
  }
  setToolsBusy(true);
  try {
    const context = await captureReadAssistantContext(false);
    if (!context?.selection) {
      updatePapersStatus("Select text on the current page first.");
      return;
    }
    armComposerExplainSelection(context.selection);
    updatePapersStatus("Selection will be included with your next message.");
    updateComposerState();
  } catch (error) {
    updatePapersStatus(`Read assistant error: ${String(error.message || error)}`);
  } finally {
    setToolsBusy(false);
  }
}

function toggleComposerShowMeWhereMode() {
  state.composerShowMeWhere = !state.composerShowMeWhere;
  if (state.composerShowMeWhere) {
    includePageContextEl.checked = true;
    updatePapersStatus("Show Me Where is armed. Send a prompt to answer it and navigate to the best section.");
    focusComposerToEnd();
  } else {
    updatePapersStatus("Show Me Where disabled.");
  }
  syncReadAssistantQuickActionState();
  updateComposerState();
}

function buildComposerPromptForSubmit(rawPrompt) {
  let prompt = String(rawPrompt || "").trim();
  if (!prompt && state.composerExplainSelection) {
    prompt = EXPLAIN_SELECTION_DEFAULT_PROMPT;
  }
  if (!prompt) {
    return "";
  }
  return prompt;
}

function buildComposerPromptSuffix() {
  const parts = [];
  if (state.composerExplainSelection) {
    parts.push(getComposerExplainSelectionBlock());
  }
  if (state.composerShowMeWhere) {
    parts.push(SHOW_ME_WHERE_FOLLOWUP);
  }
  return parts.join("\n\n").trim();
}

function buildReadAssistantPrompt(kind, context) {
  const draft = String(promptEl?.value || "").trim();
  const selection = String(context?.selection || "").trim();
  const selectionSuffix = selection
    ? `\n\nSelected passage:\n${selection}`
    : "";
  if (kind === "explain_selection") {
    return [
      "Explain the selected passage from the current page in plain language.",
      "Use the surrounding section context to resolve symbols or references.",
      "Be precise, concise, and grounded in the page only.",
      'Finish with: "Why it matters: ..."',
      selectionSuffix
    ].join("\n").trim();
  }
  if (kind === "show_me_where") {
    const question = draft
      || (selection
        ? `Show me where the current page addresses this selected passage:\n${selection}`
        : "Show me where the current page explains the main claim.");
    return [
      question,
      "",
      SHOW_ME_WHERE_FOLLOWUP
    ].join("\n").trim();
  }
  return [
    "Act as a reading guide for the current page.",
    "Give me:",
    "1. what this page is about,",
    "2. what section to read next,",
    "3. one thing that is easy to misunderstand,",
    "4. one useful follow-up question to ask.",
    draft ? `\n\nFocus request:\n${draft}` : ""
  ].join("\n").trim();
}

function updateReadAssistantStatus() {
  if (!state.toolsActiveTab?.host) {
    updatePapersStatus("Allowlist loaded. Active tab unavailable.");
    return;
  }
  if (!state.toolsActiveTab.allowed) {
    updatePapersStatus("Allow the current host to use the read assistant.");
    return;
  }
  if (!state.readContext) {
    updatePapersStatus(`Read assistant ready for ${state.toolsActiveTab.host}.`);
    return;
  }
  if (!state.readContext.selection) {
    updatePapersStatus(`Read assistant ready for ${state.toolsActiveTab.host}. No active text selection.`);
    return;
  }
  updatePapersStatus(`Read assistant ready for ${state.toolsActiveTab.host}. Selection captured.`);
}

async function captureReadAssistantContext(showErrors = true) {
  try {
    const result = await sendRuntimeMessage({ type: "assistant.read.context.capture" });
    state.readContext = result.ok ? result.context || null : null;
    if (result.active_tab && typeof result.active_tab === "object") {
      state.toolsActiveTab = result.active_tab;
    }
    renderReadAssistantPreview(state.readContext);
    setReadAssistantExplainEnabled();
    updateReadAssistantStatus();
    if (!result.ok) {
      throw new Error(
        typeof result.error === "string"
          ? result.error
          : result.error?.message || "Unable to capture the current page context."
      );
    }
    return state.readContext;
  } catch (error) {
    state.readContext = null;
    renderReadAssistantPreview(null);
    setReadAssistantExplainEnabled();
    if (showErrors) {
      updatePapersStatus(`Read assistant error: ${String(error.message || error)}`);
    }
    throw error;
  }
}

async function submitReadAssistantAction(kind) {
  setToolsBusy(true);
  try {
    const context = await captureReadAssistantContext(false);
    if (kind === "explain_selection" && !context?.selection) {
      updatePapersStatus("Select text on the current page first.");
      return;
    }
    if (!promptEl || !sendBtn) {
      throw new Error("Chat composer is unavailable.");
    }
    includePageContextEl.checked = true;
    forceBrowserActionEl.checked = kind === "show_me_where";
    promptEl.value = buildReadAssistantPrompt(kind, context);
    updateComposerState();
    sendBtn.click();
  } catch (error) {
    updatePapersStatus(`Read assistant error: ${String(error.message || error)}`);
  } finally {
    setToolsBusy(false);
  }
}

async function showReadAssistantTarget() {
  await submitReadAssistantAction("show_me_where");
}

function installReadAssistantQuickActions() {
  if (!promptEl || document.getElementById("read-assistant-quick-actions")) {
    return;
  }
  const row = document.createElement("div");
  row.id = "read-assistant-quick-actions";
  row.className = "button-row";

  const explainBtn = document.createElement("button");
  explainBtn.id = "composer-read-explain-btn";
  explainBtn.type = "button";
  explainBtn.className = "ghost composer-quick-action";
  explainBtn.textContent = "Explain Selection";
  explainBtn.setAttribute("aria-pressed", "false");
  explainBtn.addEventListener("click", async () => {
    await toggleComposerExplainSelectionMode();
  });

  const guideBtn = document.createElement("button");
  guideBtn.id = "composer-read-guide-btn";
  guideBtn.type = "button";
  guideBtn.className = "ghost composer-quick-action";
  guideBtn.textContent = "Guide This Page";
  guideBtn.addEventListener("click", async () => {
    await submitReadAssistantAction("guide_page");
  });

  const showBtn = document.createElement("button");
  showBtn.id = "composer-read-show-btn";
  showBtn.type = "button";
  showBtn.className = "ghost composer-quick-action";
  showBtn.textContent = "Show Me Where";
  showBtn.setAttribute("aria-pressed", "false");
  showBtn.addEventListener("click", () => {
    toggleComposerShowMeWhereMode();
  });

  row.appendChild(explainBtn);
  row.appendChild(guideBtn);
  row.appendChild(showBtn);
  promptEl.insertAdjacentElement("afterend", row);
  syncReadAssistantQuickActionState();
}

installReadAssistantQuickActions();
void initializeApp();

function truncatePreview(value, maxChars = 240) {
  const text = String(value || "").trim().replace(/\s+/g, " ");
  if (!text) {
    return "";
  }
  return text.length > maxChars ? `${text.slice(0, Math.max(0, maxChars - 1)).trimEnd()}…` : text;
}

function hasActiveJobs(jobs) {
  return Array.isArray(jobs) && jobs.some((job) => ACTIVE_JOB_STATUSES.has(String(job?.status || "")));
}

function clearAutoRefresh(kind) {
  const timerId = Number(state.pollTimers?.[kind] || 0);
  if (timerId > 0) {
    window.clearTimeout(timerId);
  }
  if (state.pollTimers) {
    state.pollTimers[kind] = 0;
  }
}

function scheduleAutoRefresh(kind, enabled) {
  clearAutoRefresh(kind);
  if (!enabled) {
    return;
  }
  state.pollTimers[kind] = window.setTimeout(async () => {
    state.pollTimers[kind] = 0;
    if (kind === "papers") {
      if (state.toolsBusy) {
        scheduleAutoRefresh(kind, true);
        return;
      }
      await refreshToolsState(false);
      return;
    }
    if (state.modelsBusy) {
      scheduleAutoRefresh(kind, true);
      return;
    }
    await refreshModelsState(false);
  }, JOB_POLL_INTERVAL_MS);
}

function formatMetricLabel(key) {
  return String(key || "")
    .replace(/_/g, " ")
    .replace(/\bms\b/gi, "ms")
    .replace(/\bid\b/gi, "ID")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function formatMetricValue(value) {
  if (typeof value === "number") {
    return Number.isInteger(value) ? String(value) : String(value);
  }
  if (value === null || value === undefined || value === "") {
    return "n/a";
  }
  return String(value);
}

function summarizeSummary(summary, maxItems = 3) {
  const entries = summary && typeof summary === "object" ? Object.entries(summary) : [];
  return entries
    .slice(0, maxItems)
    .map(([key, value]) => `${formatMetricLabel(key)}: ${formatMetricValue(value)}`)
    .join(" · ");
}

function renderPaperDetail(paper) {
  if (!paperInspectOutputEl) {
    return;
  }
  const artifact = paper && typeof paper === "object" ? paper : {};
  const lines = [];
  const title = String(artifact.title || artifact.paper_id || "paper").trim();
  lines.push(`Paper: ${title}`);
  if (artifact.paper_id) {
    lines.push(`Artifact ID: ${String(artifact.paper_id)}`);
  }
  const source = String(artifact.url || artifact.local_path || "").trim();
  if (source) {
    lines.push(`Source: ${source}`);
  }
  const authors = Array.isArray(artifact.authors)
    ? artifact.authors.map((value) => String(value || "").trim()).filter(Boolean)
    : [];
  if (authors.length) {
    lines.push(`Authors: ${authors.join(", ")}`);
  }
  lines.push(
    `Sections: ${Number(artifact.section_count || (Array.isArray(artifact.sections) ? artifact.sections.length : 0) || 0)}`
  );

  const latestDigest =
    artifact.latest_digest && typeof artifact.latest_digest === "object"
      ? String(artifact.latest_digest.text || "").trim()
      : "";
  if (latestDigest) {
    lines.push("");
    lines.push("Digest:");
    lines.push(truncatePreview(latestDigest, 1200));
  } else if (artifact.abstract) {
    lines.push("");
    lines.push("Abstract:");
    lines.push(truncatePreview(artifact.abstract, 900));
  }

  const sections = Array.isArray(artifact.sections) ? artifact.sections : [];
  if (sections.length) {
    lines.push("");
    lines.push("Sections:");
    for (const section of sections.slice(0, 6)) {
      const heading = String(section?.heading || section?.section_id || "Section").trim();
      const sectionId = String(section?.section_id || "").trim();
      const preview = truncatePreview(section?.preview || section?.text || "", 180);
      lines.push(`${sectionId ? `${sectionId} · ` : ""}${heading}`);
      if (preview) {
        lines.push(`  ${preview}`);
      }
    }
    if (sections.length > 6) {
      lines.push(`+${sections.length - 6} more section(s)`);
    }
  }
  paperInspectOutputEl.textContent = lines.join("\n");
}

function renderExperimentDetail(experiment) {
  if (!experimentCompareOutputEl) {
    return;
  }
  const artifact = experiment && typeof experiment === "object" ? experiment : {};
  const lines = [
    `Experiment: ${String(artifact.experiment_id || "experiment")}`,
    `Kind: ${String(artifact.kind || "unknown")}`,
    `Prompts: ${Number(artifact.prompt_count || 0)}`
  ];
  const summary = artifact.summary && typeof artifact.summary === "object" ? artifact.summary : {};
  const summaryEntries = Object.entries(summary);
  if (summaryEntries.length) {
    lines.push("");
    lines.push("Summary:");
    for (const [key, value] of summaryEntries) {
      lines.push(`${formatMetricLabel(key)}: ${formatMetricValue(value)}`);
    }
  }

  const items = Array.isArray(artifact.items) ? artifact.items : [];
  if (items.length) {
    lines.push("");
    lines.push("Samples:");
    for (const item of items.slice(0, DETAIL_ITEM_LIMIT)) {
      lines.push(`${String(item?.id || "item")} · ${truncatePreview(item?.prompt || "", 140)}`);
      if (artifact.kind === "adapter_eval") {
        lines.push(`  Base: ${truncatePreview(item?.base?.output || "", 220)}`);
        lines.push(`  Adapter: ${truncatePreview(item?.adapter?.output || "", 220)}`);
      } else {
        lines.push(`  Output: ${truncatePreview(item?.output || "", 220)}`);
      }
      if (item?.reference) {
        lines.push(`  Reference: ${truncatePreview(item.reference, 180)}`);
      }
    }
    if (items.length > DETAIL_ITEM_LIMIT) {
      lines.push(`+${items.length - DETAIL_ITEM_LIMIT} more sample(s)`);
    }
  }
  experimentCompareOutputEl.textContent = lines.join("\n");
}

function describeMlxBackendAvailability(mlx, label = "MLX") {
  const runtime = mlx && typeof mlx === "object" ? mlx : {};
  if (!runtime.available) {
    return `${label} unavailable. Configure BROKER_MLX_MODEL_PATH on the broker.`;
  }
  if (String(runtime.status || "") === "failed") {
    const errorText = truncatePreview(runtime.last_error || "worker startup failed", 180);
    return `${label} startup failed: ${errorText}.`;
  }
  return "";
}

function describeExperimentAvailability(mlx) {
  const message = describeMlxBackendAvailability(mlx, "MLX experiments");
  return message ? `${message} Experiment jobs will keep failing until runtime startup is fixed.` : "";
}

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

function isLlamaChatBackend(backend = backendEl?.value) {
  return String(backend || "").trim().toLowerCase() === "llama";
}

function buildLlamaThinkingRequestFields(backend = backendEl?.value) {
  if (!isLlamaChatBackend(backend)) {
    return {};
  }
  const enabled = Boolean(llamaEnableThinkingEl?.checked);
  return {
    chatTemplateKwargs: {
      enable_thinking: enabled,
      clear_thinking: false
    },
    reasoningBudget: enabled ? -1 : 0
  };
}

function applyLlamaThinkingRequestFields(message, backend = message?.backend) {
  if (!message || !isLlamaChatBackend(backend)) {
    return message;
  }
  const llamaFields = buildLlamaThinkingRequestFields(backend);
  return {
    ...message,
    ...llamaFields
  };
}

function updateLlamaThinkingComposer() {
  const isLlama = isLlamaChatBackend();
  const enabled = isLlama && Boolean(llamaEnableThinkingEl?.checked);
  if (llamaThinkingControlsEl) {
    llamaThinkingControlsEl.classList.toggle("hidden", !isLlama);
  }
  if (llamaThinkingToggleEl) {
    llamaThinkingToggleEl.classList.toggle("active", enabled);
  }
  if (llamaThinkingStateEl) {
    llamaThinkingStateEl.textContent = enabled ? "On · budget -1" : "Off · budget 0";
  }
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

llamaEnableThinkingEl?.addEventListener("change", () => {
  updateLlamaThinkingComposer();
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

trainingRefreshBtn?.addEventListener("click", async () => {
  await refreshModelsState(true);
});

trainingPresetEl?.addEventListener("change", () => {
  applyTrainingPreset();
});

trainingDatasetImportBtn?.addEventListener("click", async () => {
  await importTrainingDatasetFromInputs();
});

trainingStartBtn?.addEventListener("click", async () => {
  await startTrainingJobFromInputs(false);
});

trainingStopStartBtn?.addEventListener("click", async () => {
  await startTrainingJobFromInputs(true);
});

experimentsRefreshBtn?.addEventListener("click", async () => {
  await refreshModelsState(true);
});

experimentRunBtn?.addEventListener("click", async () => {
  await runExperimentJob("prompt_eval");
});

experimentAdapterRunBtn?.addEventListener("click", async () => {
  await runExperimentJob("adapter_eval");
});

toolsRefreshBtn?.addEventListener("click", async () => {
  await refreshToolsState(true);
});

toolsAllowBtn?.addEventListener("click", async () => {
  await allowHostFromInput();
});

toolsAllowActiveBtn?.addEventListener("click", async () => {
  await allowActiveTabHost();
});

toolsBrowserApplyBtn?.addEventListener("click", async () => {
  await applyBrowserConfigFromInputs();
});

paperUseActiveBtn?.addEventListener("click", async () => {
  await useActiveTabForPaperSource();
});

paperInspectBtn?.addEventListener("click", async () => {
  await inspectPaperFromInputs();
});

paperAnalyzeBtn?.addEventListener("click", async () => {
  await analyzePaperFromInputs();
});

readShowWhereBtn?.addEventListener("click", async () => {
  await showReadAssistantTarget();
});

toolsHostInputEl?.addEventListener("keydown", async (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    await allowHostFromInput();
  }
});

paperSourceInputEl?.addEventListener("keydown", async (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    await inspectPaperFromInputs();
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
    mlxUnloadAdapterBtn,
    trainingRefreshBtn,
    trainingDatasetPathEl,
    trainingDatasetNameEl,
    trainingDatasetImportBtn,
    trainingDatasetSelectEl,
    trainingRunNameEl,
    trainingModelPathEl,
    trainingPresetEl,
    trainingRankEl,
    trainingScaleEl,
    trainingDropoutEl,
    trainingNumLayersEl,
    trainingLearningRateEl,
    trainingItersEl,
    trainingBatchSizeEl,
    trainingGradAccumulationEl,
    trainingStepsReportEl,
    trainingStepsEvalEl,
    trainingSaveEveryEl,
    trainingValBatchesEl,
    trainingMaxSeqLengthEl,
    trainingSeedEl,
    trainingGradCheckpointEl,
    trainingStartBtn,
    trainingStopStartBtn,
    experimentsRefreshBtn,
    experimentRunBtn,
    experimentAdapterRunBtn
  ];
  for (const control of controls) {
    if (control) {
      control.disabled = busy;
    }
  }
  if (mlxEnableThinkingEl) {
    mlxEnableThinkingEl.disabled = busy;
  }
  for (const button of experimentJobListEl?.querySelectorAll("button") || []) {
    button.disabled = busy;
  }
  for (const button of experimentListEl?.querySelectorAll("button") || []) {
    button.disabled = busy;
  }
  for (const button of trainingDatasetListEl?.querySelectorAll("button") || []) {
    button.disabled = busy;
  }
  for (const button of trainingJobListEl?.querySelectorAll("button") || []) {
    button.disabled = busy;
  }
  for (const button of trainingRunListEl?.querySelectorAll("button") || []) {
    button.disabled = busy;
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

function renderBackendOptions(selectEl, backends = [], current = "mlx") {
  if (!selectEl) {
    return;
  }
  selectEl.textContent = "";
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
    selectEl.value = selectEl.options[0].value;
  }
}

function renderModelsBackends(backends = []) {
  const current = String(modelsBackendEl?.value || backendEl?.value || "mlx");
  renderBackendOptions(modelsBackendEl, backends, current);
  renderBackendOptions(backendEl, backends, current);
  const resolved = String(modelsBackendEl?.value || backendEl?.value || current);
  if (modelsBackendEl) {
    modelsBackendEl.value = resolved;
  }
  if (backendEl) {
    backendEl.value = resolved;
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
  state.activeMlxAdapterPath = String(payload?.active_adapter?.path || "");
  mlxAdapterListEl.classList.add("structured");
  mlxAdapterListEl.textContent = "";
  if (!adapters.length) {
    mlxAdapterListEl.textContent = "No adapters registered.";
    mlxAdapterListEl.classList.remove("structured");
    return;
  }
  for (const adapter of adapters) {
    const adapterId = String(adapter?.id || "");
    const row = document.createElement("div");
    row.className = "tools-item";
    const meta = document.createElement("div");
    meta.className = "tools-item-meta";
    const title = document.createElement("p");
    title.className = "tools-item-host";
    title.textContent = String(adapter?.name || adapter?.id || "adapter");
    meta.appendChild(title);
    const detail = document.createElement("p");
    detail.className = "tools-muted";
    const source = String(adapter?.source_type || "imported");
    const runId = String(adapter?.run_id || "");
    const step = Number(adapter?.step || 0);
    const validationLoss = adapter?.validation_loss ?? null;
    detail.textContent =
      `${source}${runId ? ` · ${runId}` : ""}${step > 0 ? ` · step ${step}` : ""}${validationLoss !== null ? ` · val ${validationLoss}` : ""}`;
    meta.appendChild(detail);
    const pathText = document.createElement("p");
    pathText.className = "tools-muted";
    pathText.textContent = String(adapter?.path || "");
    meta.appendChild(pathText);
    const tags = document.createElement("div");
    tags.className = "tools-item-tags";
    for (const tagText of [adapterId === activeId ? "active" : "", String(adapter?.checkpoint_kind || ""), String(adapter?.promoted ? "saved" : "")].filter(Boolean)) {
      const chip = document.createElement("span");
      chip.className = "tools-chip";
      chip.textContent = tagText;
      tags.appendChild(chip);
    }
    meta.appendChild(tags);
    row.appendChild(meta);
    const actions = document.createElement("div");
    actions.className = "tools-item-actions";
    if (adapterId !== activeId) {
      const loadBtn = document.createElement("button");
      loadBtn.className = "ghost small";
      loadBtn.textContent = "Load";
      loadBtn.addEventListener("click", async () => {
        await loadMlxAdapterById(adapterId);
      });
      actions.appendChild(loadBtn);
    }
    row.appendChild(actions);
    mlxAdapterListEl.appendChild(row);
  }
}

function setTrainingStatus(text) {
  if (trainingStatusEl) {
    trainingStatusEl.textContent = text;
  }
}

function applyTrainingPreset() {
  const preset = String(trainingPresetEl?.value || "balanced");
  if (preset !== "balanced") {
    return;
  }
  fillTrainingConfigInputs(TRAINING_BALANCED_PROFILE);
}

function fillTrainingConfigInputs(config) {
  const training = config && typeof config === "object" ? config : TRAINING_BALANCED_PROFILE;
  if (trainingRankEl) {
    trainingRankEl.value = String(training.rank ?? TRAINING_BALANCED_PROFILE.rank);
  }
  if (trainingScaleEl) {
    trainingScaleEl.value = String(training.scale ?? TRAINING_BALANCED_PROFILE.scale);
  }
  if (trainingDropoutEl) {
    trainingDropoutEl.value = String(training.dropout ?? TRAINING_BALANCED_PROFILE.dropout);
  }
  if (trainingNumLayersEl) {
    trainingNumLayersEl.value = String(training.num_layers ?? TRAINING_BALANCED_PROFILE.num_layers);
  }
  if (trainingLearningRateEl) {
    trainingLearningRateEl.value = String(training.learning_rate ?? TRAINING_BALANCED_PROFILE.learning_rate);
  }
  if (trainingItersEl) {
    trainingItersEl.value = String(training.iters ?? TRAINING_BALANCED_PROFILE.iters);
  }
  if (trainingBatchSizeEl) {
    trainingBatchSizeEl.value = String(training.batch_size ?? TRAINING_BALANCED_PROFILE.batch_size);
  }
  if (trainingGradAccumulationEl) {
    trainingGradAccumulationEl.value = String(
      training.grad_accumulation_steps ?? TRAINING_BALANCED_PROFILE.grad_accumulation_steps
    );
  }
  if (trainingStepsReportEl) {
    trainingStepsReportEl.value = String(training.steps_per_report ?? TRAINING_BALANCED_PROFILE.steps_per_report);
  }
  if (trainingStepsEvalEl) {
    trainingStepsEvalEl.value = String(training.steps_per_eval ?? TRAINING_BALANCED_PROFILE.steps_per_eval);
  }
  if (trainingSaveEveryEl) {
    trainingSaveEveryEl.value = String(training.save_every ?? TRAINING_BALANCED_PROFILE.save_every);
  }
  if (trainingValBatchesEl) {
    trainingValBatchesEl.value = String(training.val_batches ?? TRAINING_BALANCED_PROFILE.val_batches);
  }
  if (trainingMaxSeqLengthEl) {
    trainingMaxSeqLengthEl.value = String(training.max_seq_length ?? TRAINING_BALANCED_PROFILE.max_seq_length);
  }
  if (trainingSeedEl) {
    trainingSeedEl.value = String(training.seed ?? TRAINING_BALANCED_PROFILE.seed);
  }
  if (trainingGradCheckpointEl) {
    trainingGradCheckpointEl.checked = Boolean(
      training.grad_checkpoint ?? TRAINING_BALANCED_PROFILE.grad_checkpoint
    );
  }
}

function readTrainingConfigInputs() {
  return {
    rank: Number(trainingRankEl?.value || TRAINING_BALANCED_PROFILE.rank),
    scale: Number(trainingScaleEl?.value || TRAINING_BALANCED_PROFILE.scale),
    dropout: Number(trainingDropoutEl?.value || TRAINING_BALANCED_PROFILE.dropout),
    num_layers: Number(trainingNumLayersEl?.value || TRAINING_BALANCED_PROFILE.num_layers),
    learning_rate: Number(trainingLearningRateEl?.value || TRAINING_BALANCED_PROFILE.learning_rate),
    iters: Number(trainingItersEl?.value || TRAINING_BALANCED_PROFILE.iters),
    batch_size: Number(trainingBatchSizeEl?.value || TRAINING_BALANCED_PROFILE.batch_size),
    grad_accumulation_steps: Number(
      trainingGradAccumulationEl?.value || TRAINING_BALANCED_PROFILE.grad_accumulation_steps
    ),
    steps_per_report: Number(trainingStepsReportEl?.value || TRAINING_BALANCED_PROFILE.steps_per_report),
    steps_per_eval: Number(trainingStepsEvalEl?.value || TRAINING_BALANCED_PROFILE.steps_per_eval),
    save_every: Number(trainingSaveEveryEl?.value || TRAINING_BALANCED_PROFILE.save_every),
    val_batches: Number(trainingValBatchesEl?.value || TRAINING_BALANCED_PROFILE.val_batches),
    max_seq_length: Number(trainingMaxSeqLengthEl?.value || TRAINING_BALANCED_PROFILE.max_seq_length),
    seed: Number(trainingSeedEl?.value || TRAINING_BALANCED_PROFILE.seed),
    grad_checkpoint: Boolean(trainingGradCheckpointEl?.checked)
  };
}

function formatTrainingProgress(progress) {
  const currentStep = Number(progress?.current_step || 0);
  const totalSteps = Number(progress?.total_steps || 0);
  const percent = Number(progress?.percent || 0);
  const phase = String(progress?.phase || "queued");
  const trainLoss = progress?.latest_train_loss ?? null;
  const validationLoss = progress?.latest_validation_loss ?? null;
  return `${phase} · ${currentStep}/${totalSteps || "?"} · ${percent.toFixed(1)}%${trainLoss !== null ? ` · train ${trainLoss}` : ""}${validationLoss !== null ? ` · val ${validationLoss}` : ""}`;
}

function fillTrainingDatasetSelect(items = []) {
  if (!trainingDatasetSelectEl) {
    return;
  }
  const datasets = Array.isArray(items) ? items : [];
  const current = String(trainingDatasetSelectEl.value || "");
  trainingDatasetSelectEl.textContent = "";
  if (!datasets.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "Import a dataset first";
    trainingDatasetSelectEl.appendChild(option);
    return;
  }
  for (const dataset of datasets) {
    const option = document.createElement("option");
    option.value = String(dataset?.dataset_id || "");
    option.textContent = `${String(dataset?.name || dataset?.dataset_id || "dataset")} (${Number(dataset?.record_counts?.train || 0)} train)`;
    trainingDatasetSelectEl.appendChild(option);
  }
  if (current && [...trainingDatasetSelectEl.options].some((option) => option.value === current)) {
    trainingDatasetSelectEl.value = current;
  }
}

function renderTrainingDatasets(items = []) {
  if (!trainingDatasetListEl) {
    return;
  }
  trainingDatasetListEl.textContent = "";
  const datasets = Array.isArray(items) ? items : [];
  fillTrainingDatasetSelect(datasets);
  if (!datasets.length) {
    const empty = document.createElement("p");
    empty.className = "tools-empty";
    empty.textContent = "No imported training datasets yet.";
    trainingDatasetListEl.appendChild(empty);
    return;
  }
  for (const dataset of datasets) {
    const row = document.createElement("div");
    row.className = "tools-item";
    const meta = document.createElement("div");
    meta.className = "tools-item-meta";
    const title = document.createElement("p");
    title.className = "tools-item-host";
    title.textContent = String(dataset?.name || dataset?.dataset_id || "dataset");
    meta.appendChild(title);
    const detail = document.createElement("p");
    detail.className = "tools-muted";
    detail.textContent = `${String(dataset?.split_mode || "imported")} · train ${Number(dataset?.record_counts?.train || 0)} · valid ${Number(dataset?.record_counts?.valid || 0)}${Number(dataset?.record_counts?.test || 0) > 0 ? ` · test ${Number(dataset?.record_counts?.test || 0)}` : ""}`;
    meta.appendChild(detail);
    const pathText = document.createElement("p");
    pathText.className = "tools-muted";
    pathText.textContent = String(dataset?.source_path || "");
    meta.appendChild(pathText);
    row.appendChild(meta);
    const actions = document.createElement("div");
    actions.className = "tools-item-actions";
    const selectBtn = document.createElement("button");
    selectBtn.className = "ghost small";
    selectBtn.textContent = "Use";
    selectBtn.addEventListener("click", () => {
      if (trainingDatasetSelectEl) {
        trainingDatasetSelectEl.value = String(dataset?.dataset_id || "");
      }
      if (trainingRunNameEl && !String(trainingRunNameEl.value || "").trim()) {
        trainingRunNameEl.value = `${String(dataset?.name || dataset?.dataset_id || "dataset")} LoRA`;
      }
    });
    actions.appendChild(selectBtn);
    const deleteBtn = document.createElement("button");
    deleteBtn.className = "ghost small";
    deleteBtn.textContent = "Delete";
    deleteBtn.addEventListener("click", async () => {
      await deleteTrainingDatasetById(String(dataset?.dataset_id || ""));
    });
    actions.appendChild(deleteBtn);
    row.appendChild(actions);
    trainingDatasetListEl.appendChild(row);
  }
}

function createProgressBar(percent) {
  const wrap = document.createElement("div");
  wrap.className = "models-progress";
  const bar = document.createElement("div");
  bar.className = "models-progress-bar";
  bar.style.width = `${Math.max(0, Math.min(100, Number(percent || 0)))}%`;
  wrap.appendChild(bar);
  return wrap;
}

function renderTrainingJobs(items = []) {
  if (!trainingJobListEl) {
    return;
  }
  trainingJobListEl.textContent = "";
  const jobs = Array.isArray(items) ? items : [];
  if (!jobs.length) {
    const empty = document.createElement("p");
    empty.className = "tools-empty";
    empty.textContent = "No training jobs yet.";
    trainingJobListEl.appendChild(empty);
    return;
  }
  for (const job of jobs) {
    const progress = job?.progress && typeof job.progress === "object" ? job.progress : {};
    const row = document.createElement("div");
    row.className = "tools-item";
    const meta = document.createElement("div");
    meta.className = "tools-item-meta";
    const title = document.createElement("p");
    title.className = "tools-item-host";
    title.textContent = `${String(job?.input_summary?.dataset_name || job?.job_id || "training")} · ${String(job?.status || "unknown")}`;
    meta.appendChild(title);
    const detail = document.createElement("p");
    detail.className = "models-job-meta";
    detail.textContent = formatTrainingProgress(progress);
    meta.appendChild(detail);
    meta.appendChild(createProgressBar(progress?.percent || 0));
    row.appendChild(meta);
    const actions = document.createElement("div");
    actions.className = "tools-item-actions";
    if (job?.result?.run_id) {
      const viewBtn = document.createElement("button");
      viewBtn.className = "ghost small";
      viewBtn.textContent = "View";
      viewBtn.addEventListener("click", async () => {
        await viewTrainingRun(String(job?.result?.run_id || ""));
      });
      actions.appendChild(viewBtn);
    }
    if (!["completed", "failed", "cancelled"].includes(String(job?.status || ""))) {
      const cancelBtn = document.createElement("button");
      cancelBtn.className = "ghost small";
      cancelBtn.textContent = "Cancel";
      cancelBtn.addEventListener("click", async () => {
        await cancelBrokerJob(String(job?.job_id || ""));
      });
      actions.appendChild(cancelBtn);
    }
    row.appendChild(actions);
    trainingJobListEl.appendChild(row);
  }
}

function renderTrainingRunDetail(run) {
  if (!trainingRunOutputEl) {
    return;
  }
  const artifact = run && typeof run === "object" ? run : {};
  const progress = artifact.progress && typeof artifact.progress === "object" ? artifact.progress : {};
  const lines = [
    `Run: ${String(artifact.name || artifact.run_id || "training")}`,
    `Status: ${String(artifact.status || "unknown")}`,
    `Progress: ${formatTrainingProgress(progress)}`
  ];
  const summary = artifact.summary && typeof artifact.summary === "object" ? artifact.summary : {};
  if (Object.keys(summary).length) {
    lines.push(`Summary: ${summarizeSummary(summary, 4)}`);
  }
  const checkpoints = Array.isArray(artifact.checkpoints) ? artifact.checkpoints : [];
  if (checkpoints.length) {
    lines.push("");
    lines.push("Checkpoints:");
    for (const checkpoint of checkpoints.slice(-8)) {
      lines.push(
        `${String(checkpoint?.kind || "checkpoint")} · step ${Number(checkpoint?.step || 0)} · ${String(checkpoint?.path || "")}`
      );
    }
  }
  const recentEvents = Array.isArray(artifact.recent_events) ? artifact.recent_events : [];
  if (recentEvents.length) {
    lines.push("");
    lines.push("Recent Events:");
    for (const event of recentEvents.slice(-6)) {
      lines.push(`${String(event?.event || "event")} · ${String(event?.data?.message || event?.data?.progress?.status_message || "")}`);
    }
  }
  trainingRunOutputEl.textContent = lines.join("\n");
}

function renderTrainingRuns(items = []) {
  if (!trainingRunListEl) {
    return;
  }
  trainingRunListEl.textContent = "";
  const runs = Array.isArray(items) ? items : [];
  if (!runs.length) {
    const empty = document.createElement("p");
    empty.className = "tools-empty";
    empty.textContent = "No saved training runs yet.";
    trainingRunListEl.appendChild(empty);
    if (trainingRunOutputEl && !state.trainingRunDetail) {
      trainingRunOutputEl.textContent = "(view a training run to inspect details)";
    }
    return;
  }
  for (const run of runs) {
    const row = document.createElement("div");
    row.className = "tools-item";
    const meta = document.createElement("div");
    meta.className = "tools-item-meta";
    const title = document.createElement("p");
    title.className = "tools-item-host";
    title.textContent = `${String(run?.name || run?.run_id || "run")} · ${String(run?.status || "unknown")}`;
    meta.appendChild(title);
    const detail = document.createElement("p");
    detail.className = "models-job-meta";
    detail.textContent = formatTrainingProgress(run?.progress || {});
    meta.appendChild(detail);
    row.appendChild(meta);
    const actions = document.createElement("div");
    actions.className = "tools-item-actions";
    const viewBtn = document.createElement("button");
    viewBtn.className = "ghost small";
    viewBtn.textContent = "View";
    viewBtn.addEventListener("click", async () => {
      await viewTrainingRun(String(run?.run_id || ""));
    });
    actions.appendChild(viewBtn);
    const bestCheckpoint = run?.best_checkpoint && typeof run.best_checkpoint === "object"
      ? run.best_checkpoint
      : null;
    const latestCheckpoint = run?.latest_checkpoint && typeof run.latest_checkpoint === "object"
      ? run.latest_checkpoint
      : null;
    if (bestCheckpoint?.path) {
      const loadBtn = document.createElement("button");
      loadBtn.className = "ghost small";
      loadBtn.textContent = "Load Best";
      loadBtn.addEventListener("click", async () => {
        await loadCheckpointAsAdapter(String(run?.name || run?.run_id || "training"), bestCheckpoint);
      });
      actions.appendChild(loadBtn);
      const saveBtn = document.createElement("button");
      saveBtn.className = "ghost small";
      saveBtn.textContent = "Promote";
      saveBtn.addEventListener("click", async () => {
        await promoteTrainingCheckpoint(String(run?.run_id || ""), "best", String(run?.name || ""));
      });
      actions.appendChild(saveBtn);
    }
    if (latestCheckpoint?.path) {
      const resumeBtn = document.createElement("button");
      resumeBtn.className = "ghost small";
      resumeBtn.textContent = "Resume";
      resumeBtn.addEventListener("click", async () => {
        await resumeTrainingRun(String(run?.run_id || ""), "latest");
      });
      actions.appendChild(resumeBtn);
    }
    row.appendChild(actions);
    trainingRunListEl.appendChild(row);
  }
}

async function importTrainingDatasetFromInputs() {
  const path = String(trainingDatasetPathEl?.value || "").trim();
  const name = String(trainingDatasetNameEl?.value || "").trim();
  if (!path) {
    setTrainingStatus("Enter a dataset path first.");
    return;
  }
  setModelsBusy(true);
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.mlx.training.datasets.import",
      path,
      name
    });
    if (!result.ok) {
      throw new Error(result.error || "Dataset import failed.");
    }
    if (trainingDatasetPathEl) {
      trainingDatasetPathEl.value = "";
    }
    if (trainingDatasetNameEl) {
      trainingDatasetNameEl.value = "";
    }
    setTrainingStatus(`Imported dataset ${String(result?.dataset?.name || result?.dataset?.dataset_id || "")}.`);
    await refreshModelsState(false);
  } catch (error) {
    setTrainingStatus(`Training dataset error: ${String(error.message || error)}`);
  } finally {
    setModelsBusy(false);
  }
}

async function deleteTrainingDatasetById(datasetId) {
  if (!datasetId) {
    return;
  }
  setModelsBusy(true);
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.mlx.training.datasets.delete",
      datasetId
    });
    if (!result.ok) {
      throw new Error(result.error || "Dataset delete failed.");
    }
    setTrainingStatus(`Deleted dataset ${datasetId}.`);
    await refreshModelsState(false);
  } catch (error) {
    setTrainingStatus(`Training dataset error: ${String(error.message || error)}`);
  } finally {
    setModelsBusy(false);
  }
}

async function startTrainingJobFromInputs(stopRuntimeFirst) {
  const datasetId = String(trainingDatasetSelectEl?.value || "").trim();
  if (!datasetId) {
    setTrainingStatus("Select a dataset first.");
    return;
  }
  setModelsBusy(true);
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.mlx.training.job.start",
      datasetId,
      name: String(trainingRunNameEl?.value || "").trim(),
      modelPath: String(trainingModelPathEl?.value || mlxModelPathEl?.value || "").trim(),
      trainingConfig: readTrainingConfigInputs(),
      stopRuntimeFirst
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to start training job.");
    }
    if (trainingStopStartBtn) {
      trainingStopStartBtn.classList.add("hidden");
    }
    setTrainingStatus(`Started training job ${String(result?.job?.job_id || "")}.`);
    await refreshModelsState(false);
  } catch (error) {
    const message = String(error.message || error);
    if (trainingStopStartBtn) {
      trainingStopStartBtn.classList.toggle("hidden", !message.includes("Stop MLX and Train"));
    }
    setTrainingStatus(`Training error: ${message}`);
  } finally {
    setModelsBusy(false);
  }
}

async function viewTrainingRun(runId) {
  if (!runId) {
    return;
  }
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.mlx.training.runs.get",
      runId
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to load training run.");
    }
    const run = result.run && typeof result.run === "object" ? result.run : {};
    state.trainingRunDetail = {
      runId: String(run.run_id || runId)
    };
    renderTrainingRunDetail(run);
  } catch (error) {
    if (trainingRunOutputEl) {
      trainingRunOutputEl.textContent = `Training run error: ${String(error.message || error)}`;
    }
  }
}

async function resumeTrainingRun(runId, checkpointKind = "latest") {
  if (!runId) {
    return;
  }
  setModelsBusy(true);
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.mlx.training.job.start",
      resumeRunId: runId,
      resumeCheckpointKind: checkpointKind,
      stopRuntimeFirst: false
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to resume training.");
    }
    setTrainingStatus(`Resumed training from ${runId}.`);
    await refreshModelsState(false);
  } catch (error) {
    const message = String(error.message || error);
    if (trainingStopStartBtn) {
      trainingStopStartBtn.classList.toggle("hidden", !message.includes("Stop MLX and Train"));
    }
    setTrainingStatus(`Training error: ${message}`);
  } finally {
    setModelsBusy(false);
  }
}

async function loadMlxAdapterById(adapterId) {
  if (!adapterId) {
    return;
  }
  setModelsBusy(true);
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.mlx.adapters.load",
      adapterId
    });
    if (!result.ok) {
      throw new Error(result.error || "Adapter load failed.");
    }
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

async function loadCheckpointAsAdapter(runLabel, checkpoint) {
  if (!checkpoint?.path) {
    return;
  }
  setModelsBusy(true);
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.mlx.adapters.load",
      path: String(checkpoint.path || ""),
      name: `${String(runLabel || "training")} ${String(checkpoint.kind || "checkpoint")}`
    });
    if (!result.ok) {
      throw new Error(result.error || "Checkpoint load failed.");
    }
    renderMlxAdapters(result);
    if (mlxRuntimeStatusEl) {
      mlxRuntimeStatusEl.textContent = `Loaded ${String(checkpoint.kind || "checkpoint")} checkpoint.`;
    }
    await refreshModelsState(false);
  } catch (error) {
    if (mlxRuntimeStatusEl) {
      mlxRuntimeStatusEl.textContent = `Checkpoint load error: ${String(error.message || error)}`;
    }
  } finally {
    setModelsBusy(false);
  }
}

async function promoteTrainingCheckpoint(runId, checkpointKind, runLabel) {
  if (!runId || !checkpointKind) {
    return;
  }
  setModelsBusy(true);
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.mlx.training.checkpoint.promote",
      runId,
      checkpointKind,
      name: `${String(runLabel || runId)} ${checkpointKind}`
    });
    if (!result.ok) {
      throw new Error(result.error || "Checkpoint promotion failed.");
    }
    if (mlxRuntimeStatusEl) {
      mlxRuntimeStatusEl.textContent = `Saved ${checkpointKind} checkpoint to adapters.`;
    }
    await refreshModelsState(false);
  } catch (error) {
    if (mlxRuntimeStatusEl) {
      mlxRuntimeStatusEl.textContent = `Checkpoint promotion error: ${String(error.message || error)}`;
    }
  } finally {
    setModelsBusy(false);
  }
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

function buildPromptSetFromInputs() {
  const prompts = String(experimentPromptsEl?.value || "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
  const references = String(experimentReferencesEl?.value || "")
    .split("\n")
    .map((line) => line.trim());
  return prompts.map((prompt, index) => ({
    id: `prompt_${String(index + 1).padStart(2, "0")}`,
    prompt,
    reference: references[index] || ""
  }));
}

function getPaperAnalysisBackend() {
  const backend = String(modelsBackendEl?.value || backendEl?.value || "llama").trim().toLowerCase();
  return backend === "mlx" ? "mlx" : "llama";
}

function summarizePaperInspect(inspect, cachedPaper) {
  if (!paperInspectOutputEl) {
    return;
  }
  if (!inspect || typeof inspect !== "object") {
    paperInspectOutputEl.textContent = "No paper inspected yet.";
    return;
  }
  const lines = [];
  const title = String(inspect.title || "").trim();
  if (title) {
    lines.push(`Title: ${title}`);
  }
  const authors = Array.isArray(inspect.authors) ? inspect.authors.map((value) => String(value || "").trim()).filter(Boolean) : [];
  if (authors.length) {
    lines.push(`Authors: ${authors.join(", ")}`);
  }
  const source = String(inspect.url || inspect.local_path || "").trim();
  if (source) {
    lines.push(`Source: ${source}`);
  }
  if (inspect.abstract) {
    lines.push("");
    lines.push(`Abstract:\n${String(inspect.abstract).trim()}`);
  } else if (inspect.preview_text) {
    lines.push("");
    lines.push(`Preview:\n${String(inspect.preview_text).trim()}`);
  }
  if (cachedPaper?.paper_id) {
    lines.push("");
    lines.push(`Cached artifact: ${cachedPaper.paper_id}`);
  }
  paperInspectOutputEl.textContent = lines.join("\n") || "Paper inspect completed.";
}

function renderPaperJobs(jobs = []) {
  if (!paperJobListEl) {
    return;
  }
  paperJobListEl.textContent = "";
  const list = Array.isArray(jobs) ? jobs : [];
  if (!list.length) {
    const empty = document.createElement("p");
    empty.className = "tools-empty";
    empty.textContent = "No paper jobs yet.";
    paperJobListEl.appendChild(empty);
    return;
  }
  for (const job of list) {
    const row = document.createElement("div");
    row.className = "tools-item";
    const meta = document.createElement("div");
    meta.className = "tools-item-meta";
    const title = document.createElement("p");
    title.className = "tools-item-host";
    const inputSummary = job?.input_summary && typeof job.input_summary === "object" ? job.input_summary : {};
    title.textContent =
      String(job?.result?.title || inputSummary.url || inputSummary.pdf_path || inputSummary.html_path || inputSummary.text_path || job?.job_type || "paper job");
    meta.appendChild(title);
    const detail = document.createElement("p");
    detail.className = "tools-muted";
    const errorMessage = truncatePreview(job?.error?.message || "", 180);
    const latestDigest = truncatePreview(job?.result?.latest_digest_excerpt || "", 180);
    if (errorMessage) {
      detail.textContent = errorMessage;
    } else if (latestDigest) {
      detail.textContent = latestDigest;
    } else if (job?.result?.paper_id) {
      detail.textContent = `${Number(job?.result?.section_count || 0)} sections · ${Number(job?.result?.char_count || 0)} chars`;
    } else {
      detail.textContent = `Updated ${formatTime(job?.updated_at)}`;
    }
    meta.appendChild(detail);
    const tags = document.createElement("div");
    tags.className = "tools-item-tags";
    for (const tagText of [String(job?.status || "unknown"), String(job?.job_type || "")].filter(Boolean)) {
      const chip = document.createElement("span");
      chip.className = "tools-chip";
      chip.textContent = tagText;
      tags.appendChild(chip);
    }
    meta.appendChild(tags);
    row.appendChild(meta);
    const actions = document.createElement("div");
    actions.className = "tools-item-actions";
    const refreshBtn = document.createElement("button");
    refreshBtn.className = "ghost small";
    refreshBtn.textContent = "Refresh";
    refreshBtn.addEventListener("click", async () => {
      await refreshToolsState(true);
    });
    actions.appendChild(refreshBtn);
    if (job?.result?.paper_id) {
      const viewBtn = document.createElement("button");
      viewBtn.className = "ghost small";
      viewBtn.textContent = "View";
      viewBtn.addEventListener("click", async () => {
        await viewPaperArtifact(String(job?.result?.paper_id || ""));
      });
      actions.appendChild(viewBtn);
    }
    if (String(job?.status || "") !== "completed" && String(job?.status || "") !== "failed" && String(job?.status || "") !== "cancelled") {
      const cancelBtn = document.createElement("button");
      cancelBtn.className = "ghost small";
      cancelBtn.textContent = "Cancel";
      cancelBtn.addEventListener("click", async () => {
        await cancelBrokerJob(String(job?.job_id || ""));
      });
      actions.appendChild(cancelBtn);
    }
    row.appendChild(actions);
    paperJobListEl.appendChild(row);
  }
}

function renderPaperLibrary(items = []) {
  if (!paperListEl) {
    return;
  }
  paperListEl.textContent = "";
  const list = Array.isArray(items) ? items : [];
  if (!list.length) {
    const empty = document.createElement("p");
    empty.className = "tools-empty";
    empty.textContent = "No paper artifacts yet.";
    paperListEl.appendChild(empty);
    return;
  }
  for (const paper of list) {
    const row = document.createElement("div");
    row.className = "tools-item";
    const meta = document.createElement("div");
    meta.className = "tools-item-meta";
    const title = document.createElement("p");
    title.className = "tools-item-host";
    title.textContent = String(paper?.title || paper?.paper_id || "paper");
    meta.appendChild(title);
    const detail = document.createElement("p");
    detail.className = "tools-muted";
    detail.textContent = truncatePreview(paper?.latest_digest_excerpt || paper?.abstract || paper?.url || paper?.local_path || "", 180)
      || `Updated ${formatTime(paper?.updated_at)}`;
    meta.appendChild(detail);
    const tags = document.createElement("div");
    tags.className = "tools-item-tags";
    for (const tagText of [
      String(paper?.source_format || ""),
      `${Number(paper?.section_count || 0)} section${Number(paper?.section_count || 0) === 1 ? "" : "s"}`
    ]) {
      if (!tagText) {
        continue;
      }
      const chip = document.createElement("span");
      chip.className = "tools-chip";
      chip.textContent = tagText;
      tags.appendChild(chip);
    }
    meta.appendChild(tags);
    row.appendChild(meta);
    const actions = document.createElement("div");
    actions.className = "tools-item-actions";
    const openBtn = document.createElement("button");
    openBtn.className = "ghost small";
    openBtn.textContent = "View";
    openBtn.addEventListener("click", async () => {
      await viewPaperArtifact(String(paper?.paper_id || ""));
    });
    actions.appendChild(openBtn);
    row.appendChild(actions);
    paperListEl.appendChild(row);
  }
}

function renderExperimentJobs(jobs = []) {
  if (!experimentJobListEl) {
    return;
  }
  experimentJobListEl.textContent = "";
  const list = Array.isArray(jobs) ? jobs : [];
  if (!list.length) {
    const empty = document.createElement("p");
    empty.className = "tools-empty";
    empty.textContent = "No experiment jobs yet.";
    experimentJobListEl.appendChild(empty);
    return;
  }
  for (const job of list) {
    const row = document.createElement("div");
    row.className = "tools-item";
    const meta = document.createElement("div");
    meta.className = "tools-item-meta";
    const title = document.createElement("p");
    title.className = "tools-item-host";
    title.textContent = `${String(job?.job_type || "experiment")} · ${Number(job?.result?.prompt_count || job?.input_summary?.prompt_count || 0)} prompt(s)`;
    meta.appendChild(title);
    const detail = document.createElement("p");
    detail.className = "tools-muted";
    const errorMessage = truncatePreview(job?.error?.message || "", 180);
    const resultSummary = summarizeSummary(job?.result?.summary, 3);
    detail.textContent = errorMessage || resultSummary || `Updated ${formatTime(job?.updated_at)}`;
    meta.appendChild(detail);
    const tags = document.createElement("div");
    tags.className = "tools-item-tags";
    for (const tagText of [String(job?.status || "unknown"), String(job?.input_summary?.adapter_path ? "adapter" : "base")]) {
      const chip = document.createElement("span");
      chip.className = "tools-chip";
      chip.textContent = tagText;
      tags.appendChild(chip);
    }
    meta.appendChild(tags);
    row.appendChild(meta);
    const actions = document.createElement("div");
    actions.className = "tools-item-actions";
    const refreshBtn = document.createElement("button");
    refreshBtn.className = "ghost small";
    refreshBtn.textContent = "Refresh";
    refreshBtn.addEventListener("click", async () => {
      await refreshModelsState(true);
    });
    actions.appendChild(refreshBtn);
    if (job?.result?.experiment_id) {
      const viewBtn = document.createElement("button");
      viewBtn.className = "ghost small";
      viewBtn.textContent = "View";
      viewBtn.addEventListener("click", async () => {
        await viewExperimentArtifact(String(job?.result?.experiment_id || ""));
      });
      actions.appendChild(viewBtn);
    }
    if (String(job?.status || "") !== "completed" && String(job?.status || "") !== "failed" && String(job?.status || "") !== "cancelled") {
      const cancelBtn = document.createElement("button");
      cancelBtn.className = "ghost small";
      cancelBtn.textContent = "Cancel";
      cancelBtn.addEventListener("click", async () => {
        await cancelBrokerJob(String(job?.job_id || ""));
      });
      actions.appendChild(cancelBtn);
    }
    row.appendChild(actions);
    experimentJobListEl.appendChild(row);
  }
}

function renderExperimentLibrary(items = []) {
  if (!experimentListEl) {
    return;
  }
  experimentListEl.textContent = "";
  const list = Array.isArray(items) ? items : [];
  if (!list.length) {
    const empty = document.createElement("p");
    empty.className = "tools-empty";
    empty.textContent = "No saved experiment artifacts yet.";
    experimentListEl.appendChild(empty);
    if (experimentCompareOutputEl && !state.experimentDetail) {
      experimentCompareOutputEl.textContent = "(no comparison yet)";
    }
    return;
  }
  for (const experiment of list) {
    const row = document.createElement("div");
    row.className = "tools-item";
    const meta = document.createElement("div");
    meta.className = "tools-item-meta";
    const title = document.createElement("p");
    title.className = "tools-item-host";
    title.textContent = `${String(experiment?.kind || "experiment")} · ${Number(experiment?.prompt_count || 0)} prompt(s)`;
    meta.appendChild(title);
    const detail = document.createElement("p");
    detail.className = "tools-muted";
    detail.textContent = summarizeSummary(experiment?.summary, 3) || `Completed ${formatTime(experiment?.completed_at || experiment?.created_at)}`;
    meta.appendChild(detail);
    const tags = document.createElement("div");
    tags.className = "tools-item-tags";
    for (const tagText of [String(experiment?.experiment_id || ""), String(experiment?.adapter_path ? "adapter" : "base")].filter(Boolean)) {
      const chip = document.createElement("span");
      chip.className = "tools-chip";
      chip.textContent = tagText;
      tags.appendChild(chip);
    }
    meta.appendChild(tags);
    row.appendChild(meta);
    const actions = document.createElement("div");
    actions.className = "tools-item-actions";
    const viewBtn = document.createElement("button");
    viewBtn.className = "ghost small";
    viewBtn.textContent = "View";
    viewBtn.addEventListener("click", async () => {
      await viewExperimentArtifact(String(experiment?.experiment_id || ""));
    });
    actions.appendChild(viewBtn);
    row.appendChild(actions);
    experimentListEl.appendChild(row);
  }
  if (list.length >= 2 && state.experimentDetail?.mode !== "artifact") {
    void compareLatestExperiments(list);
  } else if (list.length < 2 && experimentCompareOutputEl && state.experimentDetail?.mode !== "artifact") {
    experimentCompareOutputEl.textContent = "(view an experiment to inspect details)";
  }
}

async function refreshModelsState(showErrors = true) {
  setModelsBusy(true);
  try {
    const [modelsState, adaptersState, jobsState, experimentsState, trainingDatasetsState, trainingJobsState, trainingRunsState] = await Promise.allSettled([
      sendRuntimeMessage({ type: "assistant.models.get" }),
      sendRuntimeMessage({ type: "assistant.mlx.adapters.list" }),
      sendRuntimeMessage({ type: "assistant.jobs.list", kind: "experiment" }),
      sendRuntimeMessage({ type: "assistant.experiments.list" }),
      sendRuntimeMessage({ type: "assistant.mlx.training.datasets.list" }),
      sendRuntimeMessage({ type: "assistant.jobs.list", kind: "training" }),
      sendRuntimeMessage({ type: "assistant.mlx.training.runs.list" })
    ]);

    if (modelsState.status === "rejected") {
      throw modelsState.reason;
    }
    const modelsResult = modelsState.value;
    if (!modelsResult.ok) {
      throw new Error(modelsResult.error || "Failed to load models metadata.");
    }
    const backends = Array.isArray(modelsResult.backends) ? modelsResult.backends : [];
    const mlx = modelsResult.mlx && typeof modelsResult.mlx === "object" ? modelsResult.mlx : {};
    state.mlxRuntime = mlx;
    renderModelsBackends(backends);
    if (modelsBackendEl?.value) {
      backendEl.value = modelsBackendEl.value;
    }
    updateComposerState();
    if (mlxModelPathEl) {
      mlxModelPathEl.value = String(mlx.model_path || "");
    }
    if (trainingModelPathEl && !String(trainingModelPathEl.value || "").trim()) {
      trainingModelPathEl.value = String(mlx.model_path || "");
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
    if (trainingPresetEl && !trainingRankEl?.value) {
      applyTrainingPreset();
    }

    if (adaptersState.status === "fulfilled" && adaptersState.value?.ok) {
      renderMlxAdapters(adaptersState.value);
    }

    state.experimentJobs =
      jobsState.status === "fulfilled" && jobsState.value?.ok && Array.isArray(jobsState.value.jobs)
        ? jobsState.value.jobs
        : [];
    state.experiments =
      experimentsState.status === "fulfilled" && experimentsState.value?.ok && Array.isArray(experimentsState.value.experiments)
        ? experimentsState.value.experiments
        : [];
    state.trainingDatasets =
      trainingDatasetsState.status === "fulfilled"
      && trainingDatasetsState.value?.ok
      && Array.isArray(trainingDatasetsState.value.datasets)
        ? trainingDatasetsState.value.datasets
        : [];
    state.trainingJobs =
      trainingJobsState.status === "fulfilled"
      && trainingJobsState.value?.ok
      && Array.isArray(trainingJobsState.value.jobs)
        ? trainingJobsState.value.jobs
        : [];
    state.trainingRuns =
      trainingRunsState.status === "fulfilled"
      && trainingRunsState.value?.ok
      && Array.isArray(trainingRunsState.value.runs)
        ? trainingRunsState.value.runs
        : [];
    renderExperimentJobs(state.experimentJobs);
    renderExperimentLibrary(state.experiments);
    renderTrainingDatasets(state.trainingDatasets);
    renderTrainingJobs(state.trainingJobs);
    renderTrainingRuns(state.trainingRuns);
    if (experimentsStatusEl) {
      const runningJobs = state.experimentJobs.filter((job) => ACTIVE_JOB_STATUSES.has(String(job?.status || ""))).length;
      const availabilityMessage = describeExperimentAvailability(mlx);
      experimentsStatusEl.textContent =
        runningJobs > 0
          ? `${runningJobs} experiment job${runningJobs === 1 ? "" : "s"} running. Auto-refreshing every ${Math.round(JOB_POLL_INTERVAL_MS / 1000)}s.`
          : availabilityMessage
            || `${state.experiments.length} saved experiment artifact${state.experiments.length === 1 ? "" : "s"}.`;
    }
    const runningTrainingJobs = state.trainingJobs.filter((job) => ACTIVE_JOB_STATUSES.has(String(job?.status || ""))).length;
    if (runningTrainingJobs > 0) {
      setTrainingStatus(
        `${runningTrainingJobs} training job${runningTrainingJobs === 1 ? "" : "s"} running. Auto-refreshing every ${Math.round(JOB_POLL_INTERVAL_MS / 1000)}s.`
      );
    } else {
      setTrainingStatus(
        `${state.trainingDatasets.length} dataset${state.trainingDatasets.length === 1 ? "" : "s"} · ${state.trainingRuns.length} run${state.trainingRuns.length === 1 ? "" : "s"}`
      );
    }
    scheduleAutoRefresh("experiments", hasActiveJobs(state.experimentJobs) || hasActiveJobs(state.trainingJobs));
  } catch (error) {
    scheduleAutoRefresh("experiments", false);
    if (showErrors && mlxRuntimeStatusEl) {
      mlxRuntimeStatusEl.textContent = `MLX status error: ${String(error.message || error)}`;
    }
    if (showErrors && mlxContractEl) {
      mlxContractEl.textContent = "(contract unavailable)";
    }
    if (showErrors && experimentsStatusEl) {
      experimentsStatusEl.textContent = `Experiment error: ${String(error.message || error)}`;
    }
    if (showErrors) {
      setTrainingStatus(`Training error: ${String(error.message || error)}`);
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

async function runExperimentJob(kind) {
  const promptSet = buildPromptSetFromInputs();
  if (!promptSet.length) {
    if (experimentsStatusEl) {
      experimentsStatusEl.textContent = "Enter at least one prompt.";
    }
    return;
  }
  const adapterPath = String(mlxAdapterPathEl?.value || "").trim() || String(state.activeMlxAdapterPath || "").trim();
  if (kind === "adapter_eval" && !adapterPath) {
    if (experimentsStatusEl) {
      experimentsStatusEl.textContent = "Load an adapter or enter an adapter path before running adapter eval.";
    }
    return;
  }
  const availabilityMessage = describeExperimentAvailability(state.mlxRuntime);
  if (availabilityMessage && experimentsStatusEl) {
    experimentsStatusEl.textContent = availabilityMessage;
  }
  setModelsBusy(true);
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.experiments.job.start",
      kind,
      modelPath: String(mlxModelPathEl?.value || "").trim(),
      adapterPath,
      promptSet,
      generation: readGenerationInputs(),
      systemPrompt: readMlxSystemPromptInput()
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to start experiment job.");
    }
    state.experimentDetail = null;
    if (experimentsStatusEl) {
      experimentsStatusEl.textContent = `Started ${kind.replace("_", " ")} job ${String(result.job?.job_id || "")}.`;
    }
    await refreshModelsState(false);
  } catch (error) {
    if (experimentsStatusEl) {
      experimentsStatusEl.textContent = `Experiment error: ${String(error.message || error)}`;
    }
  } finally {
    setModelsBusy(false);
  }
}

async function viewExperimentArtifact(experimentId) {
  if (!experimentId) {
    return;
  }
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.experiments.get",
      experimentId
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to load experiment artifact.");
    }
    const experiment = result.experiment && typeof result.experiment === "object" ? result.experiment : {};
    state.experimentDetail = {
      mode: "artifact",
      experimentId: String(experiment.experiment_id || experimentId)
    };
    renderExperimentDetail(experiment);
  } catch (error) {
    if (experimentCompareOutputEl) {
      experimentCompareOutputEl.textContent = `Experiment error: ${String(error.message || error)}`;
    }
  }
}

async function compareLatestExperiments(items) {
  const list = Array.isArray(items) ? items : [];
  if (list.length < 2 || !experimentCompareOutputEl) {
    return;
  }
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.experiments.compare",
      experimentId: String(list[0]?.experiment_id || ""),
      otherExperimentId: String(list[1]?.experiment_id || "")
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to compare experiments.");
    }
    const comparison = result.comparison && typeof result.comparison === "object" ? result.comparison : {};
    state.experimentDetail = {
      mode: "comparison",
      leftId: String(list[0]?.experiment_id || ""),
      rightId: String(list[1]?.experiment_id || "")
    };
    const lines = [
      `Latest comparison: ${String(list[0]?.experiment_id || "")} vs ${String(list[1]?.experiment_id || "")}`,
      `Latency: ${String(comparison.left_average_latency_ms ?? "n/a")} ms vs ${String(comparison.right_average_latency_ms ?? "n/a")} ms`,
      `Latency delta (second - first): ${String(comparison.average_latency_delta_ms ?? "")}`,
      `Exact match: ${String(comparison.left_exact_match_rate ?? "n/a")} vs ${String(comparison.right_exact_match_rate ?? "n/a")}`,
      `Exact match delta (second - first): ${String(comparison.exact_match_rate_delta ?? "n/a")}`,
      `Contains reference: ${String(comparison.left_contains_reference_rate ?? "n/a")} vs ${String(comparison.right_contains_reference_rate ?? "n/a")}`,
      `Contains delta (second - first): ${String(comparison.contains_reference_rate_delta ?? "n/a")}`
    ];
    experimentCompareOutputEl.textContent = lines.join("\n");
  } catch (error) {
    experimentCompareOutputEl.textContent = `Comparison error: ${String(error.message || error)}`;
  }
}

async function cancelBrokerJob(jobId) {
  if (!jobId) {
    return;
  }
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.jobs.cancel",
      jobId
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to cancel job.");
    }
    if (String(jobId).startsWith("paper_job_")) {
      await refreshToolsState(false);
    } else {
      await refreshModelsState(false);
    }
  } catch (error) {
    if (String(jobId).startsWith("paper_job_")) {
      updateToolsStatus(`Paper job error: ${String(error.message || error)}`);
    } else if (experimentsStatusEl) {
      experimentsStatusEl.textContent = `Experiment error: ${String(error.message || error)}`;
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
  const min = Number.isInteger(Number(stepLimitsRaw.min)) ? Number(stepLimitsRaw.min) : 1;
  const max = Number.isInteger(Number(stepLimitsRaw.max)) ? Number(stepLimitsRaw.max) : 40;
  const configured = Number.parseInt(String(raw.agent_max_steps ?? 20), 10);
  const agentMaxSteps = Number.isInteger(configured) ? configured : 20;
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

function renderBrowserConfig(config) {
  const normalized = normalizeBrowserConfig(config);
  state.toolsBrowserConfig = normalized;
  if (toolsAgentMaxStepsEl) {
    toolsAgentMaxStepsEl.min = String(normalized.limits.agent_max_steps.min);
    toolsAgentMaxStepsEl.max = String(normalized.limits.agent_max_steps.max);
    toolsAgentMaxStepsEl.value = String(normalized.agent_max_steps);
  }
}


function setToolsBusy(busy) {
  state.toolsBusy = busy;
  const controls = [
    toolsRefreshBtn,
    toolsHostInputEl,
    toolsAllowBtn,
    toolsAllowActiveBtn,
    toolsAgentMaxStepsEl,
    toolsBrowserApplyBtn,
    paperUseActiveBtn,
    paperInspectBtn,
    paperAnalyzeBtn,
    readShowWhereBtn,
    $("composer-read-explain-btn"),
    $("composer-read-guide-btn"),
    $("composer-read-show-btn")
  ];
  for (const control of controls) {
    if (control) {
      control.disabled = busy;
    }
  }
  for (const button of toolsAllowedListEl?.querySelectorAll("button") || []) {
    button.disabled = busy;
  }
  setReadAssistantExplainEnabled();
}

function updateToolsStatus(text) {
  if (toolsPolicyStatusEl) {
    toolsPolicyStatusEl.textContent = text;
  }
}

function updatePapersStatus(text) {
  if (papersStatusEl) {
    papersStatusEl.textContent = text;
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
  setToolsBusy(state.toolsBusy);
}


async function refreshToolsState(showErrors = true, options = {}) {
  const captureReadContext = options.captureReadContext === true || state.activeMainTab === "tools";
  setToolsBusy(true);
  try {
    const [policyState, activeState, browserConfigState, readContextState] = await Promise.allSettled([
      sendRuntimeMessage({ type: "assistant.tools.page_hosts.get" }),
      sendRuntimeMessage({ type: "assistant.tools.page_hosts.active_tab" }),
      sendRuntimeMessage({ type: "assistant.tools.browser_config.get" }),
      captureReadContext
        ? sendRuntimeMessage({ type: "assistant.read.context.capture" })
        : Promise.resolve({
            ok: true,
            context: state.readContext,
            active_tab: state.toolsActiveTab
          })
    ]);

    if (policyState.status === "rejected") {
      throw policyState.reason;
    }
    const policyResult = policyState.value;
    if (!policyResult.ok) {
      throw new Error(policyResult.error || "Failed to load tool policy.");
    }
    renderToolsPolicy(policyResult.policy);

    let browserConfigUnavailable = false;
    if (browserConfigState.status === "fulfilled" && browserConfigState.value?.ok) {
      renderBrowserConfig(browserConfigState.value.browser);
    } else {
      browserConfigUnavailable = true;
      if (!state.toolsBrowserConfig) {
        renderBrowserConfig({});
      }
      const errorText =
        browserConfigState.status === "rejected"
          ? String(browserConfigState.reason?.message || browserConfigState.reason)
          : String(browserConfigState.value?.error || "Failed to load browser settings.");
      console.warn("[secure-panel] tools refresh browser config failed:", errorText);
    }

    let activeTabUnavailable = false;
    if (activeState.status === "fulfilled") {
      state.toolsActiveTab =
        activeState.value?.ok && activeState.value.active_tab && typeof activeState.value.active_tab === "object"
          ? activeState.value.active_tab
          : null;
      if (!activeState.value?.ok) {
        activeTabUnavailable = true;
        console.warn(
          "[secure-panel] tools refresh active tab failed:",
          String(activeState.value?.error || "Failed to load active tab details.")
        );
      }
    } else {
      activeTabUnavailable = true;
      state.toolsActiveTab = null;
      console.warn(
        "[secure-panel] tools refresh active tab failed:",
        String(activeState.reason?.message || activeState.reason)
      );
    }

    if (readContextState.status === "fulfilled") {
      if (readContextState.value?.active_tab && typeof readContextState.value.active_tab === "object") {
        state.toolsActiveTab = readContextState.value.active_tab;
      }
      state.readContext = readContextState.value?.ok ? readContextState.value.context || null : null;
      if (!readContextState.value?.ok && readContextState.value?.error) {
        console.warn(
          "[secure-panel] read assistant context unavailable:",
          typeof readContextState.value.error === "string"
            ? readContextState.value.error
            : readContextState.value.error?.message || "unknown_error"
        );
      }
    } else {
      state.readContext = null;
      console.warn(
        "[secure-panel] read assistant context refresh failed:",
        String(readContextState.reason?.message || readContextState.reason)
      );
    }

    renderReadAssistantPreview(state.readContext);
    setReadAssistantExplainEnabled();
    updateReadAssistantStatus();

    const statusParts = [];
    if (state.toolsActiveTab?.host) {
      const marker = state.toolsActiveTab.allowed ? "allowed" : "not allowed";
      statusParts.push(`Active tab: ${state.toolsActiveTab.host} (${marker})`);
    } else {
      statusParts.push("Allowlist loaded. Active tab host unavailable.");
    }
    if (activeTabUnavailable && !state.toolsActiveTab?.host) {
      statusParts[0] = "Allowlist loaded. Active tab unavailable.";
    }
    if (browserConfigUnavailable) {
      statusParts.push("Browser settings unavailable.");
    }
    updateToolsStatus(statusParts.join(" "));
    scheduleAutoRefresh("papers", false);
  } catch (error) {
    scheduleAutoRefresh("papers", false);
    console.warn("[secure-panel] tools refresh failed:", String(error?.message || error));
    if (showErrors) {
      updateToolsStatus(`Tools error: ${String(error.message || error)}`);
      updatePapersStatus(`Read assistant error: ${String(error.message || error)}`);
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

async function applyBrowserConfigFromInputs() {
  const limits = state.toolsBrowserConfig?.limits?.agent_max_steps || { min: 1, max: 40 };
  const parsed = Number.parseInt(String(toolsAgentMaxStepsEl?.value || "").trim(), 10);
  if (!Number.isInteger(parsed)) {
    updateToolsStatus("Agent max steps must be an integer.");
    return;
  }
  if (parsed < limits.min || parsed > limits.max) {
    updateToolsStatus(`Agent max steps must be between ${limits.min} and ${limits.max}.`);
    return;
  }

  setToolsBusy(true);
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.tools.browser_config.update",
      agentMaxSteps: parsed
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to update browser settings.");
    }
    renderBrowserConfig(result.browser);
    updateToolsStatus(`Browser agent max steps set to ${result.browser?.agent_max_steps ?? parsed}.`);
  } catch (error) {
    updateToolsStatus(`Tools error: ${String(error.message || error)}`);
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

function buildPaperSourceMessage(rawValue) {
  const source = String(rawValue || "").trim();
  if (!source) {
    return null;
  }
  if (/^https?:\/\//i.test(source)) {
    return { url: source };
  }
  if (/\.pdf$/i.test(source)) {
    return { pdfPath: source };
  }
  if (/\.(html?|xhtml)$/i.test(source)) {
    return { htmlPath: source };
  }
  return { textPath: source };
}


async function useActiveTabForPaperSource() {
  setToolsBusy(true);
  try {
    await captureReadAssistantContext(true);
  } catch {
    // Status messaging already happens inside captureReadAssistantContext.
  } finally {
    setToolsBusy(false);
  }
}


async function inspectPaperFromInputs() {
  await submitReadAssistantAction("explain_selection");
}


async function analyzePaperFromInputs() {
  await submitReadAssistantAction("guide_page");
}

async function viewPaperArtifact(paperId) {
  if (!paperId) {
    return;
  }
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.papers.get",
      paperId
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to load paper artifact.");
    }
    const paper = result.paper && typeof result.paper === "object" ? result.paper : {};
    const latestDigest =
      paper.latest_digest && typeof paper.latest_digest === "object"
        ? String(paper.latest_digest.text || "").trim()
        : "";
    state.paperInspect = {
      inspect: {
        title: String(paper.title || ""),
        authors: Array.isArray(paper.authors) ? paper.authors : [],
        abstract: String(paper.abstract || ""),
        url: String(paper.url || paper.local_path || ""),
        preview_text: latestDigest || String(paper.text_preview || "")
      },
      cached_paper: {
        paper_id: String(paper.paper_id || ""),
        title: String(paper.title || "")
      },
      paper
    };
    renderPaperDetail(paper);
    updatePapersStatus(`Loaded paper artifact ${paperId}.`);
  } catch (error) {
    updatePapersStatus(`Paper error: ${String(error.message || error)}`);
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
    resetComposerReadAssistantModes();
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
  resetComposerReadAssistantModes();
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
  runUi.streamedAssistantText = "";
  runUi.streamedReasoningText = "";

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
    const prompt = buildComposerPromptForSubmit(promptEl.value);
    const requestPromptSuffix = buildComposerPromptSuffix();
    if (!prompt) {
      appendMessage("system", "Enter a prompt first.");
      return;
    }
    const rewriteIndex = hasRewriteTarget() ? Number(state.rewriteTargetIndex) : null;
    const isRewrite = Number.isInteger(rewriteIndex);
    const forceBrowserAction = forceBrowserActionEl?.checked === true || state.composerShowMeWhere;
    if (!isRewrite) {
      appendMessage("user", prompt);
    }
    promptEl.value = "";
    resetComposerReadAssistantModes();

    if (usesCodexRunProtocol()) {
      request = {
        type: "assistant.codex.run.start",
        backend,
        sessionId: state.sessionId,
        prompt,
        requestPromptSuffix,
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
        requestPromptSuffix,
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
        requestPromptSuffix,
        includePageContext: includePageContextEl.checked,
        forceBrowserAction,
        confirmed: false
      };
    }
    request = applyLlamaThinkingRequestFields(request, backend);
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
    title: "Page Host Not Allowlisted",
    text: `${host} is not in your allowlist. Add it and retry?`,
    confirmLabel: "Allow + Retry"
  });

  if (!accepted) {
    const text = `Not allowlisted by page policy: ${host}. Request canceled.`;
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
      streamedAssistantText: "",
      streamedReasoningText: "",
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
      runUi.streamedAssistantText = String(event?.data?.text || "");
      if (runUi.allowAssistantBubble) {
        upsertCodexAssistantBubble(
          runUi,
          runUi.streamedAssistantText,
          true,
          buildPendingReasoningPayload(runUi.streamedReasoningText)
        );
      }
      continue;
    }

    if (event.type === "partial_reasoning_text") {
      clearRunWaitingIndicator(runUi);
      runUi.streamedReasoningText = String(event?.data?.text || "");
      if (runUi.allowAssistantBubble && (runUi.streamedReasoningText || runUi.assistantNode)) {
        upsertCodexAssistantBubble(
          runUi,
          runUi.streamedAssistantText,
          true,
          buildPendingReasoningPayload(runUi.streamedReasoningText)
        );
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
      const finalAssistantText = assistantText || runUi.streamedAssistantText || getMessageText(runUi.assistantNode);
      const finalReasoningText = reasoningText || runUi.streamedReasoningText;
      runUi.streamedAssistantText = finalAssistantText;
      runUi.streamedReasoningText = finalReasoningText;
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
      disableApprovalCards(runUi);
      if (runUi.backend === "codex" && event.type === "completed" && !assistantText && !reasoningText) {
        continue;
      }
      appendCodexStatusCard(describeCodexEvent(event));
      continue;
    }

    if (event.type === "thinking") {
      if (runUi.allowAssistantBubble && (runUi.streamedAssistantText || runUi.streamedReasoningText || runUi.assistantNode)) {
        upsertCodexAssistantBubble(
          runUi,
          runUi.streamedAssistantText,
          true,
          buildPendingReasoningPayload(runUi.streamedReasoningText)
        );
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
  const reasoningState = normalizeReasoningPayload(reasoningBlocks);
  if (!text && !reasoningState.text.trim() && !reasoningState.blocks.length) {
    return;
  }
  if (!runUi.assistantNode) {
    runUi.assistantNode = appendMessage(
      "assistant",
      text,
      pending,
      "codex-assistant",
      null,
      reasoningState
    );
    return;
  }
  updateMessage(runUi.assistantNode, "assistant", text, pending, "codex-assistant", reasoningState);
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

function splitReasoningText(text) {
  return String(text || "")
    .split(/\n{2,}/)
    .map((part) => part.trim())
    .filter(Boolean);
}

function normalizeReasoningPayload(reasoning) {
  if (Array.isArray(reasoning)) {
    const blocks = reasoning.map((block) => String(block || "").trim()).filter(Boolean);
    return {
      blocks,
      text: blocks.join("\n\n"),
      pending: false
    };
  }
  if (reasoning && typeof reasoning === "object") {
    const pending = Boolean(reasoning.pending);
    const blocks = Array.isArray(reasoning.blocks)
      ? reasoning.blocks.map((block) => String(block || "").trim()).filter(Boolean)
      : [];
    const text = String(reasoning.text || "");
    if (pending) {
      return {
        blocks,
        text,
        pending: true
      };
    }
    const finalBlocks = blocks.length ? blocks : splitReasoningText(text);
    return {
      blocks: finalBlocks,
      text: finalBlocks.join("\n\n") || text,
      pending: false
    };
  }
  const text = String(reasoning || "");
  const blocks = text.trim() ? splitReasoningText(text) : [];
  return {
    blocks,
    text: blocks.join("\n\n") || text,
    pending: false
  };
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

function buildPendingReasoningPayload(text) {
  const reasoningText = String(text || "");
  if (!reasoningText.trim()) {
    return null;
  }
  return {
    text: reasoningText,
    pending: true
  };
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
  const llamaSelected = isLlamaChatBackend();
  sendBtn.disabled = locked;
  promptEl.disabled = locked;
  confirmBtn.disabled = state.busy;
  if (forceBrowserActionEl) {
    if (!browserActionSupported && forceBrowserActionEl.checked) {
      forceBrowserActionEl.checked = false;
    }
    forceBrowserActionEl.disabled = locked || !browserActionSupported;
  }
  if (llamaEnableThinkingEl) {
    llamaEnableThinkingEl.disabled = locked || !llamaSelected;
  }
  const composerGuideBtn = $("composer-read-guide-btn");
  if (composerGuideBtn) {
    composerGuideBtn.disabled = locked || state.toolsBusy;
  }
  const composerShowBtn = $("composer-read-show-btn");
  if (composerShowBtn) {
    composerShowBtn.disabled = locked || state.toolsBusy;
  }
  updateLlamaThinkingComposer();
  syncReadAssistantQuickActionState();
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
  resetComposerReadAssistantModes();
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

function createReasoningDisclosure(reasoning, existing = null) {
  const normalized = normalizeReasoningPayload(reasoning);
  const blocks = normalized.blocks;
  const text = normalized.pending ? normalized.text : normalized.text || blocks.join("\n\n");
  if (!text.trim() && !blocks.length) {
    return null;
  }

  const details = document.createElement("details");
  details.className = `reasoning-disclosure${normalized.pending ? " pending" : ""}`;
  details.open = existing ? Boolean(existing.open) : normalized.pending;

  const summary = document.createElement("summary");
  if (normalized.pending) {
    summary.textContent = "Thinking...";
  } else {
    const totalChars = blocks.reduce((sum, block) => sum + block.length, 0);
    summary.textContent = `Reasoning (${blocks.length} block${blocks.length === 1 ? "" : "s"}, ${totalChars} chars)`;
  }
  details.appendChild(summary);

  const content = document.createElement("div");
  content.className = "reasoning-content";
  const combined = normalized.pending
    ? text
    : blocks
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
  let reasoningState = null;
  if (role === "assistant") {
    const existingReasoningDisclosure = body.querySelector("details.reasoning-disclosure");
    reasoningState = normalizeReasoningPayload(reasoningBlocks);
    if (reasoningState.text.trim() || reasoningState.blocks.length) {
      displayText = rawText;
      reasoningDisclosure = createReasoningDisclosure(reasoningState, existingReasoningDisclosure);
    } else {
      const collapsed = collapseThinkBlocks(rawText);
      displayText = collapsed.visible;
      reasoningDisclosure = createReasoningDisclosure(collapsed.reasoningBlocks, existingReasoningDisclosure);
    }
  }
  body.textContent = "";
  if (displayText.trim()) {
    body.appendChild(renderMarkdownFragment(displayText));
  } else if (role === "assistant" && reasoningDisclosure) {
    const note = document.createElement("p");
    note.className = "reasoning-note";
    note.textContent = reasoningState?.pending
      ? "No final answer yet. Thinking below."
      : "No final answer text. Expand reasoning below.";
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
