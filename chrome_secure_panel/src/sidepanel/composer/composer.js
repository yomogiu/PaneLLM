function getBackendCapabilities(backend = backendEl?.value) {
  const normalized = String(backend || "").trim();
  const backends = Array.isArray(state.availableBackends) ? state.availableBackends : [];
  const selected = backends.find((item) => String(item?.id || "").trim() === normalized);
  return selected && typeof selected.capabilities === "object" ? selected.capabilities : {};
}

function backendSupportsReasoningControls(backend = backendEl?.value) {
  const normalized = String(backend || "").trim();
  const capabilities = getBackendCapabilities(normalized);
  if (Object.keys(capabilities).length > 0) {
    return Boolean(capabilities.supports_reasoning_controls);
  }
  return normalized === "llama";
}

function setReadAssistantExplainEnabled() {
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
  const browserBtn = $("composer-read-browser-btn");
  const guideBtn = $("composer-read-guide-btn");
  const showBtn = $("composer-read-show-btn");
  explainBtn?.classList.toggle("active", Boolean(state.composerExplainSelection));
  explainBtn?.setAttribute("aria-pressed", String(Boolean(state.composerExplainSelection)));
  const composerBrowserArmed = state.browserActionArmed;
  browserBtn?.classList.toggle("active", composerBrowserArmed);
  browserBtn?.setAttribute("aria-pressed", String(composerBrowserArmed));
  guideBtn?.classList.toggle("active", Boolean(state.composerGuidePageContext));
  guideBtn?.setAttribute("aria-pressed", String(Boolean(state.composerGuidePageContext)));
  showBtn?.classList.toggle("active", state.composerShowMeWhere);
  showBtn?.setAttribute("aria-pressed", String(state.composerShowMeWhere));
}

function clearComposerExplainSelection() {
  state.composerExplainSelection = "";
}

function clearComposerShowMeWhere() {
  state.composerShowMeWhere = false;
  if (state.browserActionArmed && state.browserActionArmedReason === "Show Me Where") {
    setBrowserActionArmed(false);
  }
}

function setComposerGuidePageContext(enabled) {
  const next = Boolean(enabled);
  if (!next && state.composerShowMeWhere) {
    clearComposerShowMeWhere();
  }
  state.composerGuidePageContext = next;
  if (includePageContextEl) {
    includePageContextEl.checked = next;
  }
  syncReadAssistantQuickActionState();
  renderBrowserNextActionIndicator();
}

function isComposerGuidePageContextEnabled() {
  if (includePageContextEl) {
    return includePageContextEl.checked === true;
  }
  return state.composerGuidePageContext === true;
}

function toggleComposerGuidePageContext() {
  const next = !isComposerGuidePageContextEnabled();
  setComposerGuidePageContext(next);
  updatePapersStatus(next ? "Page context included with next message." : "Page context disabled.");
  focusComposerToEnd();
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
  setComposerGuidePageContext(true);
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
    setComposerGuidePageContext(true);
    setBrowserActionArmed(true, "Show Me Where");
    updatePapersStatus("Show Me Where is armed. Send a prompt to answer it and navigate to the best section.");
    focusComposerToEnd();
  } else {
    if (state.browserActionArmedReason === "Show Me Where") {
      setBrowserActionArmed(false);
    }
    updatePapersStatus("Show Me Where disabled.");
  }
  syncReadAssistantQuickActionState();
  updateComposerState();
}

function toggleComposerBrowserActionMode() {
  if (state.composerShowMeWhere) {
    updatePapersStatus("Show Me Where already enables browser tools for the next message.");
    focusComposerToEnd();
    return;
  }
  const next = !(state.browserActionArmed && state.browserActionArmedReason !== "Show Me Where");
  setBrowserActionArmed(next);
  updatePapersStatus(
    next
      ? "Browser tools enabled for your next message."
      : "Browser tools disabled for your next message."
  );
  focusComposerToEnd();
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
  if (isComposerGuidePageContextEnabled()) {
    parts.push(GUIDE_PAGE_CONTEXT_FOLLOWUP);
  }
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
  if (!isAllowlistedActiveTab(state.toolsActiveTab)) {
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
    setComposerGuidePageContext(true);
    setBrowserActionArmed(kind === "show_me_where", kind === "show_me_where" ? "Show Me Where" : "");
    promptEl.value = buildReadAssistantPrompt(kind, context);
    updateComposerState();
    sendBtn.click();
  } catch (error) {
    updatePapersStatus(`Read assistant error: ${String(error.message || error)}`);
  } finally {
    setToolsBusy(false);
  }
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
  guideBtn.addEventListener("click", () => {
    toggleComposerGuidePageContext();
  });

  const browserBtn = document.createElement("button");
  browserBtn.id = "composer-read-browser-btn";
  browserBtn.type = "button";
  browserBtn.className = "ghost composer-quick-action";
  browserBtn.textContent = "Use Browser Tools";
  browserBtn.setAttribute("aria-pressed", "false");
  browserBtn.addEventListener("click", () => {
    toggleComposerBrowserActionMode();
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
  row.appendChild(browserBtn);
  row.appendChild(showBtn);
  promptEl.insertAdjacentElement("afterend", row);
  syncReadAssistantQuickActionState();
}

installReadAssistantQuickActions();

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

newSessionBtn.addEventListener("click", async () => {
  await handleStartNewSession();
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

browserTabBtn?.addEventListener("click", () => {
  setMainTab("browser");
});

browserPickerStartBtn?.addEventListener("click", async () => {
  try {
    const result = await sendRuntimeMessage({ type: "assistant.browser.element_picker.start" });
    if (!result?.ok) {
      throw new Error(result?.error || "Unable to start element picker.");
    }
    state.browserPickerActive = true;
    state.browserPickerRequestId = String(result.requestId || result.request_id || "");
    state.browserPickerError = "";
  } catch (error) {
    state.browserPickerActive = false;
    state.browserPickerRequestId = "";
    state.browserPickerError = `Picker error: ${String(error.message || error)}`;
  } finally {
    renderBrowserPickerPanel();
  }
});

browserPickerCancelBtn?.addEventListener("click", async () => {
  try {
    await sendRuntimeMessage({ type: "assistant.browser.element_picker.cancel" });
  } catch {
    // Ignore background cancellation failures and clear local UI state.
  } finally {
    state.browserPickerActive = false;
    state.browserPickerRequestId = "";
    renderBrowserPickerPanel();
  }
});

browserAutomationArmBtn?.addEventListener("click", () => {
  setBrowserActionArmed(!state.browserActionArmed);
});

browserProfileSelectEl?.addEventListener("change", () => {
  setSelectedBrowserProfile(browserProfileSelectEl.value, { forceNameSync: true });
});

browserProfileNameEl?.addEventListener("change", async () => {
  await commitSelectedBrowserProfileName();
});

browserProfileRecordBtn?.addEventListener("click", async () => {
  await toggleBrowserProfileRecording();
});

browserProfileSaveLatestBtn?.addEventListener("click", async () => {
  await saveLatestBrowserProfileStep();
});

browserProfileUseBtn?.addEventListener("click", async () => {
  await toggleSelectedBrowserProfileAttachment();
});

browserProfileDeleteBtn?.addEventListener("click", async () => {
  await deleteSelectedBrowserProfile();
});

paperSummaryTabBtn?.addEventListener("click", () => {
  setPaperTab("summary");
});

paperHighlightsTabBtn?.addEventListener("click", () => {
  setPaperTab("highlights");
});

paperMemoryTabBtn?.addEventListener("click", () => {
  setPaperTab("memory");
});

paperChatTabBtn?.addEventListener("click", () => {
  setPaperTab("chat");
});

paperMemorySearchBtn?.addEventListener("click", async () => {
  await refreshPaperMemory(true);
});

paperMemorySearchEl?.addEventListener("keydown", async (event) => {
  if (event.key !== "Enter") {
    return;
  }
  event.preventDefault();
  await refreshPaperMemory(true);
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

backendEl?.addEventListener("change", () => {
  updateComposerState();
});

llamaEnableThinkingEl?.addEventListener("change", () => {
  updateLlamaThinkingComposer();
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

chrome.tabs?.onActivated?.addListener(() => {
  queueActiveTabRefresh();
});

chrome.tabs?.onUpdated?.addListener((_tabId, changeInfo, tab) => {
  if (!tab?.active) {
    return;
  }
  if (typeof changeInfo.url === "string" || typeof changeInfo.title === "string" || changeInfo.status === "complete") {
    queueActiveTabRefresh();
  }
});

chrome.windows?.onFocusChanged?.addListener((windowId) => {
  if (windowId !== chrome.windows.WINDOW_ID_NONE) {
    queueActiveTabRefresh();
  }
});

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") {
    queueActiveTabRefresh();
  }
});

window.addEventListener("focus", () => {
  queueActiveTabRefresh();
});

chrome.runtime?.onMessage?.addListener((message) => {
  if (!message || typeof message !== "object") {
    return;
  }
  if (message.type === "assistant.browser.element_picker.result") {
    state.browserPickerActive = false;
    state.browserPickerRequestId = "";
    state.browserPickerError = "";
    if (!rememberBrowserPickerEntry(message.payload)) {
      state.browserPickerError = "Picker returned an invalid element.";
    }
    renderBrowserPickerPanel();
    renderBrowserNextActionIndicator();
    return;
  }
  if (message.type === "assistant.browser.element_picker.cancelled") {
    state.browserPickerActive = false;
    state.browserPickerRequestId = "";
    state.browserPickerError = "";
    renderBrowserPickerPanel();
    return;
  }
  if (message.type === "assistant.browser.element_picker.error") {
    state.browserPickerActive = false;
    state.browserPickerRequestId = "";
    state.browserPickerError = String(message.error || "Element picker failed.");
    renderBrowserPickerPanel();
  }
});

