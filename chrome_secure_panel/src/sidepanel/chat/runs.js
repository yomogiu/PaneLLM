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

async function restoreRun(conversation) {
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
  const runUi = ensureRunUi(runId, conversation.id, allowAssistantBubble, "codex");
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
      type: "assistant.run.events",
      runId,
      after: 0,
      timeoutMs: 0
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to restore run.");
    }
    renderRunEvents(runId, Array.isArray(result.events) ? result.events : []);
    runUi.lastSeq = getLastRenderedSeq(runUi);
    if (activeRunId && !isTerminalRunStatus(result.status)) {
      state.activeRunId = runId;
      showRunWaitingIndicator(runUi);
      void pollRun(runId, conversation.id);
    }
  } catch (error) {
    appendMessage("system", `Run restore failed: ${String(error.message || error)}`);
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
  if (state.activeRunId) {
    appendMessage("system", "Wait for the active run to finish or cancel it first.");
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
    backend = request.backend || "codex";
  } else {
    const prompt = buildComposerPromptForSubmit(promptEl.value);
    const requestPromptSuffix = buildRequestPromptSuffix();
    const explainSelection = String(state.composerExplainSelection || "").trim();
    if (!prompt) {
      appendMessage("system", "Enter a prompt first.");
      return;
    }
    const rewriteIndex = hasRewriteTarget() ? Number(state.rewriteTargetIndex) : null;
    const isRewrite = Number.isInteger(rewriteIndex);
    const forceBrowserAction = state.browserActionArmed === true || state.composerShowMeWhere;
    const browserElementContext = normalizeBrowserElementPayload(state.browserElementContextForNextMessage);
    if (!isRewrite) {
      appendMessage("user", prompt);
    }
    promptEl.value = "";
    resetComposerReadAssistantModes();
    if (state.browserPickerActive) {
      try {
        await sendRuntimeMessage({ type: "assistant.browser.element_picker.cancel" });
      } catch {
        // Ignore picker cancellation failures before run start.
      } finally {
        state.browserPickerActive = false;
        state.browserPickerRequestId = "";
      }
    }

    const includePageContext = isComposerGuidePageContextEnabled();

    request = {
      type: "assistant.run.start",
      backend,
      sessionId: state.sessionId,
      prompt,
      requestPromptSuffix,
      paperContext: getEffectivePaper(),
      includePageContext,
      forceBrowserAction,
      confirmed: false
    };
    if (browserElementContext && browserElementMatchesCurrentPage(browserElementContext)) {
      request.browserElementContext = browserElementContext;
    }
    if (explainSelection) {
      request.highlightContext = {
        kind: "explain_selection",
        selection: explainSelection,
        prompt
      };
    }
    if (isRewrite) {
      request.rewriteMessageIndex = rewriteIndex;
    }
    setBrowserActionArmed(false);
    clearBrowserElementAttachment();
    request = applyLlamaThinkingRequestFields(request, backend);
  }

  clearContextUsageDisplay();
  setBusy(true);
  await submitRun(request);
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

async function stopActiveRequest() {
  if (state.stopping) {
    return;
  }

  const runId = String(state.activeRunId || "").trim();
  if (!runId) {
    return;
  }

  state.stopping = true;
  updateComposerState();

  try {
    await cancelRun(runId);
  } catch (error) {
    state.stopping = false;
    appendMessage("system", `Stop failed: ${String(error.message || error)}`);
    updateComposerState();
    return;
  }
  state.stopping = false;
  updateComposerState();
}

async function submitRun(message) {
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
      throw new Error(result.error || "Failed to start run.");
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
      throw new Error("Broker did not return a run id.");
    }

    state.pendingConfirmation = false;
    state.pendingRequest = null;
    const rewriteIndex = Number.isInteger(message?.rewriteMessageIndex) ? Number(message.rewriteMessageIndex) : null;
    if (message?.paperContext) {
      setCurrentConversationPaper(message.paperContext);
      void refreshPaperState();
    }
    if (rewriteIndex !== null) {
      applyLocalRewritePreview(rewriteIndex, message.prompt);
      clearRewriteTarget();
    }
    if (message?.highlightContext && message?.paperContext) {
      state.highlightAutosaveRunIds.add(runId);
    } else {
      state.highlightAutosaveRunIds.delete(runId);
    }
    state.activeRunId = runId;
    resetBrowserAutomationState();
    setBrowserAutomationStatus("Queued", "Run started. Waiting for model output.");
    const runBackend = typeof result.backend === "string" ? result.backend : message.backend || "codex";
    const runUi = ensureRunUi(runId, state.sessionId, true, runBackend);
    showRunWaitingIndicator(runUi);
    renderBrowserAutomationPanel();
    await refreshHistory(state.sessionId);
    void pollRun(runId, state.sessionId);
  } catch (error) {
    appendMessage("assistant", `Error: ${String(error.message || error)}`);
  } finally {
    setBusy(false);
    updateComposerState();
  }
}

function ensureRunUi(runId, conversationId, allowAssistantBubble, backend = null) {
  let runUi = state.runUi.get(runId);
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
    state.runUi.set(runId, runUi);
    return runUi;
  }
  runUi.conversationId = conversationId;
  runUi.allowAssistantBubble = allowAssistantBubble;
  if (typeof backend === "string" && backend) {
    runUi.backend = backend;
  }
  return runUi;
}

async function pollRun(runId, conversationId, allowAssistantBubble = true) {
  if (state.pollingRuns.has(runId)) {
    return;
  }
  state.pollingRuns.add(runId);

  try {
    while (state.sessionId === conversationId) {
      const runUi = ensureRunUi(runId, conversationId, allowAssistantBubble);
      const result = await sendRuntimeMessage({
        type: "assistant.run.events",
        runId,
        after: runUi.lastSeq,
        timeoutMs: 20_000
      });
      if (!result.ok) {
        throw new Error(result.error || "Failed to poll run events.");
      }
      if (state.sessionId !== conversationId) {
        break;
      }

      renderRunEvents(runId, Array.isArray(result.events) ? result.events : []);
      runUi.lastSeq = getLastRenderedSeq(runUi);

      if (isTerminalRunStatus(result.status)) {
        const shouldGlowHighlights = result.status === "completed" && state.highlightAutosaveRunIds.has(runId);
        state.highlightAutosaveRunIds.delete(runId);
        state.activeRunId = "";
        clearRewriteTarget();
        clearBrowserApprovalsForRun(runId);
        await refreshHistory(state.sessionId);
        await loadConversation(state.sessionId, { preservePaperTab: !allowAssistantBubble });
        if (shouldGlowHighlights) {
          triggerHighlightsTabGlow();
        }
        break;
      }
    }
  } catch (error) {
    const runUi = state.runUi.get(runId);
    if (runUi) {
      clearRunWaitingIndicator(runUi);
    }
    appendMessage("system", `Run event polling failed: ${String(error.message || error)}`);
    state.activeRunId = "";
    clearBrowserApprovalsForRun(runId);
    setBrowserAutomationStatus("Run Error", `Run event polling failed: ${String(error.message || error)}`);
    renderBrowserAutomationPanel();
  } finally {
    state.highlightAutosaveRunIds.delete(runId);
    state.pollingRuns.delete(runId);
    updateComposerState();
  }
}

function renderRunEvents(runId, events) {
  const runUi =
    state.runUi.get(runId) || ensureRunUi(runId, state.sessionId, true);
  let browserAutomationDirty = false;
  for (const event of events) {
    const seq = Number(event?.seq || 0);
    if (!seq || runUi.renderedSeqs.has(seq)) {
      continue;
    }
    runUi.renderedSeqs.add(seq);
    const eventSummary = describeRunEvent(event);
    const eventData = event?.data && typeof event.data === "object" ? event.data : {};

    if (event.type === "partial_text" || event.type === "partial_answer_text") {
      clearRunWaitingIndicator(runUi);
      runUi.streamedAssistantText = String(event?.data?.text || "");
      if (runUi.allowAssistantBubble) {
        upsertRunAssistantBubble(
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
        upsertRunAssistantBubble(
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
      rememberBrowserApproval(runId, eventData);
      setBrowserAutomationStatus(eventSummary.label, eventSummary.text, eventSummary.detail);
      browserAutomationDirty = true;
      appendRunStatusCard(eventSummary);
      continue;
    }

    if (event.type === "approval_decision" || event.type === "approval_granted") {
      updateApprovalCard(runUi, event?.data || {}, event.message || "");
      resolveBrowserApproval(eventData.approval_id, event.message || "Approval updated.");
      setBrowserAutomationStatus(eventSummary.label, eventSummary.text, eventSummary.detail);
      browserAutomationDirty = true;
      appendRunStatusCard(eventSummary);
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
      const hasParsedReasoning = finalReasoningText.trim().length > 0 || finalReasoningBlocks.length > 0;
      if (runUi.allowAssistantBubble && (finalAssistantText || runUi.assistantNode || finalReasoningBlocks.length)) {
        upsertRunAssistantBubble(
          runUi,
          finalAssistantText,
          false,
          finalReasoningBlocks
        );
        runUi.allowAssistantBubble = false;
      }
      disableApprovalCards(runUi);
      clearBrowserApprovalsForRun(runId);
      setBrowserAutomationStatus(eventSummary.label, eventSummary.text, eventSummary.detail);
      browserAutomationDirty = true;
      if (event.type === "completed" && !hasParsedReasoning) {
        runUi.allowAssistantBubble = false;
        continue;
      }
      appendRunStatusCard(eventSummary);
      continue;
    }

    if (event.type === "thinking") {
      setBrowserAutomationStatus(eventSummary.label, eventSummary.text, eventSummary.detail);
      browserAutomationDirty = true;
      if (runUi.allowAssistantBubble && (runUi.streamedAssistantText || runUi.streamedReasoningText || runUi.assistantNode)) {
        upsertRunAssistantBubble(
          runUi,
          runUi.streamedAssistantText,
          true,
          buildPendingReasoningPayload(runUi.streamedReasoningText)
        );
      }
      continue;
    }

    if (event.type === "calling_tool") {
      state.browserAutomationLastToolSummary = event.message || eventData.summary || eventData.tool_name || "";
      setBrowserAutomationStatus(eventSummary.label, eventSummary.text, eventSummary.detail);
      browserAutomationDirty = true;
    }

    if (event.type === "tool_result") {
      if (eventData.success === false) {
        const errorMessage = String(eventData?.error?.message || event.message || "Browser action failed.");
        state.browserAutomationLastToolError = errorMessage;
      } else {
        state.browserAutomationLastToolResult = event.message || "Browser action returned a result.";
      }
      setBrowserAutomationStatus(eventSummary.label, eventSummary.text, eventSummary.detail);
      browserAutomationDirty = true;
    }

    if (event.type === "cancel_requested") {
      setBrowserAutomationStatus(eventSummary.label, eventSummary.text, eventSummary.detail);
      browserAutomationDirty = true;
    }

    if (event.type === "tool_result" || event.type === "calling_tool" || event.type === "thinking" || event.type === "cancel_requested") {
      appendRunStatusCard(eventSummary);
      continue;
    }

    appendRunStatusCard(eventSummary);
  }

  if (browserAutomationDirty) {
    renderBrowserAutomationPanel();
  }
}

function describeRunEvent(event) {
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
      return { ...base, label: "Thinking", text: event.message || "Run started." };
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
      return { ...base, label: "Completed", text: event.message || "Run completed." };
    case "failed":
      return { ...base, label: "Failed", text: event.message || "Run failed." };
    case "cancel_requested":
      return { ...base, label: "Cancel Requested", text: event.message || "Cancellation requested." };
    case "cancelled":
      return { ...base, label: "Cancelled", text: event.message || "Run cancelled." };
    case "blocked_for_review":
      return { ...base, label: "Blocked", text: event.message || "Run blocked for review." };
    default:
      return { ...base, label: "Run", text: event.message || type || "Run event" };
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
    await cancelRun(runUi.runId, card);
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
  const pending = state.browserPendingApprovals.get(String(approvalId || ""));
  if (pending) {
    pending.statusText = decision === "approve" ? "Submitting approval..." : "Submitting denial...";
    pending.resolved = true;
    state.browserPendingApprovals.set(pending.approval_id, pending);
    renderBrowserAutomationPanel();
  }
  try {
    setApprovalCardBusy(card, true, decision === "approve" ? "Submitting approval..." : "Submitting denial...");
    const result = await sendRuntimeMessage({
      type: "assistant.run.approval",
      runId: runUi.runId,
      approvalId,
      decision
    });
    if (!result.ok) {
      throw new Error(result.error || "Approval request failed.");
    }
    updateApprovalCard(runUi, { approval_id: approvalId }, decision === "approve" ? "Approved." : "Denied.");
    resolveBrowserApproval(approvalId, decision === "approve" ? "Approved." : "Denied.");
  } catch (error) {
    setApprovalCardBusy(card, false, `Approval failed: ${String(error.message || error)}`);
    if (pending) {
      pending.statusText = `Approval failed: ${String(error.message || error)}`;
      pending.resolved = false;
      state.browserPendingApprovals.set(pending.approval_id, pending);
      renderBrowserAutomationPanel();
    }
  }
}

async function cancelRun(runId, card = null) {
  try {
    if (card) {
      setApprovalCardBusy(card, true, "Canceling run...");
    }
    const result = await sendRuntimeMessage({
      type: "assistant.run.cancel",
      runId
    });
    if (!result.ok) {
      throw new Error(result.error || "Run cancel failed.");
    }
    state.activeRunId = "";
    clearBrowserApprovalsForRun(runId);
    setBrowserAutomationStatus("Cancelled", "Run canceled.");
    renderBrowserAutomationPanel();
    if (card) {
      setApprovalCardBusy(card, true, "Run canceled.");
    }
  } catch (error) {
    if (card) {
      setApprovalCardBusy(card, false, `Cancel failed: ${String(error.message || error)}`);
    } else {
      appendMessage("system", `Run cancel failed: ${String(error.message || error)}`);
    }
    setBrowserAutomationStatus("Cancel Error", `Run cancel failed: ${String(error.message || error)}`);
    renderBrowserAutomationPanel();
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

function upsertRunAssistantBubble(runUi, text, pending = true, reasoningBlocks = null) {
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

function appendRunStatusCard(eventSummary) {
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
  label.textContent = eventSummary.label || "Run";
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

function isTerminalRunStatus(status) {
  return ["completed", "failed", "cancelled", "blocked_for_review"].includes(String(status || ""));
}

function setBusy(busy) {
  state.busy = busy;
  updateComposerState();
}

function updateComposerState() {
  const locked = state.busy || Boolean(state.activeRunId);
  const hasActiveRun = Boolean(state.activeRunId);
  const showStop = hasActiveRun;
  const llamaSelected = isLlamaChatBackend();
  sendBtn.disabled = locked;
  promptEl.disabled = locked;
  confirmBtn.disabled = state.busy;
  if (llamaEnableThinkingEl) {
    llamaEnableThinkingEl.disabled = locked || !llamaSelected;
  }
  const composerGuideBtn = $("composer-read-guide-btn");
  if (composerGuideBtn) {
    composerGuideBtn.disabled = locked || state.toolsBusy;
  }
  const composerBrowserBtn = $("composer-read-browser-btn");
  if (composerBrowserBtn) {
    composerBrowserBtn.disabled = locked || state.toolsBusy;
  }
  const composerShowBtn = $("composer-read-show-btn");
  if (composerShowBtn) {
    composerShowBtn.disabled = locked || state.toolsBusy;
  }
  updateLlamaThinkingComposer();
  syncReadAssistantQuickActionState();
  stopBtn.classList.toggle("hidden", !showStop);
  stopBtn.disabled = !showStop || state.stopping;
  stopBtn.textContent = state.stopping ? "Stopping..." : hasActiveRun ? "Stop Run" : "Stop";
  if (state.busy) {
    sendBtn.textContent = "Sending...";
  } else if (state.activeRunId) {
    sendBtn.textContent = "Run Active";
  } else if (hasRewriteTarget()) {
    sendBtn.textContent = "Resend Edit";
  } else {
    sendBtn.textContent = "Send";
  }

  const disableEditButtons = locked || state.pendingConfirmation;
  for (const button of messagesEl.querySelectorAll(".message-edit-btn")) {
    button.disabled = disableEditButtons;
  }
  renderBrowserPickerPanel();
  renderBrowserProfilePanel();
  renderBrowserAutomationPanel();
  renderBrowserNextActionIndicator();
}

function showRiskConfirm(flags) {
  riskText.textContent = `High-risk action detected (${flags.join(", ")}). Confirm to continue.`;
  confirmWrap.classList.toggle("hidden", state.activePaperTab !== "chat");
  confirmWrap.setAttribute("aria-hidden", String(state.activePaperTab !== "chat"));
}

function hideRiskConfirm() {
  confirmWrap.classList.add("hidden");
  confirmWrap.setAttribute("aria-hidden", "true");
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
  if (state.busy || state.activeRunId) {
    appendMessage("system", "Finish the active run before editing a prior prompt.");
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

  state.runUi = new Map();
  appendMessage("user", prompt, false, "", messageIndex);
}

