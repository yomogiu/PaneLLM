function normalizeHistoryConversationId(rawId) {
  return String(rawId || "").trim();
}

function getConversationPinStateSet() {
  const source = Array.isArray(state.pinnedConversationIds) ? state.pinnedConversationIds : [];
  const pinnedIds = new Set();
  for (const pinnedId of source) {
    const normalized = normalizeHistoryConversationId(pinnedId);
    if (normalized) {
      pinnedIds.add(normalized);
    }
  }
  return pinnedIds;
}

function isConversationPinned(conversationId) {
  return getConversationPinStateSet().has(normalizeHistoryConversationId(conversationId));
}

function setConversationPinned(conversationId, pinned) {
  const id = normalizeHistoryConversationId(conversationId);
  if (!id) {
    return;
  }
  const pinnedIds = getConversationPinStateSet();
  if (pinned) {
    pinnedIds.add(id);
  } else {
    pinnedIds.delete(id);
  }
  state.pinnedConversationIds = [...pinnedIds];
}

function syncConversationPinnedState() {
  const conversationIds = new Set(
    (Array.isArray(state.conversationList) ? state.conversationList : [])
      .map((conversation) => normalizeHistoryConversationId(conversation?.id))
      .filter(Boolean)
  );
  const pinnedIds = getConversationPinStateSet();
  const nextPinnedIds = [...pinnedIds].filter((id) => conversationIds.has(id));
  if (nextPinnedIds.length !== pinnedIds.size) {
    state.pinnedConversationIds = nextPinnedIds;
  }
}

function prioritizePinnedConversations(conversations = []) {
  const pinnedSet = getConversationPinStateSet();
  if (!Array.isArray(conversations) || conversations.length === 0) {
    return [];
  }
  const seen = new Set();
  const pinned = [];
  const unpinned = [];
  for (const conversation of conversations) {
    const id = normalizeHistoryConversationId(conversation?.id);
    if (!id || seen.has(id)) {
      continue;
    }
    seen.add(id);
    if (pinnedSet.has(id)) {
      pinned.push(conversation);
    } else {
      unpinned.push(conversation);
    }
  }
  return [...pinned, ...unpinned];
}

function renderHistory(selectedId) {
  historyListEl.textContent = "";
  syncConversationPinnedState();
  const visibleWorkspace = getVisiblePaperWorkspace();
  const relatedPaperChats = Array.isArray(visibleWorkspace?.conversations) ? visibleWorkspace.conversations : [];
  const relatedIds = new Set(relatedPaperChats.map((conversation) => String(conversation?.id || "")));
  const remainingChats = state.conversationList.filter((conversation) => !relatedIds.has(String(conversation?.id || "")));
  const activeVersion = normalizePaperVersionLabel(getEffectivePaper()?.paper_version || "");

  if (!relatedPaperChats.length && !state.conversationList.length) {
    const empty = document.createElement("p");
    empty.className = "empty-state";
    empty.textContent = "No saved conversations yet.";
    historyListEl.appendChild(empty);
    return;
  }

  if (activeVersion) {
    const sameVersionChats = relatedPaperChats.filter((conversation) => {
      const paper = getConversationPaperSnapshot(conversation);
      return normalizePaperVersionLabel(paper?.paper_version || "") === activeVersion;
    });
    const samePaperOtherVersionChats = relatedPaperChats.filter((conversation) => {
      const paper = getConversationPaperSnapshot(conversation);
      const version = normalizePaperVersionLabel(paper?.paper_version || "");
      return version !== activeVersion;
    });
    appendHistorySection("This Version", prioritizePinnedConversations(sameVersionChats), selectedId);
    appendHistorySection("Same Paper", prioritizePinnedConversations(samePaperOtherVersionChats), selectedId);
  } else {
    appendHistorySection("This Paper", prioritizePinnedConversations(relatedPaperChats), selectedId);
  }
  const otherChats = remainingChats.length ? remainingChats : state.conversationList;
  appendHistorySection(relatedPaperChats.length ? "All Chats" : "Chats", prioritizePinnedConversations(otherChats), selectedId);
}

async function loadConversation(sessionId, options = {}) {
  const preservePaperTab = options.preservePaperTab === true;
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
    const loadedConversationId = conversation.id;

    state.sessionId = conversation.id;
    state.pendingConfirmation = false;
    state.pendingRequest = null;
    state.stopping = false;
    state.activeRunId = "";
    state.runUi = new Map();
    state.rewriteTargetIndex = null;
    setBrowserActionArmed(false);
    clearBrowserElementAttachment();
    resetBrowserAutomationState();
    resetComposerReadAssistantModes();
    const codex = conversation?.codex && typeof conversation.codex === "object" ? conversation.codex : {};
    setComposerGuidePageContext(
      String(codex.page_context_enabled || "").toLowerCase() === "true"
    );
    setCurrentConversationPaper(conversation.paper);
    hideRiskConfirm();
    renderConversationMessages(conversation.messages);
    setPaperTab(preservePaperTab ? state.activePaperTab : "chat");
    await refreshPaperState();
    if (state.sessionId !== loadedConversationId) {
      return;
    }
    await restoreRun(conversation);
  } catch (error) {
    appendMessage("system", `Conversation load failed: ${String(error.message || error)}`);
  } finally {
    updateComposerState();
  }
}

async function deleteConversationById(conversationId) {
  const id = normalizeHistoryConversationId(conversationId);
  const exists = state.conversationList.some((item) => normalizeHistoryConversationId(item?.id) === id);
  if (!exists) {
    if (id === normalizeHistoryConversationId(state.sessionId)) {
      startNewSession("Current chat is unsaved. Started a new chat.");
    } else {
      appendMessage("system", "Conversation not found.");
    }
    return;
  }
  const selectedConversation = state.conversationList.find((item) => normalizeHistoryConversationId(item?.id) === id) || null;
  const title = selectedConversation ? getConversationHistoryTitle(selectedConversation) : "this conversation";
  const accepted = await requestActionConfirm({
    title: "Delete Chat?",
    text: `Delete "${title}" permanently? This cannot be undone.`,
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
    setConversationPinned(id, false);
    await refreshHistory();
    if (id === state.sessionId) {
      if (state.conversationList.length > 0) {
        await loadConversation(state.conversationList[0].id);
      } else {
        startNewSession("Conversation deleted.");
      }
    }
  } catch (error) {
    appendMessage("system", `Delete failed: ${String(error.message || error)}`);
  }
}

async function deleteCurrentConversation() {
  await deleteConversationById(state.sessionId);
}

function toggleHistoryConversationPin(conversationId) {
  const id = normalizeHistoryConversationId(conversationId);
  if (!id) {
    return;
  }
  setConversationPinned(id, !isConversationPinned(id));
}

function getConversationBubbleCount() {
  return messagesEl?.querySelectorAll(".message.user, .message.assistant").length || 0;
}

async function maybeRequestPaperSummaryBeforeNewChat() {
  const paper = getConversationWorkspacePaper();
  if (!paper || paper.source !== "arxiv" || getConversationBubbleCount() <= 0) {
    return false;
  }
  const accepted = await requestActionConfirm({
    title: "Update Paper Summary?",
    text: `You're starting a new chat for ${getPaperStatusLabel(paper)}. Mark the paper summary for refresh too?`,
    confirmLabel: "Mark Summary"
  });
  if (!accepted) {
    return false;
  }
  const result = await sendRuntimeMessage({
    type: "assistant.paper.summary_request",
    paper,
    conversationId: state.sessionId
  });
  if (!result.ok) {
    throw new Error(result.error || "Failed to mark paper summary.");
  }
  state.paperState = {
    paper: normalizePaperPayload(result.paper),
    conversations: Array.isArray(result.conversations) ? result.conversations : [],
    memory: normalizePaperMemoryMetadata(result.memory)
  };
  renderPaperWorkspace();
  renderHistory(state.sessionId);
  return true;
}

async function requestPaperSummaryGeneration() {
  const paper = getEffectivePaper();
  if (!paper || paper.source !== "arxiv") {
    appendMessage("system", "Open an arXiv paper before generating a paper summary.");
    return;
  }
  if (state.activeRunId) {
    appendMessage("system", "Wait for the active run to finish or cancel it first.");
    return;
  }

  hideRiskConfirm();
  setBusy(true);
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.paper.summary_generate",
      paper,
      sessionId: state.sessionId,
      backend: backendEl?.value || "codex"
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to generate paper summary.");
    }

    state.paperState = {
      paper: normalizePaperPayload(result.paper),
      conversations: Array.isArray(result.conversations) ? result.conversations : [],
      memory: normalizePaperMemoryMetadata(result.memory)
    };
    renderPaperWorkspace();
    renderHistory(state.sessionId);

    const runId = typeof result.run_id === "string" ? result.run_id : "";
    if (!runId) {
      throw new Error("Broker did not return a run id.");
    }
    state.pendingConfirmation = false;
    state.pendingRequest = null;
    state.activeRunId = runId;
    const runBackend = typeof result.backend === "string" ? result.backend : backendEl?.value || "codex";
    ensureRunUi(runId, state.sessionId, false, runBackend);
    await refreshHistory(state.sessionId);
    void pollRun(runId, state.sessionId, false);
  } catch (error) {
    appendMessage("system", `Paper summary generation failed: ${String(error.message || error)}`);
    await refreshPaperState();
  } finally {
    setBusy(false);
    updateComposerState();
  }
}

async function handleStartNewSession() {
  if (state.activeRunId) {
    appendMessage("system", "Wait for the active run to finish or cancel it first.");
    return;
  }
  try {
    const markedSummary = await maybeRequestPaperSummaryBeforeNewChat();
    let message = "Started a new chat.";
    if (markedSummary) {
      message = "Started a new chat. Paper summary marked for refresh.";
    }
    startNewSession(message);
  } catch (error) {
    appendMessage("system", `Paper workspace save failed: ${String(error.message || error)}`);
  }
}

function startNewSession(message = "Started a new chat.") {
  const carryPaper = getEffectivePaper();
  resolveActionConfirm(false);
  state.sessionId = crypto.randomUUID();
  state.pendingConfirmation = false;
  state.pendingRequest = null;
  state.stopping = false;
  state.activeRunId = "";
  state.runUi = new Map();
  state.rewriteTargetIndex = null;
  state.currentConversationPaper = null;
  setBrowserActionArmed(false);
  clearBrowserElementAttachment();
  resetBrowserAutomationState();
  if (!state.activeBrowserPaper && carryPaper) {
    state.activeBrowserPaper = carryPaper;
  }
  state.paperState = null;
  resetComposerReadAssistantModes();
  hideRiskConfirm();
  clearContextUsageDisplay();
  clearMessages();
  appendMessage("system", message);
  setPaperTab("chat");
  renderPaperWorkspace();
  renderHistory(state.sessionId);
  updateComposerState();
  void refreshPaperState();
}
