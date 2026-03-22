const $ = (id) => document.getElementById(id);
const SAFE_LINK_PROTOCOLS = new Set(["http:", "https:"]);
const EXPLAIN_SELECTION_DEFAULT_PROMPT = "Explain the selected passage in plain language.";
const HIGHLIGHTS_TAB_GLOW_MS = 1_600;
const FONT_SCALE_STORAGE_KEY = "ui_font_scale";
const DEFAULT_FONT_SCALE = 1;
const MIN_FONT_SCALE = 0.9;
const MAX_FONT_SCALE = 1.3;
const FONT_SCALE_STEP = 0.1;
const GUIDE_PAGE_CONTEXT_FOLLOWUP = [
  "Treat the current page as the subject of the user's request.",
  "Interpret references like 'it', 'this', and 'the paper' as the current page unless the user explicitly says otherwise.",
  "Answer using only the captured page context. If the context is insufficient, say so instead of switching to repository or prior-chat context."
].join("\n");
const SHOW_ME_WHERE_FOLLOWUP =
  "After answering, use browser tools to scroll to and temporarily highlight the section of the current page that best answers the request above.";

const state = {
  sessionId: crypto.randomUUID(),
  pendingConfirmation: false,
  pendingRequest: null,
  conversationList: [],
  historyOpen: false,
  historyPinned: false,
  brokerHealth: null,
  availableBackends: [],
  busy: false,
  stopping: false,
  activeRunId: "",
  actionConfirmResolver: null,
  runUi: new Map(),
  pollingRuns: new Set(),
  highlightAutosaveRunIds: new Set(),
  highlightsTabGlowTimer: 0,
  rewriteTargetIndex: null,
  composerExplainSelection: "",
  composerShowMeWhere: false,
  composerGuidePageContext: false,
  activeMainTab: "chat",
  activePaperTab: "chat",
  fontScale: DEFAULT_FONT_SCALE,
  currentConversationPaper: null,
  activeBrowserPaper: null,
  pendingPaperConversationRestoreKey: "",
  restoringPaperConversation: false,
  paperState: null,
  paperMemoryResults: [],
  paperMemoryQuery: "",
  paperMemoryVersion: "",
  paperMemoryLoading: false,
  paperMemoryError: "",
  paperMemoryRequestKey: "",
  toolsBusy: false,
  toolsPolicy: null,
  toolsActiveTab: null,
  toolsBrowserConfig: null,
  readContext: null,
  pollTimers: {
    activeTabRefresh: 0
  }
};

const appEl = document.querySelector(".app");
const brokerStatusEl = $("broker-status");
const contextUsageEl = $("context-usage");
const fontScaleDownBtn = $("font-scale-down-btn");
const fontScaleUpBtn = $("font-scale-up-btn");
const messagesEl = $("messages");
const emptyStateEl = $("empty-state");
const historyToggleBtn = $("history-toggle-btn");
const historyCloseBtn = $("history-close-btn");
const historyPinBtn = $("history-pin-btn");
const historyBackdropEl = $("history-backdrop");
const historyPanelEl = $("history-panel");
const historyListEl = $("history-list");
const promptEl = $("prompt");
const composerEl = document.querySelector(".composer");
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
const toolsTabBtn = $("tools-tab-btn");
const paperSummaryTabBtn = $("paper-summary-tab-btn");
const paperHighlightsTabBtn = $("paper-highlights-tab-btn");
const paperMemoryTabBtn = $("paper-memory-tab-btn");
const paperChatTabBtn = $("paper-chat-tab-btn");
const chatViewEl = $("chat-view");
const toolsViewEl = $("tools-view");
const paperContextTitleEl = $("paper-context-title");
const paperContextMetaEl = $("paper-context-meta");
const paperContextBadgeEl = $("paper-context-badge");
const paperSummaryViewEl = $("paper-summary-view");
const paperHighlightsViewEl = $("paper-highlights-view");
const paperMemoryViewEl = $("paper-memory-view");
const paperSummaryBodyEl = $("paper-summary-body");
const paperHighlightsBodyEl = $("paper-highlights-body");
const paperMemorySearchEl = $("paper-memory-search");
const paperMemorySearchBtn = $("paper-memory-search-btn");
const paperMemoryResultsEl = $("paper-memory-results");
const toolsRefreshBtn = $("tools-refresh-btn");
const toolsPolicyStatusEl = $("tools-policy-status");
const toolsHostInputEl = $("tools-host-input");
const toolsAllowBtn = $("tools-allow-btn");
const toolsAllowActiveBtn = $("tools-allow-active-btn");
const toolsAgentMaxStepsEl = $("tools-agent-max-steps");
const toolsBrowserApplyBtn = $("tools-browser-apply-btn");
const toolsAllowedListEl = $("tools-allowed-list");

const ARXIV_HOSTS = new Set(["arxiv.org", "www.arxiv.org"]);
const ARXIV_ROUTE_PREFIXES = new Set(["abs", "pdf", "html"]);

function normalizePaperVersionLabel(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) {
    return "";
  }
  const match = raw.match(/^v?(\d+)$/i);
  if (match) {
    return `v${match[1]}`;
  }
  const suffixMatch = raw.match(/v(\d+)$/i);
  if (suffixMatch) {
    return `v${suffixMatch[1]}`;
  }
  return raw;
}

function compactInlineText(value, limit = 160) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (!text) {
    return "";
  }
  if (text.length <= limit) {
    return text;
  }
  return `${text.slice(0, Math.max(0, limit - 1)).trimEnd()}…`;
}

function canonicalizeArxivIdentifier(value) {
  const raw = String(value || "").trim().replace(/\.pdf$/i, "").replace(/^\/+|\/+$/g, "");
  return raw.replace(/v\d+$/i, "");
}

function extractArxivVersion(value) {
  const raw = String(value || "").trim().replace(/\.pdf$/i, "").replace(/^\/+|\/+$/g, "");
  const match = raw.match(/v(\d+)$/i);
  return match ? `v${match[1]}` : "";
}

function buildArxivVersionedUrl(parsed, paperId, paperVersion) {
  const version = normalizePaperVersionLabel(paperVersion);
  if (!version) {
    return "";
  }
  const route = String(parsed?.pathname || "")
    .split("/")
    .map((part) => part.trim())
    .filter(Boolean)[0]
    ?.toLowerCase();
  if (!route || !ARXIV_ROUTE_PREFIXES.has(route)) {
    return `https://arxiv.org/abs/${paperId}${version}`;
  }
  const isPdf = route === "pdf" || /\.pdf$/i.test(String(parsed?.pathname || ""));
  return `https://arxiv.org/${route}/${paperId}${version}${isPdf ? ".pdf" : ""}`;
}

function extractArxivPaperFromUrl(rawUrl, title = "") {
  let parsed;
  try {
    parsed = new URL(String(rawUrl || "").trim());
  } catch {
    return null;
  }
  if (!ARXIV_HOSTS.has(String(parsed.hostname || "").trim().toLowerCase())) {
    return null;
  }
  const parts = parsed.pathname
    .split("/")
    .map((part) => part.trim())
    .filter(Boolean);
  if (parts.length < 2 || !ARXIV_ROUTE_PREFIXES.has(parts[0].toLowerCase())) {
    return null;
  }
  const paperPath = parts.slice(1).join("/");
  const paperId = canonicalizeArxivIdentifier(paperPath);
  if (!paperId || !/^[A-Za-z0-9._/-]{1,128}$/.test(paperId)) {
    return null;
  }
  const paperVersion = normalizePaperVersionLabel(extractArxivVersion(paperPath));
  return {
    source: "arxiv",
    paper_id: paperId,
    canonical_url: `https://arxiv.org/abs/${paperId}`,
    paper_version: paperVersion,
    versioned_url: paperVersion ? buildArxivVersionedUrl(parsed, paperId, paperVersion) : "",
    title: String(title || "").trim()
  };
}

function coalescePaperPayload(value) {
  if (!value || typeof value !== "object") {
    return {};
  }
  const raw = value;
  const nestedPaper = raw.paper && typeof raw.paper === "object" ? raw.paper : {};
  const nestedCodex = raw.codex && typeof raw.codex === "object" ? raw.codex : {};
  return { ...nestedPaper, ...nestedCodex, ...raw };
}

function normalizePaperHighlightPayload(value) {
  if (typeof value === "string") {
    const text = value.trim();
    return text ? { kind: "legacy_text", text } : null;
  }
  if (!value || typeof value !== "object") {
    return null;
  }
  const raw = value;
  const kind = String(raw.kind || "").trim().toLowerCase();
  if (kind !== "explain_selection") {
    return null;
  }
  const selection = String(raw.selection || "").trim();
  const prompt = String(raw.prompt || "").trim();
  const response = String(raw.response || "").trim();
  if (!selection || !response) {
    return null;
  }
  return {
    kind: "explain_selection",
    selection,
    prompt,
    response,
    paper_version: normalizePaperVersionLabel(raw.paper_version || raw.paperVersion || ""),
    conversation_id: String(raw.conversation_id || raw.conversationId || "").trim(),
    created_at: String(raw.created_at || raw.createdAt || "").trim()
  };
}

function normalizePaperPayload(value) {
  if (!value || typeof value !== "object") {
    return null;
  }
  const raw = coalescePaperPayload(value);
  const source = String(raw.source || raw.paper_source || raw.paperSource || "").trim().toLowerCase();
  const paperId = String(raw.paper_id || raw.paperId || "").trim();
  const canonicalUrl = String(
    raw.canonical_url || raw.canonicalUrl || raw.url || raw.paper_url || raw.paperUrl || ""
  ).trim();
  const title = String(raw.title || "").trim();
  const summary = String(raw.summary || "").trim();
  const summaryStatus = String(raw.summary_status || raw.summaryStatus || "idle").trim().toLowerCase() || "idle";
  const summaryRequestedAt = String(raw.summary_requested_at || raw.summaryRequestedAt || "").trim();
  const lastSummaryConversationId = String(
    raw.last_summary_conversation_id || raw.lastSummaryConversationId || ""
  ).trim();
  const summaryError = String(raw.summary_error || raw.summaryError || "").trim();
  const paperVersion = normalizePaperVersionLabel(raw.paper_version || raw.paperVersion || "");
  const versionedUrl = String(
    raw.versioned_url || raw.versionedUrl || raw.paper_version_url || raw.paperVersionUrl || ""
  ).trim();
  const derivedVersion = paperVersion || normalizePaperVersionLabel(extractArxivVersion(versionedUrl));
  const normalizedVersionedUrl = versionedUrl || (derivedVersion && paperId ? `https://arxiv.org/abs/${paperId}${derivedVersion}` : "");
  const observedVersions = Array.isArray(raw.observed_versions || raw.observedVersions)
    ? (raw.observed_versions || raw.observedVersions)
      .map((item) => normalizePaperVersionLabel(item))
      .filter(Boolean)
    : [];
  const highlights = Array.isArray(raw.highlights)
    ? raw.highlights
      .map((item) => normalizePaperHighlightPayload(item))
      .filter(Boolean)
    : [];
  if (source && paperId) {
    return {
      source,
      paper_id: paperId,
      canonical_url: canonicalUrl,
      title,
      summary,
      summary_status: summaryStatus,
      summary_requested_at: summaryRequestedAt,
      last_summary_conversation_id: lastSummaryConversationId,
      summary_error: summaryError,
      paper_version: derivedVersion,
      versioned_url: normalizedVersionedUrl,
      observed_versions: observedVersions,
      last_summary_version: normalizePaperVersionLabel(raw.last_summary_version || raw.lastSummaryVersion || ""),
      highlights
    };
  }
  if (canonicalUrl) {
    return extractArxivPaperFromUrl(canonicalUrl, title);
  }
  return null;
}

function normalizePaperMemoryMetadata(value) {
  const raw = value && typeof value === "object" ? value : {};
  const countsRaw = raw.counts_by_version && typeof raw.counts_by_version === "object"
    ? raw.counts_by_version
    : raw.countsByVersion && typeof raw.countsByVersion === "object"
      ? raw.countsByVersion
      : {};
  const countsByVersion = {};
  for (const [key, count] of Object.entries(countsRaw)) {
    const version = normalizePaperVersionLabel(key);
    const numericCount = Number(count);
    if (version && Number.isFinite(numericCount) && numericCount > 0) {
      countsByVersion[version] = Math.max(0, Math.trunc(numericCount));
    }
  }
  return {
    default_version: normalizePaperVersionLabel(raw.default_version || raw.defaultVersion || ""),
    counts_by_version: countsByVersion,
    has_unversioned: Boolean(raw.has_unversioned ?? raw.hasUnversioned),
    latest_updated_at: String(raw.latest_updated_at || raw.latestUpdatedAt || "").trim()
  };
}

function normalizePaperMemoryResultPayload(value) {
  if (!value || typeof value !== "object") {
    return null;
  }
  const raw = value;
  const kind = String(raw.kind || "").trim().toLowerCase();
  if (!["summary", "highlight", "conversation"].includes(kind)) {
    return null;
  }
  const title = String(raw.title || "").trim();
  const snippet = String(raw.snippet || "").trim();
  if (!title && !snippet) {
    return null;
  }
  return {
    id: String(raw.id || "").trim(),
    kind,
    paper_version: normalizePaperVersionLabel(raw.paper_version || raw.paperVersion || ""),
    title,
    snippet,
    conversation_id: String(raw.conversation_id || raw.conversationId || "").trim(),
    updated_at: String(raw.updated_at || raw.updatedAt || "").trim(),
    source_label: String(raw.source_label || raw.sourceLabel || "").trim()
  };
}

function papersEqual(left, right) {
  const leftPaper = normalizePaperPayload(left);
  const rightPaper = normalizePaperPayload(right);
  if (!leftPaper && !rightPaper) {
    return true;
  }
  if (!leftPaper || !rightPaper) {
    return false;
  }
  return leftPaper.source === rightPaper.source && leftPaper.paper_id === rightPaper.paper_id;
}

function getPaperKey(paper) {
  const normalized = normalizePaperPayload(paper);
  if (!normalized) {
    return "";
  }
  return `${normalized.source}:${normalized.paper_id}`;
}

function getPaperMemoryVersion(paper = getEffectivePaper(), paperState = state.paperState) {
  const normalized = normalizePaperPayload(paper);
  const memory = normalizePaperMemoryMetadata(paperState?.memory);
  return normalizePaperVersionLabel(normalized?.paper_version || memory.default_version || "");
}

function getPaperMemoryCountForVersion(paper, paperState = state.paperState) {
  const version = getPaperMemoryVersion(paper, paperState);
  if (!version) {
    return 0;
  }
  const memory = normalizePaperMemoryMetadata(paperState?.memory);
  return Number(memory.counts_by_version?.[version] || 0);
}

function resetPaperMemoryState() {
  state.paperMemoryResults = [];
  state.paperMemoryQuery = "";
  state.paperMemoryVersion = "";
  state.paperMemoryLoading = false;
  state.paperMemoryError = "";
  state.paperMemoryRequestKey = "";
}

function hasResolvedActiveTab() {
  return Boolean(String(state.toolsActiveTab?.url || "").trim());
}

function getEffectivePaper() {
  if (hasResolvedActiveTab()) {
    return normalizePaperPayload(state.activeBrowserPaper);
  }
  return normalizePaperPayload(state.currentConversationPaper);
}

function getConversationWorkspacePaper() {
  const conversationPaper = normalizePaperPayload(state.currentConversationPaper);
  const activePaper = normalizePaperPayload(state.activeBrowserPaper);
  if (conversationPaper && activePaper && papersEqual(conversationPaper, activePaper)) {
    return {
      ...conversationPaper,
      title: conversationPaper.title || activePaper.title,
      canonical_url: conversationPaper.canonical_url || activePaper.canonical_url,
      paper_version: conversationPaper.paper_version || activePaper.paper_version,
      versioned_url: conversationPaper.versioned_url || activePaper.versioned_url
    };
  }
  return conversationPaper || activePaper;
}

function getPaperDisplayTitle(paper) {
  const normalized = normalizePaperPayload(paper);
  if (!normalized) {
    return "No active arXiv paper.";
  }
  return normalized.title || `arXiv:${normalized.paper_id}`;
}

function getPaperStatusLabel(paper) {
  const normalized = normalizePaperPayload(paper);
  if (!normalized) {
    return "";
  }
  const version = normalized.paper_version ? ` ${normalized.paper_version}` : "";
  return normalized.source === "arxiv" ? `arXiv:${normalized.paper_id}${version}` : normalized.paper_id;
}

function getVisiblePaperWorkspace(paper = getEffectivePaper()) {
  const normalizedPaper = normalizePaperPayload(paper);
  const workspacePaper = normalizePaperPayload(state.paperState?.paper);
  if (!normalizedPaper || !workspacePaper || !papersEqual(normalizedPaper, workspacePaper)) {
    return null;
  }
  return state.paperState && typeof state.paperState === "object" ? state.paperState : null;
}

function getPaperMetaText(paper, paperState = state.paperState) {
  const normalized = normalizePaperPayload(paper);
  if (!normalized) {
    if (hasResolvedActiveTab()) {
      return "Open an arXiv paper to use the paper workspace for the current page.";
    }
    return "Open an arXiv paper or load a paper-linked chat to bring back related sessions.";
  }
  const workspace = paperState && typeof paperState === "object" ? paperState : null;
  const relatedCount = Array.isArray(workspace?.conversations) ? workspace.conversations.length : 0;
  const memoryCount = getPaperMemoryCountForVersion(normalized, workspace);
  const memoryVersion = getPaperMemoryVersion(normalized, workspace);
  const summaryStatus = String(workspace?.paper?.summary_status || normalized.summary_status || "idle").trim().toLowerCase();
  if (summaryStatus === "requested") {
    return memoryCount > 0 && memoryVersion
      ? `Summary requested. ${relatedCount} related chat${relatedCount === 1 ? "" : "s"} linked to this paper. ${memoryCount} memory item${memoryCount === 1 ? "" : "s"} available for ${memoryVersion}.`
      : `Summary requested. ${relatedCount} related chat${relatedCount === 1 ? "" : "s"} linked to this paper.`;
  }
  if (relatedCount > 0 || memoryCount > 0) {
    if (memoryCount > 0 && memoryVersion) {
      return `${relatedCount} related chat${relatedCount === 1 ? "" : "s"} found for this paper. ${memoryCount} memory item${memoryCount === 1 ? "" : "s"} ready for ${memoryVersion}.`;
    }
    return `${relatedCount} related chat${relatedCount === 1 ? "" : "s"} found for this paper.`;
  }
  return "This paper is ready for a focused workspace. Start chatting to create the first linked session.";
}

function renderPaperMemoryPanel() {
  if (paperMemorySearchEl) {
    paperMemorySearchEl.value = state.paperMemoryQuery;
  }
  if (!paperMemoryResultsEl) {
    return;
  }
  paperMemoryResultsEl.textContent = "";

  const paper = getEffectivePaper();
  const version = getPaperMemoryVersion(paper);
  if (paperMemorySearchEl) {
    paperMemorySearchEl.disabled = state.paperMemoryLoading || !paper;
  }
  if (paperMemorySearchBtn) {
    paperMemorySearchBtn.disabled = state.paperMemoryLoading || !paper;
  }

  if (!paper) {
    const empty = document.createElement("p");
    empty.className = "paper-body-copy";
    empty.textContent = "Open an arXiv paper to search paper memory.";
    paperMemoryResultsEl.appendChild(empty);
    return;
  }
  if (!version) {
    const empty = document.createElement("p");
    empty.className = "paper-body-copy";
    empty.textContent = "No version-specific memory is available for this paper yet.";
    paperMemoryResultsEl.appendChild(empty);
    return;
  }
  if (state.paperMemoryLoading) {
    const loading = document.createElement("p");
    loading.className = "paper-body-copy";
    loading.textContent = `Loading paper memory for ${version}...`;
    paperMemoryResultsEl.appendChild(loading);
    return;
  }
  if (state.paperMemoryError) {
    const error = document.createElement("p");
    error.className = "paper-body-copy";
    error.textContent = state.paperMemoryError;
    paperMemoryResultsEl.appendChild(error);
    return;
  }
  if (!state.paperMemoryResults.length) {
    const empty = document.createElement("p");
    empty.className = "paper-body-copy";
    empty.textContent = state.paperMemoryQuery
      ? "No paper memory matched that query."
      : `No version-specific memory stored yet for ${version}.`;
    paperMemoryResultsEl.appendChild(empty);
    return;
  }

  const list = document.createElement("div");
  list.className = "paper-memory-results";
  for (const item of state.paperMemoryResults) {
    const card = document.createElement("article");
    card.className = "paper-memory-card";

    const header = document.createElement("div");
    header.className = "paper-memory-card-header";

    const copy = document.createElement("div");
    copy.className = "paper-memory-card-copy";

    const title = document.createElement("p");
    title.className = "paper-memory-card-title";
    title.textContent = item.title || item.source_label || "Paper memory";
    copy.appendChild(title);

    const badges = document.createElement("div");
    badges.className = "paper-memory-badges";

    const kindBadge = document.createElement("span");
    kindBadge.className = "status-badge paper-memory-kind-badge";
    kindBadge.textContent = item.source_label || item.kind;
    badges.appendChild(kindBadge);

    if (item.paper_version) {
      const versionBadge = document.createElement("span");
      versionBadge.className = "status-badge paper-highlight-version-badge";
      versionBadge.textContent = item.paper_version;
      badges.appendChild(versionBadge);
    }

    header.appendChild(copy);
    header.appendChild(badges);
    card.appendChild(header);

    const snippet = document.createElement("p");
    snippet.className = "paper-memory-card-snippet";
    snippet.textContent = item.snippet;
    card.appendChild(snippet);

    if (item.kind === "conversation" && item.conversation_id) {
      const actions = document.createElement("div");
      actions.className = "button-row";
      const openBtn = document.createElement("button");
      openBtn.type = "button";
      openBtn.className = "ghost small";
      openBtn.textContent = "Open Chat";
      openBtn.addEventListener("click", async () => {
        await loadConversation(item.conversation_id);
      });
      actions.appendChild(openBtn);
      card.appendChild(actions);
    }

    list.appendChild(card);
  }
  paperMemoryResultsEl.appendChild(list);
}

function renderPaperWorkspace() {
  const paper = getEffectivePaper();
  const visibleWorkspace = getVisiblePaperWorkspace(paper);
  if (paperContextTitleEl) {
    paperContextTitleEl.textContent = getPaperDisplayTitle(paper);
  }
  if (paperContextMetaEl) {
    paperContextMetaEl.textContent = getPaperMetaText(paper, visibleWorkspace);
  }
  if (paperContextBadgeEl) {
    if (paper) {
      paperContextBadgeEl.classList.remove("hidden");
      paperContextBadgeEl.textContent = getPaperStatusLabel(paper);
    } else {
      paperContextBadgeEl.classList.add("hidden");
      paperContextBadgeEl.textContent = "Paper";
    }
  }

  if (paperSummaryBodyEl) {
    paperSummaryBodyEl.textContent = "";
    const workspacePaper = normalizePaperPayload(visibleWorkspace?.paper) || paper;
    const heading = document.createElement("p");
    heading.className = "paper-body-heading";
    heading.textContent = "Summary";
    paperSummaryBodyEl.appendChild(heading);

    const body = document.createElement("p");
    body.className = "paper-body-copy";
    const summaryText = String(visibleWorkspace?.paper?.summary || "").trim();
    const summaryStatus = String(visibleWorkspace?.paper?.summary_status || "").trim().toLowerCase();
    const summaryError = String(visibleWorkspace?.paper?.summary_error || "").trim();
    const summaryVersion = normalizePaperVersionLabel(visibleWorkspace?.paper?.last_summary_version || "");
    if (summaryText) {
      body.textContent = summaryText;
    } else if (summaryStatus === "requested" && workspacePaper) {
      body.textContent = `Summary requested for ${getPaperDisplayTitle(workspacePaper)}.`;
    } else if (summaryStatus === "error" && summaryError) {
      body.textContent = summaryError;
    } else if (workspacePaper) {
      body.textContent = "No summary yet. Generate one from the current page context.";
    } else {
      body.textContent = "Open an arXiv paper to create a paper summary workspace.";
    }
    paperSummaryBodyEl.appendChild(body);

    const noteText = summaryText
      ? (summaryStatus === "requested"
        ? "Refreshing summary from the current page."
        : summaryStatus === "error" && summaryError
          ? `Last refresh failed: ${summaryError}`
          : "")
      : "";
    if (noteText) {
      const note = document.createElement("p");
      note.className = "paper-body-meta";
      note.textContent = noteText;
      paperSummaryBodyEl.appendChild(note);
    }
    if (summaryVersion) {
      const provenance = document.createElement("p");
      provenance.className = "paper-body-meta paper-summary-provenance";
      provenance.textContent = `Summary generated from ${summaryVersion}.`;
      paperSummaryBodyEl.appendChild(provenance);
    }

    if (workspacePaper) {
      const actions = document.createElement("div");
      actions.className = "button-row paper-summary-actions";

      const refreshBtn = document.createElement("button");
      refreshBtn.type = "button";
      refreshBtn.className = "ghost small";
      refreshBtn.textContent = state.activeRunId && summaryStatus === "requested"
        ? "Generating Summary..."
        : summaryText
          ? "Refresh Summary"
          : "Generate Summary";
      refreshBtn.disabled = state.busy || Boolean(state.activeRunId);
      refreshBtn.addEventListener("click", async () => {
        await requestPaperSummaryGeneration();
      });
      actions.appendChild(refreshBtn);
      paperSummaryBodyEl.appendChild(actions);
    }
  }

  if (paperHighlightsBodyEl) {
    paperHighlightsBodyEl.textContent = "";
    const heading = document.createElement("p");
    heading.className = "paper-body-heading";
    heading.textContent = "Highlights";
    paperHighlightsBodyEl.appendChild(heading);

    const highlights = Array.isArray(visibleWorkspace?.paper?.highlights) ? visibleWorkspace.paper.highlights : [];
    if (highlights.length > 0) {
      const list = document.createElement("div");
      list.className = "paper-highlight-stack";
      for (const highlight of highlights) {
        if (highlight && typeof highlight === "object" && highlight.kind === "explain_selection") {
          const card = document.createElement("article");
          card.className = "paper-highlight-card";

          const header = document.createElement("div");
          header.className = "paper-highlight-header";

          const label = document.createElement("p");
          label.className = "paper-highlight-label";
          label.textContent = "Explain Selection";
          header.appendChild(label);

          const version = normalizePaperVersionLabel(highlight.paper_version || "");
          if (version) {
            const badge = document.createElement("span");
            badge.className = "status-badge paper-highlight-version-badge";
            badge.textContent = version;
            header.appendChild(badge);
          }

          card.appendChild(header);

          const selection = document.createElement("blockquote");
          selection.className = "paper-highlight-selection";
          selection.textContent = highlight.selection;
          card.appendChild(selection);

          if (highlight.prompt) {
            const prompt = document.createElement("p");
            prompt.className = "paper-highlight-prompt";
            prompt.textContent = `Ask: ${highlight.prompt}`;
            card.appendChild(prompt);
          }

          const response = document.createElement("p");
          response.className = "paper-highlight-response";
          response.textContent = highlight.response;
          card.appendChild(response);
          list.appendChild(card);
          continue;
        }

        const item = document.createElement("p");
        item.className = "paper-highlight-legacy";
        item.textContent = String(highlight?.text || "");
        list.appendChild(item);
      }
      paperHighlightsBodyEl.appendChild(list);
    } else {
      const body = document.createElement("p");
      body.className = "paper-body-copy";
      body.textContent = paper
        ? "No explain-selection highlights stored yet for this paper."
        : "Open an arXiv paper to keep paper-specific highlights here.";
      paperHighlightsBodyEl.appendChild(body);
    }
  }

  renderPaperMemoryPanel();
}

function setPaperTab(tab) {
  const next = tab === "summary" || tab === "highlights" || tab === "memory" ? tab : "chat";
  state.activePaperTab = next;

  const tabConfig = [
    { key: "chat", button: paperChatTabBtn, panel: null },
    { key: "highlights", button: paperHighlightsTabBtn, panel: paperHighlightsViewEl },
    { key: "memory", button: paperMemoryTabBtn, panel: paperMemoryViewEl },
    { key: "summary", button: paperSummaryTabBtn, panel: paperSummaryViewEl }
  ];

  for (const config of tabConfig) {
    const active = config.key === next;
    config.button?.classList.toggle("active", active);
    config.button?.setAttribute("aria-selected", String(active));
    if (config.panel) {
      config.panel.classList.toggle("hidden", !active);
      config.panel.setAttribute("aria-hidden", String(!active));
    }
  }

  const chatVisible = next === "chat";
  messagesEl?.classList.toggle("hidden", !chatVisible);
  messagesEl?.setAttribute("aria-hidden", String(!chatVisible));
  confirmWrap?.classList.toggle("hidden", !chatVisible || !state.pendingConfirmation);
  confirmWrap?.setAttribute("aria-hidden", String(!chatVisible || !state.pendingConfirmation));
  composerEl?.classList.toggle("hidden", !chatVisible);
  composerEl?.setAttribute("aria-hidden", String(!chatVisible));
  if (next === "memory") {
    void refreshPaperMemory(false);
  }
}

function triggerHighlightsTabGlow() {
  if (!paperHighlightsTabBtn) {
    return;
  }
  paperHighlightsTabBtn.classList.remove("saved-glow");
  window.clearTimeout(state.highlightsTabGlowTimer);
  void paperHighlightsTabBtn.offsetWidth;
  paperHighlightsTabBtn.classList.add("saved-glow");
  state.highlightsTabGlowTimer = window.setTimeout(() => {
    paperHighlightsTabBtn.classList.remove("saved-glow");
    state.highlightsTabGlowTimer = 0;
  }, HIGHLIGHTS_TAB_GLOW_MS);
}

function getConversationDisplayData(conversation) {
  const raw = conversation && typeof conversation === "object" ? conversation : {};
  const codex = raw.codex && typeof raw.codex === "object" ? raw.codex : {};
  return { ...raw, ...codex };
}

function getConversationPaperSnapshot(conversation) {
  const display = getConversationDisplayData(conversation);
  return normalizePaperPayload({
    ...(display.paper && typeof display.paper === "object" ? display.paper : {}),
    ...display
  });
}

function getPaperRestoreKey(paper) {
  const normalized = normalizePaperPayload(paper);
  if (!normalized) {
    return "";
  }
  return `${normalized.source}:${normalized.paper_id}:${normalized.paper_version || ""}`;
}

function comparePaperRestoreCandidates(left, right) {
  const leftTime = Date.parse(String(left?.updated_at || left?.updatedAt || "")) || 0;
  const rightTime = Date.parse(String(right?.updated_at || right?.updatedAt || "")) || 0;
  if (leftTime !== rightTime) {
    return rightTime - leftTime;
  }
  return String(right?.id || "").localeCompare(String(left?.id || ""));
}

function getConversationHistoryTitle(conversation) {
  const display = getConversationDisplayData(conversation);
  const label = compactInlineText(display.paper_history_label || display.paperHistoryLabel || "", 120);
  if (label) {
    return label;
  }
  return compactInlineText(display.title || "", 120) || "Untitled";
}

function getConversationHistoryKindLabel(conversation) {
  const display = getConversationDisplayData(conversation);
  const kind = String(display.paper_chat_kind || display.paperChatKind || "").trim().toLowerCase();
  if (!kind) {
    return "";
  }
  if (kind === "explain_selection") {
    return "Explain Selection";
  }
  if (kind === "general") {
    return "General";
  }
  return kind.replace(/_/g, " ").replace(/\b\w/g, (character) => character.toUpperCase());
}

function getConversationHistoryVersionLabel(conversation) {
  const paper = getConversationPaperSnapshot(conversation);
  const display = getConversationDisplayData(conversation);
  return normalizePaperVersionLabel(
    paper?.paper_version || display.paper_version || display.paperVersion || ""
  );
}

function getConversationHistoryDetail(conversation) {
  const paper = getConversationPaperSnapshot(conversation);
  if (!paper) {
    return "";
  }
  const display = getConversationDisplayData(conversation);
  const focus = compactInlineText(display.paper_focus_text || display.paperFocusText || "", 180);
  const preview = compactInlineText(display.preview || "", 180);
  const parts = [focus, preview].filter(Boolean);
  if (parts.length > 1 && parts[0] === parts[1]) {
    return parts[0];
  }
  return parts.join(" · ");
}

function setCurrentConversationPaper(paper) {
  state.currentConversationPaper = normalizePaperPayload(paper);
  if (state.currentConversationPaper && state.activeBrowserPaper && papersEqual(state.activeBrowserPaper, state.currentConversationPaper)) {
    state.activeBrowserPaper = {
      ...state.activeBrowserPaper,
      title: state.activeBrowserPaper.title || state.currentConversationPaper.title,
      canonical_url: state.activeBrowserPaper.canonical_url || state.currentConversationPaper.canonical_url,
      paper_version: state.activeBrowserPaper.paper_version || state.currentConversationPaper.paper_version,
      versioned_url: state.activeBrowserPaper.versioned_url || state.currentConversationPaper.versioned_url
    };
  }
}

function syncActiveBrowserPaperFromContext() {
  const previousKey = getPaperRestoreKey(state.activeBrowserPaper);
  const title = String(state.toolsActiveTab?.title || "").trim();
  const url = String(state.toolsActiveTab?.url || "").trim();
  const nextPaper = extractArxivPaperFromUrl(url, title);
  state.activeBrowserPaper = normalizePaperPayload(nextPaper);
  const nextKey = getPaperRestoreKey(state.activeBrowserPaper);
  if (previousKey !== nextKey) {
    state.pendingPaperConversationRestoreKey = nextKey;
  }
}

async function maybeRestoreConversationForActivePaper() {
  const activePaper = normalizePaperPayload(state.activeBrowserPaper);
  const activeKey = getPaperRestoreKey(activePaper);
  if (!activeKey || state.pendingPaperConversationRestoreKey !== activeKey) {
    return false;
  }
  if (state.restoringPaperConversation || state.activeRunId) {
    return false;
  }

  const relatedChats = Array.isArray(state.paperState?.conversations) ? state.paperState.conversations : [];
  state.pendingPaperConversationRestoreKey = "";
  if (!relatedChats.length) {
    return false;
  }
  if (getConversationBubbleCount() > 0) {
    return false;
  }
  const rankedChats = relatedChats
    .map((conversation) => ({
      conversation,
      paper: getConversationPaperSnapshot(conversation)
    }))
    .filter(({ paper }) => paper && papersEqual(paper, activePaper))
    .sort((left, right) => {
      const leftExact = Boolean(activePaper?.paper_version && left.paper?.paper_version && left.paper.paper_version === activePaper.paper_version);
      const rightExact = Boolean(activePaper?.paper_version && right.paper?.paper_version && right.paper.paper_version === activePaper.paper_version);
      if (leftExact !== rightExact) {
        return leftExact ? -1 : 1;
      }
      return comparePaperRestoreCandidates(left.conversation, right.conversation);
    });
  const targetConversationId = String(rankedChats[0]?.conversation?.id || "").trim();
  if (!targetConversationId || targetConversationId === state.sessionId) {
    return false;
  }

  state.restoringPaperConversation = true;
  try {
    await loadConversation(targetConversationId, { preservePaperTab: true });
    return true;
  } finally {
    state.restoringPaperConversation = false;
  }
}

async function refreshPaperState() {
  renderPaperWorkspace();
  const paper = getEffectivePaper();
  if (!paper) {
    state.paperState = null;
    resetPaperMemoryState();
    renderPaperWorkspace();
    renderHistory(state.sessionId);
    return;
  }
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.paper.get",
      source: paper.source,
      paperId: paper.paper_id
    });
    if (!result.ok) {
      throw new Error(result.error || "Failed to load paper workspace.");
    }
    state.paperState = {
      paper: normalizePaperPayload(result.paper),
      conversations: Array.isArray(result.conversations) ? result.conversations : [],
      memory: normalizePaperMemoryMetadata(result.memory)
    };
  } catch (error) {
    state.paperState = {
      paper,
      conversations: [],
      memory: normalizePaperMemoryMetadata(null),
      error: String(error.message || error)
    };
  }
  if (await maybeRestoreConversationForActivePaper()) {
    return;
  }
  renderPaperWorkspace();
  renderHistory(state.sessionId);
  if (state.activePaperTab === "memory") {
    void refreshPaperMemory(true);
  }
}

async function syncPaperContext() {
  syncActiveBrowserPaperFromContext();
  await refreshPaperState();
}


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
  const guideBtn = $("composer-read-guide-btn");
  const showBtn = $("composer-read-show-btn");
  explainBtn?.classList.toggle("active", Boolean(state.composerExplainSelection));
  explainBtn?.setAttribute("aria-pressed", String(Boolean(state.composerExplainSelection)));
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
}

function setComposerGuidePageContext(enabled) {
  state.composerGuidePageContext = Boolean(enabled);
  if (includePageContextEl) {
    includePageContextEl.checked = state.composerGuidePageContext;
  }
  syncReadAssistantQuickActionState();
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

toolsTabBtn?.addEventListener("click", () => {
  setMainTab("tools");
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

async function initializeApp() {
  setHistoryPanel(false);
  setMainTab("chat");
  setPaperTab("chat");
  renderPaperWorkspace();
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
  const next = tab === "tools" ? tab : "chat";
  state.activeMainTab = next;

  const tabConfig = [
    { key: "chat", button: chatTabBtn, view: chatViewEl },
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
  if (next === "tools") {
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

function renderBrowserConfig(config) {
  const normalized = normalizeBrowserConfig(config);
  state.toolsBrowserConfig = normalized;
  if (toolsAgentMaxStepsEl) {
    toolsAgentMaxStepsEl.min = "0";
    if (normalized.limits.agent_max_steps.max === null) {
      toolsAgentMaxStepsEl.removeAttribute("max");
    } else {
      toolsAgentMaxStepsEl.max = String(normalized.limits.agent_max_steps.max);
    }
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
  const papersStatusEl = $("read-assistant-status");
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
  const captureReadContext = options.captureReadContext === true;
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

    if (captureReadContext && readContextState.status === "fulfilled") {
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
    }
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
    await syncPaperContext();
  } catch (error) {
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
  const limits = state.toolsBrowserConfig?.limits?.agent_max_steps || { min: 1, max: null };
  const hasFiniteMax = Number.isInteger(limits.max) && limits.max > 0;
  const parsed = Number.parseInt(String(toolsAgentMaxStepsEl?.value || "").trim(), 10);
  if (!Number.isInteger(parsed)) {
    updateToolsStatus("Agent max steps must be an integer.");
    return;
  }
  if (parsed < 0) {
    updateToolsStatus("Agent max steps must be 0 (unlimited) or a positive integer.");
    return;
  }
  if (parsed > 0 && parsed < limits.min) {
    updateToolsStatus(`Agent max steps must be at least ${limits.min} or 0 for unlimited.`);
    return;
  }
  if (hasFiniteMax && parsed > limits.max) {
    updateToolsStatus(`Agent max steps must be at most ${limits.max}.`);
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

function createHistoryItem(conversation, selectedId) {
  const button = document.createElement("button");
  button.className = `history-item${conversation.id === selectedId ? " active" : ""}`;
  button.addEventListener("click", async () => {
    await loadConversation(conversation.id);
    if (!state.historyPinned) {
      closeHistoryPanel();
    }
  });

  const header = document.createElement("div");
  header.className = "history-item-header";

  const title = document.createElement("span");
  title.className = "history-title";
  title.textContent = getConversationHistoryTitle(conversation);
  header.appendChild(title);

  const metaRow = document.createElement("div");
  metaRow.className = "history-meta-row";

  const badges = document.createElement("div");
  badges.className = "history-badges";

  const version = getConversationHistoryVersionLabel(conversation);
  if (version) {
    const versionBadge = document.createElement("span");
    versionBadge.className = "status-badge history-badge";
    versionBadge.textContent = version;
    badges.appendChild(versionBadge);
  }

  const kind = getConversationHistoryKindLabel(conversation);
  if (kind) {
    const kindBadge = document.createElement("span");
    kindBadge.className = "status-badge history-badge";
    kindBadge.textContent = kind;
    badges.appendChild(kindBadge);
  }

  metaRow.appendChild(badges);

  const meta = document.createElement("span");
  meta.className = "history-meta";
  const count = Number(conversation.message_count || 0);
  meta.textContent = `${count} msg • ${formatTime(conversation.updated_at)}`;
  metaRow.appendChild(meta);

  const detailText = getConversationHistoryDetail(conversation);
  const detail = document.createElement("span");
  detail.className = "history-detail";
  detail.textContent = detailText;

  button.appendChild(header);
  button.appendChild(metaRow);
  if (detailText) {
    button.appendChild(detail);
  }
  return button;
}

function appendHistorySection(label, conversations, selectedId) {
  if (!historyListEl || !Array.isArray(conversations) || conversations.length === 0) {
    return;
  }
  const section = document.createElement("section");
  section.className = "history-section";

  const title = document.createElement("p");
  title.className = "history-section-title";
  title.textContent = label;
  section.appendChild(title);

  for (const conversation of conversations) {
    section.appendChild(createHistoryItem(conversation, selectedId));
  }

  historyListEl.appendChild(section);
}

function renderHistory(selectedId) {
  historyListEl.textContent = "";
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
    appendHistorySection("This Version", sameVersionChats, selectedId);
    appendHistorySection("Same Paper", samePaperOtherVersionChats, selectedId);
  } else {
    appendHistorySection("This Paper", relatedPaperChats, selectedId);
  }
  appendHistorySection(relatedPaperChats.length ? "All Chats" : "Chats", remainingChats.length ? remainingChats : state.conversationList, selectedId);
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
    const requestPromptSuffix = buildComposerPromptSuffix();
    const explainSelection = String(state.composerExplainSelection || "").trim();
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

    request = {
      type: "assistant.run.start",
      backend,
      sessionId: state.sessionId,
      prompt,
      requestPromptSuffix,
      paperContext: getEffectivePaper(),
      includePageContext: isComposerGuidePageContextEnabled(),
      forceBrowserAction,
      confirmed: false
    };
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
    const runBackend = typeof result.backend === "string" ? result.backend : message.backend || "codex";
    const runUi = ensureRunUi(runId, state.sessionId, true, runBackend);
    showRunWaitingIndicator(runUi);
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
  } finally {
    state.highlightAutosaveRunIds.delete(runId);
    state.pollingRuns.delete(runId);
    updateComposerState();
  }
}

function renderRunEvents(runId, events) {
  const runUi =
    state.runUi.get(runId) || ensureRunUi(runId, state.sessionId, true);
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
      appendRunStatusCard(describeRunEvent(event));
      continue;
    }

    if (event.type === "approval_decision" || event.type === "approval_granted") {
      updateApprovalCard(runUi, event?.data || {}, event.message || "");
      appendRunStatusCard(describeRunEvent(event));
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
        upsertRunAssistantBubble(
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
      appendRunStatusCard(describeRunEvent(event));
      continue;
    }

    if (event.type === "thinking") {
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

    if (event.type === "tool_result" || event.type === "calling_tool" || event.type === "thinking" || event.type === "cancel_requested") {
      appendRunStatusCard(describeRunEvent(event));
      continue;
    }

    appendRunStatusCard(describeRunEvent(event));
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
  } catch (error) {
    setApprovalCardBusy(card, false, `Approval failed: ${String(error.message || error)}`);
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

function clampFontScale(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return DEFAULT_FONT_SCALE;
  }
  const rounded = Math.round(numeric * 100) / 100;
  return Math.min(MAX_FONT_SCALE, Math.max(MIN_FONT_SCALE, rounded));
}

function applyFontScale(scale) {
  const next = clampFontScale(scale);
  state.fontScale = next;
  document.documentElement.style.setProperty("--font-scale", String(next));
  if (fontScaleDownBtn) {
    fontScaleDownBtn.disabled = next <= MIN_FONT_SCALE;
  }
  if (fontScaleUpBtn) {
    fontScaleUpBtn.disabled = next >= MAX_FONT_SCALE;
  }
}

function persistFontScalePreference(scale) {
  if (!chrome?.storage?.local) {
    return;
  }
  chrome.storage.local.set({ [FONT_SCALE_STORAGE_KEY]: clampFontScale(scale) });
}

function restoreFontScalePreference() {
  applyFontScale(DEFAULT_FONT_SCALE);
  if (!chrome?.storage?.local) {
    return;
  }
  chrome.storage.local.get([FONT_SCALE_STORAGE_KEY], (stored) => {
    if (chrome.runtime?.lastError) {
      return;
    }
    applyFontScale(stored?.[FONT_SCALE_STORAGE_KEY]);
  });
}

function adjustFontScale(direction) {
  const delta = direction > 0 ? FONT_SCALE_STEP : -FONT_SCALE_STEP;
  const next = clampFontScale(state.fontScale + delta);
  if (next === state.fontScale) {
    return;
  }
  applyFontScale(next);
  persistFontScalePreference(next);
}

fontScaleDownBtn?.addEventListener("click", () => {
  adjustFontScale(-1);
});

fontScaleUpBtn?.addEventListener("click", () => {
  adjustFontScale(1);
});

restoreFontScalePreference();
