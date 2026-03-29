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

function normalizeBrowserElementPayload(value) {
  if (!value || typeof value !== "object") {
    return null;
  }
  const raw = value;
  const tabId = Number(raw.tabId ?? raw.tab_id);
  const selectorHint = String(raw.selector || "").trim();
  const xpathHint = compactInlineText(raw.xpath || "", 240);
  const selector = selectorHint || xpathHint;
  const rawUrl = String(raw.url || "").trim();
  const url = normalizeLink(rawUrl) || rawUrl;
  if (!selector || !url) {
    return null;
  }
  return {
    tabId: Number.isInteger(tabId) ? tabId : null,
    url,
    title: compactInlineText(raw.title || "", 180),
    selector,
    xpath: compactInlineText(raw.xpath || "", 240),
    tagName: compactInlineText(raw.tagName || raw.tag_name || "", 48),
    role: compactInlineText(raw.role || "", 80),
    label: compactInlineText(raw.label || "", 160),
    name: compactInlineText(raw.name || "", 120),
    placeholder: compactInlineText(raw.placeholder || "", 120),
    enabled: raw.enabled !== false,
    editable: raw.editable === true,
    bounds: {
      x: Number(raw.x ?? raw.bounds?.x ?? 0) || 0,
      y: Number(raw.y ?? raw.bounds?.y ?? 0) || 0,
      width: Number(raw.width ?? raw.bounds?.width ?? 0) || 0,
      height: Number(raw.height ?? raw.bounds?.height ?? 0) || 0
    },
    pickedAt: String(raw.pickedAt || raw.picked_at || "").trim()
  };
}

function describeBrowserElementTarget(entry) {
  const normalized = normalizeBrowserElementPayload(entry);
  if (!normalized) {
    return "Selected element";
  }
  return (
    normalized.label
    || normalized.selector
    || normalized.role
    || normalized.tagName
    || "Selected element"
  );
}

function describeBrowserElementPage(entry, limit = 160) {
  const normalized = normalizeBrowserElementPayload(entry);
  if (!normalized) {
    return "Unknown page";
  }
  return compactInlineText(normalized.url, limit) || "Unknown page";
}

function isAllowlistedActiveTab(tab) {
  return tab?.allowed === true || tab?.allowlisted === true;
}

function browserElementMatchesCurrentPage(entry) {
  const normalized = normalizeBrowserElementPayload(entry);
  return Boolean(normalized && sameDocumentUrl(normalized.url, state.toolsActiveTab?.url || ""));
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


