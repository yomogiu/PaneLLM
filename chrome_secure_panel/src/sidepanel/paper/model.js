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
