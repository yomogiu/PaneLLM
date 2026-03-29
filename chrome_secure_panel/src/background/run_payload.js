(() => {
  const BG = self.PLLM_BG = self.PLLM_BG || {};

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

  function validateRunStartMessage(message) {
    validatePromptMessage(message);
    const backend = String(message.backend || "codex").trim();
    if (backend !== "llama" && backend !== "codex" && backend !== "mlx") {
      throw new Error("backend must be 'llama', 'codex', or 'mlx'.");
    }
    if (message.rewriteMessageIndex !== undefined) {
      BG.normalizeRewriteMessageIndex(message.rewriteMessageIndex);
    }
  }

  function detectRiskSignals(prompt) {
    if (BG.HIGH_RISK_PATTERN.test(prompt)) {
      return ["high_risk_prompt"];
    }
    return [];
  }

  async function buildAssistantBrokerPayload(message) {
    await BG.hostPolicyReady;
    validatePromptMessage(message);
    const includePageContext = Boolean(message.includePageContext);
    const forceBrowserAction = message.forceBrowserAction === true;
    let tab = null;

    if (includePageContext) {
      tab = await BG.getActiveTab();
      if (tab?.id && tab.url) {
        if (!BG.isHostAllowed(tab.url)) {
          throw BG.createHostNotAllowlistedError(
            tab.url,
            "Active tab is not in the extension allowlist."
          );
        }
      } else {
        tab = null;
      }
    }

    const pageContext = message.includePageContext && tab ? await BG.capturePageContext(tab) : null;
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
    const browserElementContext =
      message.browserElementContext && typeof message.browserElementContext === "object"
        ? message.browserElementContext
        : message.browser_element_context && typeof message.browser_element_context === "object"
          ? message.browser_element_context
          : null;
    const browserRuntimeRequested = forceBrowserAction;
    let browserRuntimeContext = null;
    if (browserRuntimeRequested) {
      const runtimeTab = await BG.getActiveTab();
      if (runtimeTab?.id && runtimeTab.url) {
        const host = BG.extractUrlHost(runtimeTab.url);
        browserRuntimeContext = {
          tabId: runtimeTab.id,
          url: String(runtimeTab.url),
          title: String(runtimeTab.title || ""),
          host: host || "",
          allowlisted: host ? BG.isHostAllowed(runtimeTab.url) : false,
          active: true
        };
      }
    }

    const payload = {
      session_id: message.sessionId,
      prompt: message.prompt,
      request_prompt_suffix: String(message.requestPromptSuffix || "").trim(),
      include_page_context: includePageContext,
      includePageContext: includePageContext,
      page_context: pageContext,
      allowed_hosts: BG.normalizeAllowedHosts(),
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
    if (browserElementContext) {
      payload.browser_element_context = browserElementContext;
    }
    if (browserRuntimeContext) {
      payload.browser_runtime_context = browserRuntimeContext;
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
      brokerPayload.rewrite_message_index = BG.normalizeRewriteMessageIndex(message.rewriteMessageIndex);
    }
    return await BG.brokerRequest("POST", "/runs", brokerPayload);
  }

  async function pollAssistantRunEvents(message) {
    if (!message?.runId || typeof message.runId !== "string") {
      throw new Error("runId is required.");
    }
    const after = Number.isInteger(message.after) ? message.after : 0;
    const timeoutMs = Number.isInteger(message.timeoutMs) ? message.timeoutMs : 20_000;
    const path = `/runs/${encodeURIComponent(message.runId)}/events?after=${encodeURIComponent(after)}&timeout_ms=${encodeURIComponent(timeoutMs)}`;
    return await BG.brokerRequest("GET", path);
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
    return await BG.brokerRequest("POST", path, {
      approval_id: message.approvalId,
      decision: message.decision
    });
  }

  async function cancelAssistantRun(message) {
    if (!message?.runId || typeof message.runId !== "string") {
      throw new Error("runId is required.");
    }
    const path = `/runs/${encodeURIComponent(message.runId)}/cancel`;
    return await BG.brokerRequest("POST", path, {});
  }

  async function listConversations() {
    return await BG.brokerRequest("GET", "/conversations");
  }

  async function getConversation(message) {
    if (!message?.sessionId || typeof message.sessionId !== "string") {
      throw new Error("sessionId is required.");
    }
    const path = `/conversations/${encodeURIComponent(message.sessionId)}`;
    return await BG.brokerRequest("GET", path);
  }

  async function deleteConversation(message) {
    if (!message?.sessionId || typeof message.sessionId !== "string") {
      throw new Error("sessionId is required.");
    }
    const path = `/conversations/${encodeURIComponent(message.sessionId)}`;
    return await BG.brokerRequest("DELETE", path);
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
    return await BG.brokerRequest("GET", `/papers/lookup?${params.toString()}`);
  }

  async function requestPaperSummary(message) {
    const paper = message?.paper && typeof message.paper === "object" ? message.paper : null;
    if (!paper) {
      throw new Error("paper is required.");
    }
    return await BG.brokerRequest("POST", "/papers/summary_request", {
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
    return await BG.brokerRequest("POST", "/papers/memory_query", {
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
    return await BG.brokerRequest("POST", "/papers/highlights_capture", {
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
    const capture = await BG.captureReadAssistantContext(message);
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
    return await BG.brokerRequest("POST", "/papers/summary_generate", {
      paper,
      session_id: sessionId,
      backend: typeof message?.backend === "string" ? message.backend : "codex",
      page_context: capture.context
    });
  }

  async function getModels() {
    return await BG.brokerRequest("GET", "/models");
  }

  async function getBrowserConfig() {
    return await BG.brokerRequest("GET", "/browser/config");
  }

  async function updateBrowserConfig(message) {
    const body = {};
    if (Object.prototype.hasOwnProperty.call(message || {}, "agentMaxSteps")) {
      body.agent_max_steps = message.agentMaxSteps;
    } else if (Object.prototype.hasOwnProperty.call(message || {}, "agent_max_steps")) {
      body.agent_max_steps = message.agent_max_steps;
    }
    return await BG.brokerRequest("POST", "/browser/config", body);
  }

  async function getBrowserProfiles() {
    return await BG.brokerRequest("GET", "/browser/profiles");
  }

  async function updateBrowserProfiles(message) {
    const payload = message?.browserProfiles && typeof message.browserProfiles === "object"
      ? message.browserProfiles
      : message;
    const body = {};
    if (Array.isArray(payload?.profiles)) {
      body.profiles = payload.profiles;
    }
    if (Object.prototype.hasOwnProperty.call(payload || {}, "selectedProfileId")) {
      body.selected_profile_id = payload.selectedProfileId;
    } else if (Object.prototype.hasOwnProperty.call(payload || {}, "selected_profile_id")) {
      body.selected_profile_id = payload.selected_profile_id;
    }
    if (Object.prototype.hasOwnProperty.call(payload || {}, "attachedProfile")) {
      body.attached_profile = payload.attachedProfile;
    } else if (Object.prototype.hasOwnProperty.call(payload || {}, "attached_profile")) {
      body.attached_profile = payload.attached_profile;
    }
    return await BG.brokerRequest("POST", "/browser/profiles", body);
  }

  BG.validatePromptMessage = validatePromptMessage;
  BG.validateRunStartMessage = validateRunStartMessage;
  BG.detectRiskSignals = detectRiskSignals;
  BG.buildAssistantBrokerPayload = buildAssistantBrokerPayload;
  BG.startAssistantRun = startAssistantRun;
  BG.pollAssistantRunEvents = pollAssistantRunEvents;
  BG.submitAssistantRunApproval = submitAssistantRunApproval;
  BG.cancelAssistantRun = cancelAssistantRun;
  BG.listConversations = listConversations;
  BG.getConversation = getConversation;
  BG.deleteConversation = deleteConversation;
  BG.getPaperWorkspace = getPaperWorkspace;
  BG.requestPaperSummary = requestPaperSummary;
  BG.queryPaperMemory = queryPaperMemory;
  BG.capturePaperHighlights = capturePaperHighlights;
  BG.generatePaperSummary = generatePaperSummary;
  BG.getModels = getModels;
  BG.getBrowserConfig = getBrowserConfig;
  BG.updateBrowserConfig = updateBrowserConfig;
  BG.getBrowserProfiles = getBrowserProfiles;
  BG.updateBrowserProfiles = updateBrowserProfiles;
})();
