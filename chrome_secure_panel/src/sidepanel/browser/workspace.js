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

function setBrowserActionArmed(armed, reason = "") {
  state.browserActionArmed = armed === true;
  state.browserActionArmedReason = state.browserActionArmed ? String(reason || "").trim() : "";
  syncReadAssistantQuickActionState();
  renderBrowserAutomationPanel();
  renderBrowserNextActionIndicator();
}

function clearBrowserElementAttachment() {
  state.browserElementContextForNextMessage = null;
  renderBrowserPickerPanel();
  renderBrowserNextActionIndicator();
}

function attachBrowserElementForNextMessage(entry = state.browserPickerLatest) {
  const normalized = normalizeBrowserElementPayload(entry);
  if (!normalized || !browserElementMatchesCurrentPage(normalized)) {
    state.browserPickerError = "Pick an element from the current page before attaching it.";
    renderBrowserPickerPanel();
    return false;
  }
  state.browserElementContextForNextMessage = normalized;
  state.browserPickerError = "";
  renderBrowserPickerPanel();
  renderBrowserNextActionIndicator();
  return true;
}

function rememberBrowserPickerEntry(entry) {
  const normalized = normalizeBrowserElementPayload(entry);
  if (!normalized) {
    return null;
  }
  state.browserPickerLatest = normalized;
  if (browserElementMatchesCurrentPage(normalized)) {
    state.browserElementContextForNextMessage = normalized;
    state.browserPickerError = "";
  }
  const key = `${normalized.url}::${normalized.selector}`;
  const deduped = state.browserPickerHistory.filter((item) => `${item.url}::${item.selector}` !== key);
  state.browserPickerHistory = [normalized, ...deduped].slice(0, 5);
  void maybeRecordActiveBrowserProfileStep();
  return normalized;
}

function resetBrowserAutomationState() {
  state.browserAutomationStatus = {
    label: "Idle",
    text: "Browser actions are idle.",
    detail: ""
  };
  state.browserAutomationLastToolSummary = "";
  state.browserAutomationLastToolResult = "";
  state.browserAutomationLastToolError = "";
  state.browserPendingApprovals = new Map();
  renderBrowserAutomationPanel();
}

function setBrowserAutomationStatus(label, text, detail = "") {
  state.browserAutomationStatus = {
    label: String(label || "Idle"),
    text: String(text || "Browser actions are idle."),
    detail: String(detail || "")
  };
}

function syncBrowserWorkspaceToActiveTab() {
  if (!browserElementMatchesCurrentPage(state.browserElementContextForNextMessage)) {
    state.browserElementContextForNextMessage = null;
  }
  if (state.browserPickerLatest && !browserElementMatchesCurrentPage(state.browserPickerLatest)) {
    state.browserPickerError = "";
  }
  void maybeRecordActiveBrowserProfileStep();
  renderBrowserCurrentPage();
  renderBrowserPickerPanel();
  renderBrowserProfilePanel();
  renderBrowserNextActionIndicator();
  renderBrowserAutomationPanel();
}

function renderBrowserCurrentPage() {
  if (!browserCurrentPageTitleEl || !browserCurrentPageUrlEl) {
    return;
  }
  const activeTab = state.toolsActiveTab && typeof state.toolsActiveTab === "object" ? state.toolsActiveTab : null;
  if (!activeTab?.url) {
    browserCurrentPageTitleEl.textContent = "No active tab available.";
    browserCurrentPageUrlEl.textContent = "Bring a browser tab into focus to inspect the current page.";
    return;
  }
  const host = String(activeTab.host || "").trim();
  const title = String(activeTab.title || "").trim();
  const marker = isAllowlistedActiveTab(activeTab) ? "allowlisted" : "not allowlisted";
  browserCurrentPageTitleEl.textContent = title || host || "Current page";
  browserCurrentPageUrlEl.textContent = `${String(activeTab.url)}${host ? ` · ${host} (${marker})` : ""}`;
}

function renderBrowserNextActionIndicator() {
  if (!browserNextActionIndicatorEl) {
    return;
  }
  browserNextActionIndicatorEl.textContent = "";
  const fragments = [];
  const attachedProfile = getAttachedBrowserProfileState();
  if (state.browserActionArmed) {
    fragments.push({
      key: "armed",
      label: state.browserActionArmedReason
        ? `Browser tools enabled: ${state.browserActionArmedReason}`
        : "Browser tools enabled for next message"
    });
  }
  if (state.composerGuidePageContext) {
    fragments.push({
      key: "page-context",
      label: "Page context shared"
    });
  }
  const attached = normalizeBrowserElementPayload(state.browserElementContextForNextMessage);
  if (attached && browserElementMatchesCurrentPage(attached)) {
    fragments.push({
      key: "element",
      label: `Element attached: ${describeBrowserElementTarget(attached)} (${describeBrowserElementPage(attached, 110)})`
    });
  }
  if (attachedProfile.profile) {
    fragments.push({
      key: "profile",
      label: attachedProfile.step
        ? `Profile attached: ${attachedProfile.profile.name} · Step ${attachedProfile.stepIndex + 1}`
        : `Profile attached: ${attachedProfile.profile.name}`
    });
  }

  if (!fragments.length) {
    browserNextActionIndicatorEl.classList.add("hidden");
    return;
  }

  browserNextActionIndicatorEl.classList.remove("hidden");
  for (const fragment of fragments) {
    const chip = document.createElement("div");
    chip.className = "browser-next-action-chip";

    const label = document.createElement("span");
    label.textContent = fragment.label;
    chip.appendChild(label);

    const clearBtn = document.createElement("button");
    clearBtn.type = "button";
    clearBtn.className = "ghost small";
    clearBtn.textContent = "Clear";
    clearBtn.addEventListener("click", () => {
      if (fragment.key === "armed") {
        if (state.browserActionArmedReason === "Show Me Where") {
          clearComposerShowMeWhere();
          updatePapersStatus("Show Me Where disabled.");
        } else {
          setBrowserActionArmed(false);
          updatePapersStatus("Browser tools disabled for your next message.");
        }
        return;
      }
      if (fragment.key === "page-context") {
        setComposerGuidePageContext(false);
        updatePapersStatus("Page context disabled.");
        return;
      }
      if (fragment.key === "profile") {
        clearAttachedBrowserProfile();
        return;
      }
      clearBrowserElementAttachment();
    });
    chip.appendChild(clearBtn);

    browserNextActionIndicatorEl.appendChild(chip);
  }
}

function renderBrowserPickerPanel() {
  if (browserPickerStatusEl) {
    if (state.browserPickerActive) {
      browserPickerStatusEl.textContent = "Picker active. Click an element on the page or press Escape to cancel.";
    } else if (state.browserPickerError) {
      browserPickerStatusEl.textContent = state.browserPickerError;
    } else if (state.browserPickerLatest) {
      browserPickerStatusEl.textContent = browserElementMatchesCurrentPage(state.browserPickerLatest)
        ? "Latest pick is ready to attach to your next message."
        : "Latest pick was made on a different page. Pick again on the current page to attach it.";
    } else {
      browserPickerStatusEl.textContent =
        "Pick an element on the current allowlisted page to attach browser context to your next message.";
    }
  }

  if (browserPickerStartBtn) {
    browserPickerStartBtn.disabled =
      state.toolsBusy || Boolean(state.activeRunId) || !state.toolsActiveTab?.url || !isAllowlistedActiveTab(state.toolsActiveTab);
  }
  if (browserPickerCancelBtn) {
    browserPickerCancelBtn.disabled = state.toolsBusy || !state.browserPickerActive;
  }

  if (browserPickedElementEl) {
    browserPickedElementEl.textContent = "";
    const latest = normalizeBrowserElementPayload(state.browserPickerLatest);
    if (latest) {
      const isAttached = state.browserElementContextForNextMessage?.selector === latest.selector
        && sameDocumentUrl(state.browserElementContextForNextMessage?.url || "", latest.url);
      const card = document.createElement("article");
      card.className = "browser-picker-card";

      const title = document.createElement("p");
      title.className = "browser-picker-card-title";
      title.textContent = describeBrowserElementTarget(latest);
      card.appendChild(title);

      const selector = document.createElement("p");
      selector.className = "browser-picker-card-detail";
      selector.textContent = `Selector: ${latest.selector}`;
      card.appendChild(selector);

      const meta = document.createElement("p");
      meta.className = "browser-picker-card-meta";
      const pageLabel = describeBrowserElementPage(latest);
      meta.textContent = `${isAttached ? "Attached to context • " : ""}${pageLabel} • ${
        browserElementMatchesCurrentPage(latest) ? "Current page" : "Different page"
      }`;
      card.appendChild(meta);

      const actions = document.createElement("div");
      actions.className = "button-row";

      const useBtn = document.createElement("button");
      useBtn.type = "button";
      useBtn.className = "small";
      useBtn.textContent = isAttached
        && sameDocumentUrl(state.browserElementContextForNextMessage?.url || "", latest.url)
        ? "Attached"
        : "Use In Next Message";
      useBtn.disabled =
        state.toolsBusy
        || Boolean(state.activeRunId)
        || !browserElementMatchesCurrentPage(latest)
        || (
          state.browserElementContextForNextMessage?.selector === latest.selector
          && sameDocumentUrl(state.browserElementContextForNextMessage?.url || "", latest.url)
        );
      useBtn.addEventListener("click", () => {
        attachBrowserElementForNextMessage(latest);
      });
      actions.appendChild(useBtn);

      const clearBtn = document.createElement("button");
      clearBtn.type = "button";
      clearBtn.className = "ghost small";
      clearBtn.textContent = "Clear Pick";
      clearBtn.addEventListener("click", () => {
        state.browserPickerLatest = null;
        if (
          state.browserElementContextForNextMessage
          && latest.selector === state.browserElementContextForNextMessage.selector
          && sameDocumentUrl(latest.url, state.browserElementContextForNextMessage.url)
        ) {
          clearBrowserElementAttachment();
        } else {
          renderBrowserPickerPanel();
        }
      });
      actions.appendChild(clearBtn);

      card.appendChild(actions);
      browserPickedElementEl.appendChild(card);
    } else {
      const empty = document.createElement("p");
      empty.className = "tools-empty";
      empty.textContent = "No picked element yet.";
      browserPickedElementEl.appendChild(empty);
    }
  }

  if (browserPickedHistoryEl) {
    browserPickedHistoryEl.textContent = "";
    if (!state.browserPickerHistory.length) {
      const empty = document.createElement("p");
      empty.className = "tools-empty";
      empty.textContent = "Recent picks will appear here for this session.";
      browserPickedHistoryEl.appendChild(empty);
    } else {
      for (const entry of state.browserPickerHistory) {
        const normalized = normalizeBrowserElementPayload(entry);
        if (!normalized) {
          continue;
        }
        const row = document.createElement("div");
        row.className = "tools-item";

        const meta = document.createElement("div");
        meta.className = "tools-item-meta";

        const isAttached = state.browserElementContextForNextMessage?.selector === normalized.selector
          && sameDocumentUrl(state.browserElementContextForNextMessage?.url || "", normalized.url);
        const hostEl = document.createElement("div");
        hostEl.className = "tools-item-host";
        hostEl.textContent = `${describeBrowserElementTarget(normalized)}${isAttached ? " (Attached)" : ""}`;
        meta.appendChild(hostEl);

        const detail = document.createElement("div");
        detail.className = "browser-picker-history-detail";
        detail.textContent = `Page: ${describeBrowserElementPage(normalized, 120)} • Selector: ${normalized.selector}`;
        meta.appendChild(detail);
        row.appendChild(meta);

        const actions = document.createElement("div");
        actions.className = "tools-item-actions";

        const useBtn = document.createElement("button");
        useBtn.type = "button";
        useBtn.className = "ghost small";
        useBtn.textContent = browserElementMatchesCurrentPage(entry) ? "Use" : "Different Page";
        useBtn.disabled = state.toolsBusy || Boolean(state.activeRunId) || !browserElementMatchesCurrentPage(entry);
        useBtn.addEventListener("click", () => {
          attachBrowserElementForNextMessage(entry);
        });
        actions.appendChild(useBtn);

        row.appendChild(actions);
        browserPickedHistoryEl.appendChild(row);
      }
    }
  }
}

function renderBrowserAutomationPanel() {
  if (browserAutomationStatusEl) {
    browserAutomationStatusEl.textContent = state.browserAutomationStatus.text || "Browser actions are idle.";
  }
  if (browserAutomationArmBtn) {
    browserAutomationArmBtn.textContent = state.browserActionArmed
      ? "Browser Tools Enabled"
      : "Use Browser Tools On Next Message";
    browserAutomationArmBtn.classList.toggle("active", state.browserActionArmed);
    browserAutomationArmBtn.disabled = state.toolsBusy || Boolean(state.activeRunId);
  }

  if (browserAutomationSummaryEl) {
    browserAutomationSummaryEl.textContent = "";
    const items = [];
    if (state.activeRunId) {
      items.push(`Active run: ${state.activeRunId}`);
    }
    if (state.browserAutomationLastToolSummary) {
      items.push(`Last action: ${state.browserAutomationLastToolSummary}`);
    }
    if (state.browserAutomationLastToolResult) {
      items.push(`Last result: ${state.browserAutomationLastToolResult}`);
    }
    if (state.browserAutomationLastToolError) {
      items.push(`Last error: ${state.browserAutomationLastToolError}`);
    }
    if (!items.length) {
      const empty = document.createElement("p");
      empty.className = "tools-empty";
      empty.textContent = "No browser automation activity yet.";
      browserAutomationSummaryEl.appendChild(empty);
    } else {
      for (const item of items) {
        const line = document.createElement("p");
        line.className = "browser-automation-line";
        line.textContent = item;
        browserAutomationSummaryEl.appendChild(line);
      }
    }
  }

  if (browserAutomationApprovalsEl) {
    browserAutomationApprovalsEl.textContent = "";
    const approvals = [...state.browserPendingApprovals.values()];
    if (!approvals.length) {
      const empty = document.createElement("p");
      empty.className = "tools-empty";
      empty.textContent = "No pending browser approvals.";
      browserAutomationApprovalsEl.appendChild(empty);
    } else {
      for (const approval of approvals) {
        const card = document.createElement("article");
        card.className = "browser-approval-card";

        const title = document.createElement("p");
        title.className = "browser-approval-title";
        title.textContent = approval.summary || "Browser action requires approval.";
        card.appendChild(title);

        if (approval.host) {
          const host = document.createElement("p");
          host.className = "browser-approval-detail";
          host.textContent = `Host: ${approval.host}`;
          card.appendChild(host);
        }
        if (approval.selector) {
          const selector = document.createElement("p");
          selector.className = "browser-approval-detail";
          selector.textContent = `Selector: ${approval.selector}`;
          card.appendChild(selector);
        }
        if (approval.text_preview) {
          const preview = document.createElement("p");
          preview.className = "browser-approval-detail";
          preview.textContent = `Text: ${approval.text_preview}`;
          card.appendChild(preview);
        }

        const status = document.createElement("p");
        status.className = "browser-approval-status";
        status.textContent = approval.statusText || "Waiting for your decision.";
        card.appendChild(status);

        const actions = document.createElement("div");
        actions.className = "button-row";

        const approveBtn = document.createElement("button");
        approveBtn.type = "button";
        approveBtn.textContent = "Approve";
        approveBtn.disabled = approval.resolved === true;
        approveBtn.addEventListener("click", async () => {
          await submitBrowserApprovalDecision(approval, "approve");
        });
        actions.appendChild(approveBtn);

        const denyBtn = document.createElement("button");
        denyBtn.type = "button";
        denyBtn.className = "ghost";
        denyBtn.textContent = "Deny";
        denyBtn.disabled = approval.resolved === true;
        denyBtn.addEventListener("click", async () => {
          await submitBrowserApprovalDecision(approval, "deny");
        });
        actions.appendChild(denyBtn);

        const cancelBtn = document.createElement("button");
        cancelBtn.type = "button";
        cancelBtn.className = "ghost";
        cancelBtn.textContent = "Cancel Run";
        cancelBtn.disabled = approval.resolved === true;
        cancelBtn.addEventListener("click", async () => {
          await cancelRun(approval.runId);
        });
        actions.appendChild(cancelBtn);

        card.appendChild(actions);
        browserAutomationApprovalsEl.appendChild(card);
      }
    }
  }
}

async function submitBrowserApprovalDecision(approval, decision) {
  const approvalId = String(approval?.approval_id || "").trim();
  const runId = String(approval?.runId || "").trim();
  if (!approvalId || !runId) {
    return;
  }
  const existing = state.browserPendingApprovals.get(approvalId);
  if (existing) {
    existing.statusText = decision === "approve" ? "Submitting approval..." : "Submitting denial...";
    existing.resolved = true;
    state.browserPendingApprovals.set(approvalId, existing);
    renderBrowserAutomationPanel();
  }
  try {
    const result = await sendRuntimeMessage({
      type: "assistant.run.approval",
      runId,
      approvalId,
      decision
    });
    if (!result.ok) {
      throw new Error(result.error || "Approval request failed.");
    }
    const approved = state.browserPendingApprovals.get(approvalId);
    if (approved) {
      approved.statusText = decision === "approve" ? "Approved." : "Denied.";
      approved.resolved = true;
      state.browserPendingApprovals.set(approvalId, approved);
      renderBrowserAutomationPanel();
    }
  } catch (error) {
    const pending = state.browserPendingApprovals.get(approvalId);
    if (pending) {
      pending.statusText = `Approval failed: ${String(error.message || error)}`;
      pending.resolved = false;
      state.browserPendingApprovals.set(approvalId, pending);
      renderBrowserAutomationPanel();
    }
  }
}

function rememberBrowserApproval(runId, approval) {
  const approvalId = String(approval?.approval_id || "").trim();
  if (!approvalId) {
    return;
  }
  state.browserPendingApprovals.set(approvalId, {
    ...approval,
    runId,
    statusText: "Waiting for your decision.",
    resolved: false
  });
  renderBrowserAutomationPanel();
}

function resolveBrowserApproval(approvalId, statusText) {
  const approval = state.browserPendingApprovals.get(String(approvalId || ""));
  if (!approval) {
    return;
  }
  approval.statusText = String(statusText || "Approval updated.");
  approval.resolved = true;
  state.browserPendingApprovals.set(approval.approval_id, approval);
  renderBrowserAutomationPanel();
}

function clearBrowserApprovalsForRun(runId) {
  if (!runId) {
    state.browserPendingApprovals = new Map();
    renderBrowserAutomationPanel();
    return;
  }
  let changed = false;
  for (const [approvalId, approval] of state.browserPendingApprovals.entries()) {
    if (approval.runId === runId) {
      state.browserPendingApprovals.delete(approvalId);
      changed = true;
    }
  }
  if (changed) {
    renderBrowserAutomationPanel();
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
    browserProfileSelectEl,
    browserProfileNameEl,
    browserProfileRecordBtn,
    browserProfileSaveLatestBtn,
    browserProfileUseBtn,
    browserProfileDeleteBtn,
    browserPickerStartBtn,
    browserPickerCancelBtn,
    browserAutomationArmBtn,
    $("composer-read-explain-btn"),
    $("composer-read-browser-btn"),
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
  for (const button of browserPickedElementEl?.querySelectorAll("button") || []) {
    button.disabled = busy || Boolean(state.activeRunId);
  }
  for (const button of browserPickedHistoryEl?.querySelectorAll("button") || []) {
    button.disabled = busy || Boolean(state.activeRunId) || button.disabled;
  }
  for (const button of browserAutomationApprovalsEl?.querySelectorAll("button") || []) {
    button.disabled = busy || button.disabled;
  }
  setReadAssistantExplainEnabled();
  renderBrowserProfilePanel();
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
      const marker = isAllowlistedActiveTab(state.toolsActiveTab) ? "allowed" : "not allowed";
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
    syncBrowserWorkspaceToActiveTab();
  } catch (error) {
    console.warn("[secure-panel] tools refresh failed:", String(error?.message || error));
    if (showErrors) {
      updateToolsStatus(`Browser error: ${String(error.message || error)}`);
      updatePapersStatus(`Read assistant error: ${String(error.message || error)}`);
      state.browserPickerError = `Browser error: ${String(error.message || error)}`;
    }
  } finally {
    setToolsBusy(false);
    renderBrowserPickerPanel();
    renderBrowserAutomationPanel();
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
    updateToolsStatus(successStatus || "Browser policy updated.");
    return true;
  } catch (error) {
    updateToolsStatus(`Browser error: ${String(error.message || error)}`);
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
    updateToolsStatus(`Browser error: ${String(error.message || error)}`);
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
    updateToolsStatus(`Browser error: ${String(error.message || error)}`);
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

