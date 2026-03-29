(() => {
  const BG = self.PLLM_BG = self.PLLM_BG || {};

  async function handleMessage(message, sender = null) {
    await BG.hostPolicyReady;
    if (!message || typeof message !== "object") {
      throw new Error("Invalid message payload.");
    }
    if (
      message.type === BG.ELEMENT_PICKER_RESULT_EVENT
      || message.type === BG.ELEMENT_PICKER_CANCELLED_EVENT
      || message.type === BG.ELEMENT_PICKER_ERROR_EVENT
    ) {
      return { acknowledged: true };
    }
    if (message.type === BG.ELEMENT_PICKER_PAGE_EVENT) {
      return await BG.handleElementPickerPageEvent(message, sender);
    }
    if (message.type === "assistant.health") {
      return { health: await BG.checkBrokerHealth() };
    }
    if (message.type === "assistant.run.start") {
      return await BG.startAssistantRun(message);
    }
    if (message.type === "assistant.run.events") {
      return await BG.pollAssistantRunEvents(message);
    }
    if (message.type === "assistant.run.approval") {
      return await BG.submitAssistantRunApproval(message);
    }
    if (message.type === "assistant.run.cancel") {
      return await BG.cancelAssistantRun(message);
    }
    if (message.type === "assistant.history.list") {
      return await BG.listConversations();
    }
    if (message.type === "assistant.history.get") {
      return await BG.getConversation(message);
    }
    if (message.type === "assistant.history.delete") {
      return await BG.deleteConversation(message);
    }
    if (message.type === "assistant.paper.get") {
      return await BG.getPaperWorkspace(message);
    }
    if (message.type === "assistant.paper.memory_query") {
      return await BG.queryPaperMemory(message);
    }
    if (message.type === "assistant.paper.summary_request") {
      return await BG.requestPaperSummary(message);
    }
    if (message.type === "assistant.paper.highlights_capture") {
      return await BG.capturePaperHighlights(message);
    }
    if (message.type === "assistant.paper.summary_generate") {
      return await BG.generatePaperSummary(message);
    }
    if (message.type === "assistant.models.get") {
      return await BG.getModels();
    }
    if (message.type === "assistant.read.context.capture") {
      return await BG.captureReadAssistantContext(message);
    }
    if (message.type === "assistant.browser.element_picker.start") {
      return await BG.startElementPicker(message);
    }
    if (message.type === "assistant.browser.element_picker.cancel") {
      return await BG.cancelElementPicker(message);
    }
    if (message.type === "assistant.tools.browser_config.get") {
      return await BG.getBrowserConfig();
    }
    if (message.type === "assistant.tools.browser_config.update") {
      return await BG.updateBrowserConfig(message);
    }
    if (message.type === "assistant.tools.browser_profiles.get") {
      return await BG.getBrowserProfiles();
    }
    if (message.type === "assistant.tools.browser_profiles.update") {
      return await BG.updateBrowserProfiles(message);
    }
    if (message.type === "assistant.tools.page_hosts.get") {
      return { policy: BG.getHostPolicySnapshot() };
    }
    if (message.type === "assistant.tools.page_hosts.allow") {
      return { policy: await BG.allowHost(message.host) };
    }
    if (message.type === "assistant.tools.page_hosts.remove_allow") {
      return { policy: await BG.removeAllowedHost(message.host) };
    }
    if (message.type === "assistant.tools.page_hosts.allow_active_tab") {
      const activeTab = await BG.getActiveTab();
      if (!activeTab?.url) {
        throw new Error("Unable to resolve the active tab URL.");
      }
      const host = BG.extractUrlHost(activeTab.url);
      if (!host) {
        throw new Error("Active tab host is invalid.");
      }
      return { policy: await BG.allowHost(host), host };
    }
    if (message.type === "assistant.tools.page_hosts.active_tab") {
      const activeTab = await BG.getActiveTab();
      if (!activeTab?.url) {
        return { active_tab: null };
      }
      const host = BG.extractUrlHost(activeTab.url);
      if (!host) {
        return { active_tab: null };
      }
      const snapshot = BG.getHostPolicySnapshot();
      return {
        active_tab: {
          tabId: Number.isInteger(activeTab.id) ? activeTab.id : null,
          host,
          url: String(activeTab.url),
          title: String(activeTab.title || ""),
          allowed: BG.isHostAllowed(activeTab.url, snapshot.effective_allowed_hosts),
          allowlisted: BG.isHostAllowed(activeTab.url, snapshot.effective_allowed_hosts),
          active: true
        }
      };
    }
    if (message.type === "assistant.browser.tool.call") {
      return await BG.brokerRequest("POST", "/browser/tools/call", {
        name: message.name,
        arguments: message.arguments || {}
      });
    }
    throw new Error(`Unsupported message type: ${String(message.type)}`);
  }

  BG.handleMessage = handleMessage;
})();
