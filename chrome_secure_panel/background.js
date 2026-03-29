const BG = self.PLLM_BG = self.PLLM_BG || {};

importScripts(
  "src/background/broker_client.js",
  "src/background/host_policy.js",
  "src/background/tab_utils.js",
  "src/background/page_context.js",
  "src/background/element_picker.js",
  "src/background/browser_commands.js",
  "src/background/run_payload.js",
  "src/background/relay.js",
  "src/background/message_router.js"
);

chrome.runtime.onInstalled.addListener(async () => {
  await chrome.sidePanel.setPanelBehavior({ openPanelOnActionClick: true });
  BG.startBrokerCommandLoop().catch((error) => {
    console.error("[secure-panel] command loop start failed on install:", error);
  });
});

chrome.runtime.onStartup.addListener(() => {
  BG.startBrokerCommandLoop().catch((error) => {
    console.error("[secure-panel] command loop start failed on startup:", error);
  });
});

BG.startBrokerCommandLoop().catch((error) => {
  console.error("[secure-panel] command loop start failed:", error);
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  BG.handleMessage(message, sender)
    .then((result) => sendResponse({ ok: true, ...result }))
    .catch((error) => {
      const payload = {
        ok: false,
        error: String(error?.message || error)
      };
      if (error?.code) {
        payload.error_code = String(error.code);
      }
      if (error?.data && typeof error.data === "object") {
        payload.error_data = error.data;
      }
      sendResponse(payload);
    });
  return true;
});

chrome.tabs.onUpdated.addListener((tabId, changeInfo) => {
  if (!BG.activePicker || BG.activePicker.tabId !== tabId) {
    return;
  }
  if (typeof changeInfo.url === "string" || changeInfo.status === "loading") {
    void BG.cancelActivePicker({ reason: "tab_updated", notify: true });
  }
});

chrome.tabs.onActivated.addListener((activeInfo) => {
  if (!BG.activePicker || activeInfo?.tabId === BG.activePicker.tabId) {
    return;
  }
  void BG.cancelActivePicker({ reason: "tab_switched", notify: true });
});

chrome.tabs.onRemoved.addListener((tabId) => {
  if (!BG.activePicker || BG.activePicker.tabId !== tabId) {
    return;
  }
  const current = BG.activePicker;
  BG.clearActivePickerState();
  BG.emitElementPickerEvent(BG.ELEMENT_PICKER_CANCELLED_EVENT, {
    tabId,
    url: current?.url || "",
    reason: "tab_closed"
  });
});
