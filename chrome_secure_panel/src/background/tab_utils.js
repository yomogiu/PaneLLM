(() => {
  const BG = self.PLLM_BG = self.PLLM_BG || {};

  function normalizePositiveInt(value, defaultValue, { min = 0, max = Number.MAX_SAFE_INTEGER } = {}) {
    const parsed = Number.parseInt(String(value ?? defaultValue), 10);
    if (!Number.isInteger(parsed)) {
      return defaultValue;
    }
    return Math.min(max, Math.max(min, parsed));
  }

  function ensureUrl(rawUrl, allowedHosts = null) {
    if (typeof rawUrl !== "string" || rawUrl.trim().length === 0) {
      throw new Error("Missing url");
    }
    const parsed = new URL(rawUrl);
    if (!["http:", "https:"].includes(parsed.protocol)) {
      throw new Error("URL protocol must be http or https.");
    }
    const url = parsed.toString();
    if (!BG.isHostAllowed(url, allowedHosts)) {
      throw BG.createHostNotAllowlistedError(url, "Target URL host is not allowlisted.", allowedHosts);
    }
    return url;
  }

  function ensureSelector(selector) {
    if (typeof selector !== "string" || selector.trim().length === 0) {
      throw new Error("Missing selector");
    }
    return selector.trim();
  }

  function parseTabId(tabId) {
    const parsed = Number.parseInt(String(tabId), 10);
    if (!Number.isInteger(parsed) || parsed < 0) {
      throw new Error(`Invalid tabId: ${tabId}`);
    }
    return parsed;
  }

  async function getActiveTab() {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    return tab || null;
  }

  async function resolveTabId(tabId) {
    if (tabId !== undefined && tabId !== null) {
      return parseTabId(tabId);
    }
    const activeTab = await getActiveTab();
    if (!activeTab || typeof activeTab.id !== "number") {
      throw new Error("No active tab available.");
    }
    return activeTab.id;
  }

  async function getAllowedTab(tabId, allowedHosts) {
    const tab = await chrome.tabs.get(tabId);
    if (!tab?.url || !BG.isHostAllowed(tab.url, allowedHosts)) {
      throw BG.createHostNotAllowlistedError(tab?.url || "", "Tab URL is not allowlisted.", allowedHosts);
    }
    return tab;
  }

  async function waitForTabLoad(tabId, timeoutMs) {
    const tab = await chrome.tabs.get(tabId);
    if (tab.status === "complete") {
      return;
    }

    await new Promise((resolve, reject) => {
      let done = false;
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error("Timed out waiting for tab load."));
      }, timeoutMs);

      const listener = (updatedTabId, changeInfo) => {
        if (updatedTabId !== tabId) {
          return;
        }
        if (changeInfo.status === "complete") {
          cleanup();
          resolve();
        }
      };

      function cleanup() {
        if (done) {
          return;
        }
        done = true;
        clearTimeout(timer);
        chrome.tabs.onUpdated.removeListener(listener);
      }

      chrome.tabs.onUpdated.addListener(listener);
    });
  }

  async function runInTab(tabId, func, args = []) {
    const tab = await chrome.tabs.get(tabId);
    await BG.ensureUrlHostPermission(tab?.url);
    const results = await chrome.scripting.executeScript({
      target: { tabId },
      func,
      args
    });
    return results?.[0]?.result ?? null;
  }

  async function delay(ms) {
    await new Promise((resolve) => setTimeout(resolve, ms));
  }

  function normalizeWaitCondition(value) {
    const normalized = typeof value === "string" ? value.trim().toLowerCase() : "";
    if (["present", "visible", "hidden", "gone"].includes(normalized)) {
      return normalized;
    }
    return "visible";
  }

  function normalizeSelectOptionRequest(args) {
    const normalized = {};
    let selectedModeCount = 0;

    if (typeof args.value === "string") {
      normalized.value = args.value;
      selectedModeCount += 1;
    }
    if (typeof args.text === "string") {
      normalized.text = args.text;
      selectedModeCount += 1;
    }
    if (args.optionIndex !== undefined && args.optionIndex !== null && String(args.optionIndex).trim() !== "") {
      normalized.optionIndex = normalizePositiveInt(args.optionIndex, 0, { min: 0, max: 10_000 });
      selectedModeCount += 1;
    }

    if (selectedModeCount !== 1) {
      throw new Error("select_option requires exactly one of value, text, or optionIndex.");
    }

    return normalized;
  }

  function normalizeLocator(rawLocator, options = {}) {
    const defaultVisible =
      Object.prototype.hasOwnProperty.call(options, "defaultVisible") ? options.defaultVisible : true;
    const allowVisibility =
      Object.prototype.hasOwnProperty.call(options, "allowVisibility") ? options.allowVisibility : true;
    if (!rawLocator || typeof rawLocator !== "object" || Array.isArray(rawLocator)) {
      throw new Error("Missing locator.");
    }

    const normalized = {};
    let hasLocatorField = false;
    for (const key of ["selector", "text", "label", "role", "placeholder", "name"]) {
      if (typeof rawLocator[key] === "string" && rawLocator[key].trim().length > 0) {
        normalized[key] = rawLocator[key].trim();
        hasLocatorField = true;
      }
    }

    if (!hasLocatorField) {
      throw new Error("Locator requires selector, text, label, role, placeholder, or name.");
    }

    normalized.exact = rawLocator.exact === true;
    normalized.index = normalizePositiveInt(rawLocator.index, 0, { min: 0, max: 100 });

    if (allowVisibility) {
      if (rawLocator.visible === true || rawLocator.visible === false) {
        normalized.visible = rawLocator.visible;
      } else {
        normalized.visible = defaultVisible;
      }
    } else {
      normalized.visible = null;
    }

    return normalized;
  }

  function normalizeRewriteMessageIndex(value) {
    const index = Number(value);
    if (!Number.isInteger(index) || index < 0) {
      throw new Error("messageIndex must be a non-negative integer.");
    }
    return index;
  }

  BG.normalizePositiveInt = normalizePositiveInt;
  BG.ensureUrl = ensureUrl;
  BG.ensureSelector = ensureSelector;
  BG.parseTabId = parseTabId;
  BG.getActiveTab = getActiveTab;
  BG.resolveTabId = resolveTabId;
  BG.getAllowedTab = getAllowedTab;
  BG.waitForTabLoad = waitForTabLoad;
  BG.runInTab = runInTab;
  BG.delay = delay;
  BG.normalizeWaitCondition = normalizeWaitCondition;
  BG.normalizeSelectOptionRequest = normalizeSelectOptionRequest;
  BG.normalizeLocator = normalizeLocator;
  BG.normalizeRewriteMessageIndex = normalizeRewriteMessageIndex;
})();
