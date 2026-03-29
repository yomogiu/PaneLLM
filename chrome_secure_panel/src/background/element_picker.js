(() => {
  const BG = self.PLLM_BG = self.PLLM_BG || {};

  BG.ELEMENT_PICKER_PAGE_EVENT = "assistant.browser.element_picker.page_event";
  BG.ELEMENT_PICKER_RESULT_EVENT = "assistant.browser.element_picker.result";
  BG.ELEMENT_PICKER_CANCELLED_EVENT = "assistant.browser.element_picker.cancelled";
  BG.ELEMENT_PICKER_ERROR_EVENT = "assistant.browser.element_picker.error";

  BG.activePicker = null;

  function getPickerActiveTabInfo(tab, allowedHosts) {
    if (!tab?.url || typeof tab.id !== "number") {
      throw new Error("Unable to resolve the active tab.");
    }
    const host = BG.extractUrlHost(tab.url);
    if (!host) {
      throw new Error("Active tab host is invalid.");
    }
    if (!BG.isHostAllowed(tab.url, allowedHosts)) {
      throw BG.createHostNotAllowlistedError(tab.url, "Active tab is not in the extension allowlist.", allowedHosts);
    }
    return {
      tabId: tab.id,
      url: String(tab.url),
      title: String(tab.title || ""),
      host
    };
  }

  function clearActivePickerState() {
    BG.activePicker = null;
  }

  function emitElementPickerEvent(type, payload = {}) {
    try {
      const result = chrome.runtime.sendMessage({
        type,
        ...payload
      });
      if (result && typeof result.catch === "function") {
        result.catch(() => {});
      }
    } catch {
      // Ignore missing listeners; the sidepanel may not be open.
    }
  }

  async function clearElementPickerInPage() {
    const CLEANUP_KEY = "__assistElementPickerCleanup";
    const cleanup = window[CLEANUP_KEY];
    if (typeof cleanup === "function") {
      cleanup();
      return { cleared: true };
    }
    return { cleared: false };
  }

  async function cleanupElementPickerInTab(tabId) {
    if (!Number.isInteger(tabId) || tabId < 0) {
      return;
    }
    try {
      await BG.runInTab(tabId, clearElementPickerInPage, []);
    } catch {
      // Best effort cleanup only. The tab may have navigated or been closed.
    }
  }

  async function cancelActivePicker(options = {}) {
    const current = BG.activePicker;
    if (!current) {
      return false;
    }
    clearActivePickerState();
    await cleanupElementPickerInTab(current.tabId);
    if (options.notify) {
      emitElementPickerEvent(BG.ELEMENT_PICKER_CANCELLED_EVENT, {
        tabId: current.tabId,
        url: current.url,
        reason: typeof options.reason === "string" && options.reason ? options.reason : "cancelled"
      });
    }
    return true;
  }

  async function startElementPicker(_message) {
    const allowedHosts = BG.normalizeAllowedHosts();
    const activeTab = await BG.getActiveTab();
    const activeInfo = getPickerActiveTabInfo(activeTab, allowedHosts);
    await BG.ensureUrlHostPermission(activeInfo.url);

    if (BG.activePicker) {
      await cancelActivePicker({ reason: "replaced", notify: false });
    }

    const requestId = `pick_${crypto.randomUUID()}`;
    try {
      await chrome.scripting.executeScript({
        target: { tabId: activeInfo.tabId },
        func: startElementPickerInPage,
        args: [requestId]
      });
    } catch (error) {
      clearActivePickerState();
      throw error;
    }

    BG.activePicker = {
      requestId,
      tabId: activeInfo.tabId,
      url: activeInfo.url,
      startedAt: Date.now()
    };
    return {
      started: true,
      requestId,
      tabId: activeInfo.tabId,
      url: activeInfo.url,
      title: activeInfo.title
    };
  }

  async function cancelElementPicker(message) {
    const cancelled = await cancelActivePicker({
      reason: typeof message?.reason === "string" && message.reason ? message.reason : "cancelled_by_user",
      notify: true
    });
    return { cancelled };
  }

  async function handleElementPickerPageEvent(message, sender) {
    const requestId = typeof message?.requestId === "string" ? message.requestId : "";
    const current = BG.activePicker;
    if (!current || !requestId || current.requestId !== requestId) {
      return { acknowledged: false };
    }
    const senderTabId = sender?.tab?.id;
    if (typeof senderTabId !== "number" || senderTabId !== current.tabId) {
      return { acknowledged: false };
    }

    const eventType = typeof message?.eventType === "string" ? message.eventType : "";
    const payload = message?.payload && typeof message.payload === "object" ? message.payload : {};
    clearActivePickerState();

    if (eventType === "result") {
      emitElementPickerEvent(BG.ELEMENT_PICKER_RESULT_EVENT, {
        payload: {
          ...payload,
          tabId: Number.isInteger(payload.tabId) ? payload.tabId : current.tabId,
          url: typeof payload.url === "string" && payload.url ? payload.url : current.url,
          pickedAt: typeof payload.pickedAt === "string" && payload.pickedAt ? payload.pickedAt : new Date().toISOString()
        }
      });
      return { acknowledged: true };
    }

    if (eventType === "cancelled") {
      emitElementPickerEvent(BG.ELEMENT_PICKER_CANCELLED_EVENT, {
        tabId: current.tabId,
        url: current.url,
        reason: typeof payload.reason === "string" && payload.reason ? payload.reason : "cancelled"
      });
      return { acknowledged: true };
    }

    emitElementPickerEvent(BG.ELEMENT_PICKER_ERROR_EVENT, {
      tabId: current.tabId,
      url: current.url,
      error: typeof payload.error === "string" && payload.error ? payload.error : "Element picker failed."
    });
    return { acknowledged: true };
  }

  function startElementPickerInPage(requestId) {
    const CLEANUP_KEY = "__assistElementPickerCleanup";
    const STATE_KEY = "__assistElementPickerState";
    const OVERLAY_ID = "assist-element-picker-banner";
    const HIGHLIGHT_ID = "assist-element-picker-highlight";
    const TEXT_PREVIEW_LIMIT = 160;
    const FIELD_PREVIEW_LIMIT = 120;
    const HOVER_THROTTLE_MS = 16;

    const existingCleanup = window[CLEANUP_KEY];
    if (typeof existingCleanup === "function") {
      existingCleanup();
    }

    function normalizeText(value) {
      return String(value || "").replace(/\s+/g, " ").trim();
    }

    function clipText(value, limit) {
      const text = normalizeText(value);
      if (!limit || text.length <= limit) {
        return text;
      }
      if (limit <= 3) {
        return text.slice(0, limit);
      }
      return `${text.slice(0, Math.max(1, limit - 3))}...`;
    }

    function escapeSelectorValue(value) {
      if (typeof CSS !== "undefined" && typeof CSS.escape === "function") {
        return CSS.escape(String(value));
      }
      return String(value).replace(/["\\]/g, "\\$&");
    }

    function selectorCount(selector) {
      try {
        return document.querySelectorAll(selector).length;
      } catch {
        return 0;
      }
    }

    function uniqueAttributeSelector(tagName, attr, rawValue) {
      const value = normalizeText(rawValue);
      if (!value) {
        return "";
      }
      const selector = `${tagName}[${attr}="${escapeSelectorValue(value)}"]`;
      return selectorCount(selector) === 1 ? selector : "";
    }

    function getRole(element) {
      const explicitRole = normalizeText(element.getAttribute?.("role"));
      if (explicitRole) {
        return explicitRole.toLowerCase();
      }
      const tag = String(element.tagName || "").toLowerCase();
      if (tag === "a" && element.hasAttribute("href")) {
        return "link";
      }
      if (tag === "button") {
        return "button";
      }
      if (tag === "textarea") {
        return "textbox";
      }
      if (tag === "select") {
        return "combobox";
      }
      if (tag === "option") {
        return "option";
      }
      if (tag === "input") {
        const type = String(element.getAttribute("type") || "text").toLowerCase();
        if (["button", "submit", "reset"].includes(type)) {
          return "button";
        }
        if (type === "checkbox") {
          return "checkbox";
        }
        if (type === "radio") {
          return "radio";
        }
        return "textbox";
      }
      return "";
    }

    function isVisible(element) {
      const style = window.getComputedStyle(element);
      if (!style || style.display === "none" || style.visibility === "hidden" || Number(style.opacity) === 0) {
        return false;
      }
      const rect = element.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0;
    }

    function isEditable(element) {
      return (
        element instanceof HTMLInputElement ||
        element instanceof HTMLTextAreaElement ||
        element instanceof HTMLSelectElement ||
        element.isContentEditable === true
      );
    }

    function isEnabled(element) {
      if ("disabled" in element) {
        return element.disabled !== true;
      }
      return normalizeText(element.getAttribute?.("aria-disabled")).toLowerCase() !== "true";
    }

    function getElementText(element) {
      return normalizeText(element.innerText || element.textContent || "");
    }

    function getLabelText(element) {
      const values = [];
      const ariaLabel = normalizeText(element.getAttribute?.("aria-label"));
      if (ariaLabel) {
        values.push(ariaLabel);
      }

      const labelledBy = normalizeText(element.getAttribute?.("aria-labelledby"));
      if (labelledBy) {
        for (const id of labelledBy.split(/\s+/)) {
          const labelNode = id ? document.getElementById(id) : null;
          const text = normalizeText(labelNode?.innerText || labelNode?.textContent || "");
          if (text) {
            values.push(text);
          }
        }
      }

      if (element.labels && element.labels.length > 0) {
        for (const label of Array.from(element.labels)) {
          const text = normalizeText(label?.innerText || label?.textContent || "");
          if (text) {
            values.push(text);
          }
        }
      }

      return normalizeText(values.join(" "));
    }

    function getValuePreview(element) {
      if (element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement) {
        return clipText(element.value, FIELD_PREVIEW_LIMIT);
      }
      if (element instanceof HTMLSelectElement) {
        const selected = element.selectedOptions?.[0] || null;
        return clipText(selected?.text || selected?.value || "", FIELD_PREVIEW_LIMIT);
      }
      if (element.isContentEditable) {
        return clipText(element.textContent || "", FIELD_PREVIEW_LIMIT);
      }
      return "";
    }

    function buildSelectorHint(element) {
      if (!(element instanceof Element)) {
        return "";
      }

      if (element.id) {
        const selector = `#${escapeSelectorValue(element.id)}`;
        if (selectorCount(selector) === 1) {
          return selector;
        }
      }

      const tagName = String(element.tagName || "").toLowerCase() || "*";
      const attributes = [
        ["data-testid", element.getAttribute?.("data-testid")],
        ["data-test", element.getAttribute?.("data-test")],
        ["name", element.getAttribute?.("name")],
        ["aria-label", element.getAttribute?.("aria-label")],
        ["placeholder", element.getAttribute?.("placeholder")],
        ["href", element.getAttribute?.("href")]
      ];

      for (const [attr, value] of attributes) {
        const selector = uniqueAttributeSelector(tagName, attr, value);
        if (selector) {
          return selector;
        }
      }

      const path = [];
      let current = element;
      while (current && current.nodeType === Node.ELEMENT_NODE && current !== document.body) {
        const currentTag = String(current.tagName || "").toLowerCase();
        if (!currentTag) {
          break;
        }
        if (current.id) {
          path.unshift(`#${escapeSelectorValue(current.id)}`);
          const selector = path.join(" > ");
          if (selectorCount(selector) === 1) {
            return selector;
          }
          break;
        }

        let segment = currentTag;
        const parent = current.parentElement;
        if (parent) {
          const siblings = Array.from(parent.children).filter(
            (child) => String(child.tagName || "").toLowerCase() === currentTag
          );
          if (siblings.length > 1) {
            const index = siblings.indexOf(current) + 1;
            segment += `:nth-of-type(${index})`;
          }
        }
        path.unshift(segment);
        const selector = path.join(" > ");
        if (selectorCount(selector) === 1) {
          return selector;
        }
        current = parent;
      }

      return path.join(" > ");
    }

    function buildXPathHint(element) {
      if (!(element instanceof Element)) {
        return "";
      }
      if (element.id) {
        return `//*[@id="${String(element.id).replace(/"/g, '\\"')}"]`;
      }
      const segments = [];
      let current = element;
      while (current && current.nodeType === Node.ELEMENT_NODE) {
        const tagName = String(current.tagName || "").toLowerCase();
        if (!tagName) {
          break;
        }
        const siblings = current.parentElement
          ? Array.from(current.parentElement.children).filter(
              (child) => String(child.tagName || "").toLowerCase() === tagName
            )
          : [];
        const index = siblings.length > 1 ? siblings.indexOf(current) + 1 : 1;
        segments.unshift(siblings.length > 1 ? `${tagName}[${index}]` : tagName);
        current = current.parentElement;
      }
      return segments.length ? `/${segments.join("/")}` : "";
    }

    function buildElementSnapshot(element) {
      const rect = element.getBoundingClientRect();
      const selectorHint = buildSelectorHint(element);
      const fallbackSelector = buildXPathHint(element);
      return {
        tabId: null,
        url: String(location?.href || ""),
        title: clipText(document.title || "", 240),
        selector: selectorHint || fallbackSelector || null,
        xpath: buildXPathHint(element) || null,
        tagName: String(element.tagName || "").toLowerCase(),
        role: getRole(element) || null,
        label: clipText(getLabelText(element), FIELD_PREVIEW_LIMIT) || null,
        name: clipText(element.getAttribute?.("name"), FIELD_PREVIEW_LIMIT) || null,
        placeholder: clipText(element.getAttribute?.("placeholder"), FIELD_PREVIEW_LIMIT) || null,
        enabled: isEnabled(element),
        editable: isEditable(element),
        x: Math.round(rect.left + rect.width / 2),
        y: Math.round(rect.top + rect.height / 2),
        width: Math.round(rect.width),
        height: Math.round(rect.height),
        pickedAt: new Date().toISOString()
      };
    }

    function sendPickerEvent(eventType, payload) {
      try {
        const result = chrome?.runtime?.sendMessage?.({
          type: "assistant.browser.element_picker.page_event",
          requestId,
          eventType,
          payload
        });
        if (result && typeof result.catch === "function") {
          result.catch(() => {});
        }
      } catch {
        // Ignore failures when the extension runtime is briefly unavailable.
      }
    }

    const overlay = document.createElement("div");
    overlay.id = OVERLAY_ID;
    overlay.textContent = "Element picker active. Hover an element and click to select. Press Esc to cancel.";
    overlay.setAttribute("role", "status");
    Object.assign(overlay.style, {
      position: "fixed",
      top: "12px",
      right: "12px",
      zIndex: "2147483647",
      maxWidth: "420px",
      padding: "10px 12px",
      borderRadius: "12px",
      background: "rgba(7, 12, 24, 0.96)",
      color: "#f4f7ff",
      border: "1px solid rgba(255, 179, 0, 0.7)",
      boxShadow: "0 8px 24px rgba(0, 0, 0, 0.35)",
      font: '12px/1.4 "IBM Plex Sans", system-ui, sans-serif',
      pointerEvents: "none"
    });

    const highlight = document.createElement("div");
    highlight.id = HIGHLIGHT_ID;
    Object.assign(highlight.style, {
      position: "fixed",
      zIndex: "2147483646",
      pointerEvents: "none",
      border: "2px solid #ffb300",
      borderRadius: "8px",
      boxShadow: "0 0 0 9999px rgba(3, 6, 12, 0.12), 0 0 0 6px rgba(255, 179, 0, 0.2)",
      transition: "transform 60ms ease, width 60ms ease, height 60ms ease",
      display: "none"
    });

    document.documentElement.appendChild(highlight);
    document.documentElement.appendChild(overlay);

    let currentElement = null;
    let lastHoverAt = 0;

    function resolveCandidate(rawTarget) {
      const element = rawTarget instanceof Element ? rawTarget : rawTarget?.parentElement || null;
      if (!(element instanceof Element)) {
        return null;
      }
      if (!isVisible(element)) {
        return null;
      }
      if (element === document.documentElement || element === document.body) {
        return null;
      }
      if (element.closest(`#${OVERLAY_ID}`) || element.closest(`#${HIGHLIGHT_ID}`)) {
        return null;
      }
      return element;
    }

    function syncHighlight() {
      if (!(currentElement instanceof Element) || !document.contains(currentElement) || !isVisible(currentElement)) {
        highlight.style.display = "none";
        return;
      }
      const rect = currentElement.getBoundingClientRect();
      highlight.style.display = "block";
      highlight.style.left = `${Math.round(rect.left)}px`;
      highlight.style.top = `${Math.round(rect.top)}px`;
      highlight.style.width = `${Math.max(0, Math.round(rect.width))}px`;
      highlight.style.height = `${Math.max(0, Math.round(rect.height))}px`;
    }

    function cleanup() {
      document.removeEventListener("mousemove", onMouseMove, true);
      document.removeEventListener("click", onClick, true);
      document.removeEventListener("keydown", onKeyDown, true);
      window.removeEventListener("scroll", syncHighlight, true);
      window.removeEventListener("resize", syncHighlight, true);
      highlight.remove();
      overlay.remove();
      currentElement = null;
      window[CLEANUP_KEY] = null;
      window[STATE_KEY] = null;
    }

    function onMouseMove(event) {
      const now = Date.now();
      if (now - lastHoverAt < HOVER_THROTTLE_MS) {
        return;
      }
      lastHoverAt = now;
      currentElement = resolveCandidate(event.target);
      syncHighlight();
    }

    function onClick(event) {
      const candidate = resolveCandidate(event.target) || currentElement;
      if (!candidate) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      event.stopImmediatePropagation?.();
      currentElement = candidate;
      syncHighlight();
      sendPickerEvent("result", buildElementSnapshot(candidate));
      cleanup();
    }

    function onKeyDown(event) {
      if (event.key !== "Escape") {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      event.stopImmediatePropagation?.();
      sendPickerEvent("cancelled", { reason: "escape" });
      cleanup();
    }

    document.addEventListener("mousemove", onMouseMove, true);
    document.addEventListener("click", onClick, true);
    document.addEventListener("keydown", onKeyDown, true);
    window.addEventListener("scroll", syncHighlight, true);
    window.addEventListener("resize", syncHighlight, true);

    window[CLEANUP_KEY] = cleanup;
    window[STATE_KEY] = { requestId };

    return {
      started: true,
      requestId
    };
  }

  BG.getPickerActiveTabInfo = getPickerActiveTabInfo;
  BG.clearActivePickerState = clearActivePickerState;
  BG.emitElementPickerEvent = emitElementPickerEvent;
  BG.cleanupElementPickerInTab = cleanupElementPickerInTab;
  BG.cancelActivePicker = cancelActivePicker;
  BG.startElementPicker = startElementPicker;
  BG.cancelElementPicker = cancelElementPicker;
  BG.handleElementPickerPageEvent = handleElementPickerPageEvent;
  BG.startElementPickerInPage = startElementPickerInPage;
  BG.clearElementPickerInPage = clearElementPickerInPage;
})();
