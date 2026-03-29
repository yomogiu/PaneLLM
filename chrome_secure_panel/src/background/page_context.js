(() => {
  const BG = self.PLLM_BG = self.PLLM_BG || {};

  BG.PAGE_CONTEXT_TEXT_CHARS = 5_000;

  async function capturePageContext(tab) {
    const fallback = {
      title: "",
      url: typeof tab?.url === "string" ? tab.url : "",
      content_kind: "unknown",
      selection: "",
      text_excerpt: "",
      heading_path: [],
      selection_context: null
    };

    try {
      await BG.ensureUrlHostPermission(tab?.url);
      const tabId = BG.parseTabId(tab?.id);
      const [injected] = await chrome.scripting.executeScript({
        target: { tabId },
        func: (textLimit, selectionLimit, contextLimit, headingDepth) => {
          const ROOT_SELECTOR = "article, main, [role='main']";
          const BLOCK_SELECTOR = [
            "article",
            "section",
            "main",
            "div",
            "p",
            "li",
            "blockquote",
            "pre",
            "figure",
            "figcaption",
            "h1",
            "h2",
            "h3",
            "h4",
            "h5",
            "h6"
          ].join(", ");
          const HEADING_SELECTOR = "h1, h2, h3, h4, h5, h6";
          const normalizeSpace = (value) => String(value || "").replace(/\s+/g, " ").trim();
          const clip = (value, limit) => normalizeSpace(value).slice(0, limit);
          const isVisible = (element) => {
            if (!(element instanceof Element)) {
              return false;
            }
            const style = window.getComputedStyle(element);
            if (!style || style.display === "none" || style.visibility === "hidden" || Number(style.opacity) === 0) {
              return false;
            }
            const rect = element.getBoundingClientRect();
            return rect.width > 0 && rect.height > 0;
          };
          const getText = (element) => clip(element?.innerText || element?.textContent || "", textLimit);
          const closestElement = (node) => {
            if (!node) {
              return null;
            }
            if (node instanceof Element) {
              return node;
            }
            return node.parentElement || null;
          };
          const pickRoot = () => {
            const candidates = Array.from(document.querySelectorAll(ROOT_SELECTOR)).filter(isVisible);
            let best = null;
            let bestLength = 0;
            for (const candidate of candidates) {
              const length = getText(candidate).length;
              if (length > bestLength) {
                best = candidate;
                bestLength = length;
              }
            }
            return best || document.body || document.documentElement || null;
          };
          const root = pickRoot();
          const selectionObject = typeof window.getSelection === "function" ? window.getSelection() : null;
          const selection = clip(selectionObject?.toString() || "", selectionLimit);
          const range = selectionObject && selectionObject.rangeCount > 0 ? selectionObject.getRangeAt(0) : null;
          const selectedElement = closestElement(range?.commonAncestorContainer) || root;
          const nearestBlock = (element) => {
            let current = element instanceof Element ? element : null;
            while (current && current !== root && !current.matches?.(BLOCK_SELECTOR)) {
              current = current.parentElement;
            }
            return current || root;
          };
          const focusBlock = nearestBlock(selectedElement);
          const focusText = clip(getText(focusBlock), contextLimit * 3);
          let selectionContext = null;
          if (selection) {
            const focusLower = focusText.toLowerCase();
            const selectionLower = selection.toLowerCase();
            const probe = selectionLower.slice(0, Math.min(selectionLower.length, 180));
            const index = probe ? focusLower.indexOf(probe) : -1;
            if (index >= 0) {
              const before = focusText.slice(Math.max(0, index - contextLimit), index).trim();
              const afterStart = Math.min(focusText.length, index + selection.length);
              const after = focusText.slice(afterStart, afterStart + contextLimit).trim();
              selectionContext = {
                before: before.slice(0, contextLimit),
                focus: selection.slice(0, contextLimit),
                after: after.slice(0, contextLimit)
              };
            } else {
              selectionContext = {
                before: "",
                focus: selection.slice(0, contextLimit),
                after: focusText.slice(0, contextLimit)
              };
            }
          }
          const headingPath = [];
          const seen = new Set();
          let probe = focusBlock instanceof Element ? focusBlock : root;
          while (probe && headingPath.length < headingDepth) {
            let current = probe;
            let found = null;
            while (current && !found) {
              if (current.matches?.(HEADING_SELECTOR) && isVisible(current)) {
                found = current;
                break;
              }
              const nested = current.querySelector?.(HEADING_SELECTOR);
              if (nested && isVisible(nested)) {
                found = nested;
                break;
              }
              current = current.previousElementSibling;
            }
            const headingText = clip(found?.innerText || found?.textContent || "", 160);
            if (headingText && !seen.has(headingText)) {
              seen.add(headingText);
              headingPath.unshift(headingText);
            }
            probe = probe?.parentElement || null;
          }
          const textExcerpt = getText(root);
          return {
            title: clip(document.title || "", 240),
            url: clip(location?.href || "", 2000),
            content_kind: textExcerpt ? "html" : "unknown",
            selection,
            text_excerpt: textExcerpt,
            heading_path: headingPath,
            selection_context: selectionContext
          };
        },
        args: [BG.PAGE_CONTEXT_TEXT_CHARS, 1200, 500, 4]
      });
      const result = injected?.result;
      if (!result || typeof result !== "object") {
        return fallback;
      }
      return {
        title: typeof result.title === "string" ? result.title : fallback.title,
        url: typeof result.url === "string" && result.url ? result.url : fallback.url,
        content_kind: result.content_kind === "html" ? "html" : "unknown",
        selection: typeof result.selection === "string" ? result.selection : "",
        text_excerpt: typeof result.text_excerpt === "string" ? result.text_excerpt : "",
        heading_path: Array.isArray(result.heading_path) ? result.heading_path.filter((item) => typeof item === "string") : [],
        selection_context:
          result.selection_context && typeof result.selection_context === "object"
            ? {
                before: typeof result.selection_context.before === "string" ? result.selection_context.before : "",
                focus: typeof result.selection_context.focus === "string" ? result.selection_context.focus : "",
                after: typeof result.selection_context.after === "string" ? result.selection_context.after : ""
              }
            : null
      };
    } catch (error) {
      console.warn("[secure-panel] page context capture fallback:", String(error?.message || error));
      return fallback;
    }
  }

  async function captureReadAssistantContext(_message) {
    const activeTab = await BG.getActiveTab();
    if (!activeTab?.url || typeof activeTab.id !== "number") {
      return {
        ok: false,
        error: "Unable to resolve the active tab.",
        context: null,
        active_tab: null
      };
    }
    const host = BG.extractUrlHost(activeTab.url);
    if (!host) {
      return {
        ok: false,
        error: "Active tab host is invalid.",
        context: null,
        active_tab: null
      };
    }
    const snapshot = BG.getHostPolicySnapshot();
    const activeInfo = {
      tabId: activeTab.id,
      host,
      url: String(activeTab.url),
      title: String(activeTab.title || ""),
      allowed: BG.isHostAllowed(activeTab.url, snapshot.effective_allowed_hosts),
      allowlisted: BG.isHostAllowed(activeTab.url, snapshot.effective_allowed_hosts),
      active: true
    };
    if (!activeInfo.allowed) {
      return {
        ok: false,
        error: "Active tab is not in the extension allowlist.",
        context: null,
        active_tab: activeInfo
      };
    }
    return {
      context: await capturePageContext(activeTab),
      active_tab: activeInfo
    };
  }

  BG.capturePageContext = capturePageContext;
  BG.captureReadAssistantContext = captureReadAssistantContext;
})();
