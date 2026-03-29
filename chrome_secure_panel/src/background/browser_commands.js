(() => {
  const BG = self.PLLM_BG = self.PLLM_BG || {};

  function normalizeGroupColor(value) {
    const allowed = new Set([
      "grey",
      "blue",
      "red",
      "yellow",
      "green",
      "pink",
      "purple",
      "cyan",
      "orange"
    ]);
    const normalized =
      typeof value === "string" && value.trim().length > 0 ? value.trim().toLowerCase() : "grey";
    return allowed.has(normalized) ? normalized : "grey";
  }

  function runGetContentTaskInPage(task) {
    const RAW_MAX_CHARS_DEFAULT = 6_000;
    const RAW_MAX_CHARS_LIMIT = 50_000;
    const NAV_MAX_CHARS_DEFAULT = 1_200;
    const NAV_MAX_CHARS_LIMIT = 6_000;
    const NAV_MAX_ITEMS_DEFAULT = 10;
    const NAV_MAX_ITEMS_LIMIT = 20;
    const FIELD_PREVIEW_LIMIT = 120;
    const TEXT_PREVIEW_LIMIT = 160;
    const HEADING_PREVIEW_LIMIT = 120;

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

    function toPositiveInt(value, fallback, min, max) {
      const parsed = Number.parseInt(String(value ?? fallback), 10);
      if (!Number.isInteger(parsed)) {
        return fallback;
      }
      return Math.min(max, Math.max(min, parsed));
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

    function interactiveSortKey(element) {
      const rect = element.getBoundingClientRect();
      return [Math.round(rect.top), Math.round(rect.left)];
    }

    function buildInteractiveItem(element) {
      const tagName = String(element.tagName || "").toLowerCase();
      const hrefRaw = typeof element.href === "string" ? element.href : "";
      const href = /^https?:/i.test(hrefRaw) ? hrefRaw : null;
      const type =
        element instanceof HTMLInputElement || element instanceof HTMLButtonElement
          ? normalizeText(element.getAttribute?.("type")) || null
          : null;

      return {
        selector: buildSelectorHint(element) || null,
        tagName,
        type,
        role: getRole(element) || null,
        textPreview: clipText(getElementText(element), TEXT_PREVIEW_LIMIT) || null,
        label: clipText(getLabelText(element), FIELD_PREVIEW_LIMIT) || null,
        name: clipText(element.getAttribute?.("name"), FIELD_PREVIEW_LIMIT) || null,
        placeholder: clipText(element.getAttribute?.("placeholder"), FIELD_PREVIEW_LIMIT) || null,
        valuePreview: getValuePreview(element) || null,
        href,
        enabled: isEnabled(element),
        editable: isEditable(element)
      };
    }

    function dedupeBySignature(items) {
      const deduped = [];
      const seen = new Set();
      for (const item of items) {
        const signature = JSON.stringify([
          item.selector,
          item.tagName,
          item.role,
          item.textPreview,
          item.text,
          item.label,
          item.name,
          item.placeholder
        ]);
        if (seen.has(signature)) {
          continue;
        }
        seen.add(signature);
        deduped.push(item);
      }
      return deduped;
    }

    function buildFormSummary(form, maxItems) {
      const fields = Array.from(form.querySelectorAll("input, textarea, select"))
        .filter((element) => isVisible(element))
        .slice(0, Math.min(maxItems, 6))
        .map((element) => ({
          selector: buildSelectorHint(element) || null,
          tagName: String(element.tagName || "").toLowerCase(),
          type: element instanceof HTMLInputElement ? normalizeText(element.type) || null : null,
          role: getRole(element) || null,
          label: clipText(getLabelText(element), FIELD_PREVIEW_LIMIT) || null,
          name: clipText(element.getAttribute?.("name"), FIELD_PREVIEW_LIMIT) || null,
          placeholder: clipText(element.getAttribute?.("placeholder"), FIELD_PREVIEW_LIMIT) || null,
          valuePreview: getValuePreview(element) || null,
          required: typeof element.required === "boolean" ? element.required : false
        }));

      return {
        selector: buildSelectorHint(form) || null,
        name: clipText(form.getAttribute?.("name"), FIELD_PREVIEW_LIMIT) || null,
        method: normalizeText(form.getAttribute?.("method") || "get").toLowerCase() || "get",
        action: clipText(form.getAttribute?.("action"), FIELD_PREVIEW_LIMIT) || null,
        fieldCount: Array.from(form.querySelectorAll("input, textarea, select")).length,
        fields
      };
    }

    const selector = typeof task?.selector === "string" && task.selector.trim().length > 0 ? task.selector.trim() : null;
    const mode = task?.mode === "raw_html" ? "raw_html" : "navigation";
    const rawMaxChars = toPositiveInt(task?.maxChars, RAW_MAX_CHARS_DEFAULT, 1, RAW_MAX_CHARS_LIMIT);
    const navMaxChars = toPositiveInt(task?.maxChars, NAV_MAX_CHARS_DEFAULT, 250, NAV_MAX_CHARS_LIMIT);
    const maxItems = toPositiveInt(task?.maxItems, NAV_MAX_ITEMS_DEFAULT, 1, NAV_MAX_ITEMS_LIMIT);
    const node = selector ? document.querySelector(selector) : document.documentElement;
    const title = clipText(document.title, 200);
    const finalUrl = String(window.location.href || "");

    if (!node) {
      if (mode === "raw_html") {
        return {
          mode,
          html: "",
          fullLength: 0,
          truncated: false,
          finalUrl,
          title,
          selector,
          selectorMatched: false
        };
      }
      return {
        mode,
        title,
        finalUrl,
        selector,
        selectorMatched: false,
        textPreview: "",
        headings: [],
        interactive: [],
        forms: [],
        counts: {
          headingCount: 0,
          interactiveCount: 0,
          formCount: 0
        },
        truncated: {
          textPreview: false,
          headings: false,
          interactive: false,
          forms: false
        }
      };
    }

    if (mode === "raw_html") {
      const html = typeof node.outerHTML === "string" ? node.outerHTML : String(node.textContent ?? "");
      return {
        mode,
        html: html.slice(0, rawMaxChars),
        fullLength: html.length,
        truncated: html.length > rawMaxChars,
        finalUrl,
        title,
        selector,
        selectorMatched: selector ? true : null
      };
    }

    const root = node;
    const rootText =
      typeof root.innerText === "string"
        ? root.innerText
        : typeof root.textContent === "string"
          ? root.textContent
          : "";
    const normalizedText = normalizeText(rootText);
    const textPreview = clipText(normalizedText, navMaxChars);

    const headingNodes = [];
    if (root instanceof Element && /^h[1-6]$/i.test(root.tagName || "")) {
      headingNodes.push(root);
    }
    headingNodes.push(...Array.from(root.querySelectorAll?.("h1, h2, h3, h4, h5, h6") || []));
    const visibleHeadings = dedupeBySignature(
      headingNodes
        .filter((element) => element instanceof Element && isVisible(element))
        .map((element) => ({
          selector: buildSelectorHint(element) || null,
          level: Number.parseInt(String(element.tagName || "").replace(/^h/i, ""), 10) || null,
          text: clipText(getElementText(element), HEADING_PREVIEW_LIMIT) || null
        }))
        .filter((item) => item.text)
    );
    const headings = visibleHeadings.slice(0, Math.min(maxItems, 8));

    const interactiveSelector = [
      "a[href]",
      "button",
      "input",
      "select",
      "textarea",
      "[role='button']",
      "[role='link']",
      "[role='textbox']",
      "[role='combobox']",
      "[contenteditable='true']",
      "[contenteditable='']"
    ].join(", ");
    const interactiveNodes = [];
    if (root instanceof Element && root.matches?.(interactiveSelector)) {
      interactiveNodes.push(root);
    }
    interactiveNodes.push(...Array.from(root.querySelectorAll?.(interactiveSelector) || []));
    interactiveNodes.sort((left, right) => {
      const [leftTop, leftLeft] = interactiveSortKey(left);
      const [rightTop, rightLeft] = interactiveSortKey(right);
      if (leftTop !== rightTop) {
        return leftTop - rightTop;
      }
      return leftLeft - rightLeft;
    });
    const interactiveItems = dedupeBySignature(
      interactiveNodes
        .filter((element) => element instanceof Element && isVisible(element))
        .map((element) => buildInteractiveItem(element))
        .filter((item) => item.selector || item.textPreview || item.label || item.name || item.placeholder)
    );
    const interactive = interactiveItems.slice(0, maxItems);

    const formNodes = [];
    if (root instanceof HTMLFormElement) {
      formNodes.push(root);
    }
    formNodes.push(...Array.from(root.querySelectorAll?.("form") || []));
    const visibleForms = formNodes.filter((form) => form instanceof HTMLFormElement && isVisible(form));
    const forms = visibleForms.slice(0, Math.min(maxItems, 4)).map((form) => buildFormSummary(form, maxItems));

    return {
      mode,
      title,
      finalUrl,
      selector,
      selectorMatched: selector ? true : null,
      textPreview,
      headings,
      interactive,
      forms,
      counts: {
        headingCount: visibleHeadings.length,
        interactiveCount: interactiveItems.length,
        formCount: visibleForms.length
      },
      truncated: {
        textPreview: normalizedText.length > textPreview.length,
        headings: visibleHeadings.length > headings.length,
        interactive: interactiveItems.length > interactive.length,
        forms: visibleForms.length > forms.length
      }
    };
  }

  function runHighlightTaskInPage(task) {
    const CLEANUP_KEY = "__assistReadAssistantHighlightCleanup";
    const BLOCK_SELECTOR = "article, main, section, p, li, blockquote, pre, div, h1, h2, h3, h4, h5, h6";
    const normalizeText = (value) => String(value || "").replace(/\s+/g, " ").trim();
    const preview = (value, limit = 200) => normalizeText(value).slice(0, limit);
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
    const getText = (element) => preview(element?.innerText || element?.textContent || "", 5000);
    const getRole = (element) => normalizeText(element.getAttribute?.("role")).toLowerCase();
    const getLabel = (element) => normalizeText([
      element.getAttribute?.("aria-label"),
      element.getAttribute?.("name"),
      element.getAttribute?.("placeholder")
    ].join(" "));
    const clearExisting = () => {
      const cleanup = window[CLEANUP_KEY];
      if (typeof cleanup === "function") {
        cleanup();
      }
    };
    const selectByText = (query) => {
      const needle = normalizeText(query).toLowerCase();
      if (!needle) {
        return null;
      }
      const root = document.querySelector("article, main, [role='main']") || document.body || document.documentElement;
      const candidates = Array.from(root.querySelectorAll(BLOCK_SELECTOR)).filter(isVisible);
      return candidates.find((element) => getText(element).toLowerCase().includes(needle)) || null;
    };
    const selectByLocator = (locator) => {
      if (!locator || typeof locator !== "object") {
        return null;
      }
      if (typeof locator.selector === "string" && locator.selector.trim()) {
        try {
          const selectorMatch = Array.from(document.querySelectorAll(locator.selector.trim())).find((element) => {
            if (!isVisible(element)) {
              return false;
            }
            if (locator.text && !getText(element).toLowerCase().includes(normalizeText(locator.text).toLowerCase())) {
              return false;
            }
            return true;
          });
          if (selectorMatch) {
            return selectorMatch;
          }
        } catch {
          // Ignore invalid selectors from model input.
        }
      }
      const all = Array.from(document.querySelectorAll("*")).filter(isVisible);
      return all.find((element) => {
        if (locator.text && !getText(element).toLowerCase().includes(normalizeText(locator.text).toLowerCase())) {
          return false;
        }
        if (locator.label && !getLabel(element).toLowerCase().includes(normalizeText(locator.label).toLowerCase())) {
          return false;
        }
        if (locator.role && getRole(element) !== normalizeText(locator.role).toLowerCase()) {
          return false;
        }
        if (locator.name && !normalizeText(element.getAttribute?.("name")).toLowerCase().includes(normalizeText(locator.name).toLowerCase())) {
          return false;
        }
        if (locator.placeholder && !normalizeText(element.getAttribute?.("placeholder")).toLowerCase().includes(normalizeText(locator.placeholder).toLowerCase())) {
          return false;
        }
        return Boolean(locator.text || locator.label || locator.role || locator.name || locator.placeholder);
      }) || null;
    };

    const durationMs = Number.isInteger(task?.durationMs) ? Math.max(500, Math.min(task.durationMs, 20000)) : 6000;
    const strategy = task?.locator ? "locator" : "text";
    const target = selectByLocator(task?.locator) || selectByText(task?.text || "");
    clearExisting();
    if (!target) {
      return {
        ok: true,
        highlighted: false,
        strategy,
        preview_text: "",
        duration_ms: durationMs
      };
    }
    if (task?.scroll !== false) {
      target.scrollIntoView({ behavior: "instant", block: "center", inline: "nearest" });
    }
    const previous = {
      outline: target.style.outline,
      outlineOffset: target.style.outlineOffset,
      boxShadow: target.style.boxShadow,
      transition: target.style.transition
    };
    target.setAttribute("data-assist-read-highlight", "true");
    target.style.outline = "3px solid #ffb300";
    target.style.outlineOffset = "4px";
    target.style.boxShadow = "0 0 0 8px rgba(255, 179, 0, 0.22)";
    target.style.transition = "outline-color 120ms ease, box-shadow 120ms ease";
    window[CLEANUP_KEY] = () => {
      target.style.outline = previous.outline || "";
      target.style.outlineOffset = previous.outlineOffset || "";
      target.style.boxShadow = previous.boxShadow || "";
      target.style.transition = previous.transition || "";
      target.removeAttribute("data-assist-read-highlight");
      window[CLEANUP_KEY] = null;
    };
    window.setTimeout(() => {
      if (typeof window[CLEANUP_KEY] === "function") {
        window[CLEANUP_KEY]();
      }
    }, durationMs);
    return {
      ok: true,
      highlighted: true,
      strategy,
      preview_text: preview(getText(target), 200),
      duration_ms: durationMs
    };
  }

  async function commandNavigate(args, allowedHosts) {
    const tabId = await BG.resolveTabId(args.tabId);
    const url = BG.ensureUrl(args.url, allowedHosts);
    await BG.ensureUrlHostPermission(url);
    await chrome.tabs.update(tabId, { url });
    await BG.waitForTabLoad(tabId, 20_000);
    const tab = await chrome.tabs.get(tabId);
    if (!tab?.url || !BG.isHostAllowed(tab.url, allowedHosts)) {
      throw new Error("Navigation completed on a non-allowlisted host.");
    }
    return {
      tabId,
      requestedUrl: url,
      finalUrl: tab.url,
      title: tab.title ?? null
    };
  }

  async function commandOpenTab(args, allowedHosts) {
    const url = BG.ensureUrl(args.url, allowedHosts);
    await BG.ensureUrlHostPermission(url);
    const tab = await chrome.tabs.create({ url, active: true });
    if (typeof tab?.id !== "number") {
      throw new Error("Unable to open browser tab.");
    }
    let resolvedTab = tab;
    try {
      await BG.waitForTabLoad(tab.id, 20_000);
      const latest = await chrome.tabs.get(tab.id);
      if (latest) {
        resolvedTab = latest;
      }
    } catch {
      // Keep best-effort behavior: if loading is slow, still return the opened tab.
    }

    let policyUrl = resolvedTab.pendingUrl || resolvedTab.url || url;
    try {
      const parsed = new URL(policyUrl);
      if (!["http:", "https:"].includes(parsed.protocol)) {
        policyUrl = url;
      }
    } catch {
      policyUrl = url;
    }

    if (!BG.isHostAllowed(policyUrl, allowedHosts)) {
      await chrome.tabs.remove(tab.id);
      throw new Error("Opened tab host is not allowlisted.");
    }
    return {
      tabId: tab.id,
      url: resolvedTab.url || resolvedTab.pendingUrl || url,
      title: resolvedTab.title ?? null
    };
  }

  async function commandSwitchTab(args, allowedHosts) {
    const tabId = BG.parseTabId(args.tabId);
    await BG.getAllowedTab(tabId, allowedHosts);
    const tab = await chrome.tabs.update(tabId, { active: true });
    return {
      tabId,
      url: tab.url ?? null,
      title: tab.title ?? null
    };
  }

  async function commandFocusTab(args, allowedHosts) {
    const tabId = BG.parseTabId(args.tabId);
    await BG.getAllowedTab(tabId, allowedHosts);
    const tab = await chrome.tabs.update(tabId, { active: true });
    return {
      tabId,
      focused: true,
      url: tab.url ?? null,
      title: tab.title ?? null
    };
  }

  async function commandCloseTab(args, allowedHosts) {
    const tabId = BG.parseTabId(args.tabId);
    await BG.getAllowedTab(tabId, allowedHosts);
    await chrome.tabs.remove(tabId);
    return {
      tabId,
      closed: true
    };
  }

  async function commandGetTabs(allowedHosts) {
    const tabs = await chrome.tabs.query({ currentWindow: true });
    const filtered = tabs.filter((tab) => typeof tab.id === "number" && tab.url && BG.isHostAllowed(tab.url, allowedHosts));
    const activeTab = filtered.find((tab) => tab.active);
    return {
      activeTabId: typeof activeTab?.id === "number" ? activeTab.id : null,
      tabs: filtered.map((tab) => ({
        tabId: tab.id,
        title: tab.title ?? null,
        url: tab.url ?? null,
        active: tab.active === true,
        groupId: typeof tab.groupId === "number" && tab.groupId >= 0 ? tab.groupId : null
      }))
    };
  }

  async function commandDescribeSessionTabs(allowedHosts) {
    const tabsResult = await commandGetTabs(allowedHosts);
    const groupsRaw = await chrome.tabGroups.query({ windowId: chrome.windows.WINDOW_ID_CURRENT });
    const groupById = new Map(
      groupsRaw.map((group) => [
        group.id,
        {
          groupId: group.id,
          groupName: group.title ?? "Session Group",
          color: group.color ?? "grey",
          collapsed: group.collapsed === true,
          tabIds: []
        }
      ])
    );

    for (const tab of tabsResult.tabs) {
      if (typeof tab.groupId === "number" && tab.groupId >= 0) {
        const group = groupById.get(tab.groupId);
        if (group) {
          group.tabIds.push(tab.tabId);
        }
      }
    }

    const groups = [...groupById.values()].filter((group) => group.tabIds.length > 0);
    return {
      activeTabId: tabsResult.activeTabId,
      tabCount: tabsResult.tabs.length,
      tabs: tabsResult.tabs.map((tab) => {
        const group = typeof tab.groupId === "number" ? groupById.get(tab.groupId) : null;
        return {
          tabId: tab.tabId,
          title: tab.title,
          url: tab.url,
          groupId: tab.groupId,
          groupName: group?.groupName ?? null
        };
      }),
      groups
    };
  }

  async function commandGroupTabs(args, allowedHosts) {
    const tabIds = Array.isArray(args.tabIds) ? args.tabIds.map(BG.parseTabId) : [];
    if (tabIds.length === 0) {
      throw new Error("group_tabs requires at least one tabId.");
    }
    for (const tabId of tabIds) {
      await BG.getAllowedTab(tabId, allowedHosts);
    }

    const groupName = typeof args.groupName === "string" && args.groupName.trim() ? args.groupName.trim() : "Session Group";
    const color = normalizeGroupColor(args.color);
    const collapsed = args.collapsed === true;

    const groupId = await chrome.tabs.group({ tabIds });
    await chrome.tabGroups.update(groupId, {
      title: groupName,
      color,
      collapsed
    });

    return {
      groupId,
      groupName,
      color,
      collapsed,
      tabIds
    };
  }

  async function commandClick(args, allowedHosts) {
    const tabId = await BG.resolveTabId(args.tabId);
    await BG.getAllowedTab(tabId, allowedHosts);
    const selector = BG.ensureSelector(args.selector);
    const result = await BG.runInTab(
      tabId,
      (sel) => {
        const element = document.querySelector(sel);
        if (!element) {
          return { clicked: false, error: "selector_not_found" };
        }
        element.scrollIntoView({ block: "center", inline: "center" });
        const rect = element.getBoundingClientRect();
        const x = Math.round(rect.left + rect.width / 2);
        const y = Math.round(rect.top + rect.height / 2);
        element.click();
        return {
          clicked: true,
          x,
          y,
          tagName: String(element.tagName || "").toLowerCase(),
          textPreview: String((element.innerText || "").slice(0, 120)),
          finalUrl: window.location.href,
          title: document.title
        };
      },
      [selector]
    );

    if (!result?.clicked) {
      throw new Error(`click failed: ${selector}`);
    }
    return result;
  }

  async function commandType(args, allowedHosts) {
    const tabId = await BG.resolveTabId(args.tabId);
    await BG.getAllowedTab(tabId, allowedHosts);
    const selector = BG.ensureSelector(args.selector);
    const text = typeof args.text === "string" ? args.text : "";
    const clear = args.clear !== false;

    const result = await BG.runInTab(
      tabId,
      (sel, value, shouldClear) => {
        const element = document.querySelector(sel);
        if (!element) {
          return { ok: false, error: "selector_not_found" };
        }
        element.scrollIntoView({ block: "center", inline: "center" });
        try {
          element.focus({ preventScroll: true });
        } catch {
          element.focus();
        }

        const isInput = element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement;
        const isEditable = element.isContentEditable;
        if (!isInput && !isEditable) {
          return { ok: false, error: "not_editable" };
        }

        if (isInput) {
          const current = shouldClear ? "" : String(element.value ?? "");
          element.value = current + value;
        } else {
          const current = shouldClear ? "" : String(element.textContent ?? "");
          element.textContent = current + value;
        }

        element.dispatchEvent(new Event("input", { bubbles: true }));
        element.dispatchEvent(new Event("change", { bubbles: true }));

        return {
          ok: true,
          typedChars: value.length,
          tagName: String(element.tagName || "").toLowerCase(),
          type: String(element.getAttribute("type") || "").toLowerCase()
        };
      },
      [selector, text, clear]
    );

    if (!result?.ok) {
      throw new Error(`type failed: ${selector}`);
    }

    return {
      typedChars: Number(result.typedChars ?? text.length),
      tagName: result.tagName ?? null,
      type: result.type ?? null
    };
  }

  async function commandPressKey(args, allowedHosts) {
    const tabId = await BG.resolveTabId(args.tabId);
    await BG.getAllowedTab(tabId, allowedHosts);
    const key = typeof args.key === "string" && args.key.length > 0 ? args.key : "Enter";
    const modifiers = Array.isArray(args.modifiers)
      ? args.modifiers.filter((modifier) => typeof modifier === "string")
      : [];
    const repeat = Math.max(1, Number.parseInt(String(args.repeat ?? 1), 10));
    const delayMs = Math.max(0, Number.parseInt(String(args.delayMs ?? 0), 10));

    await BG.runInTab(
      tabId,
      async (pressedKey, modifierList, totalRepeat, perRepeatDelayMs) => {
        const target = document.activeElement || document.body;
        const modifierFlags = {
          altKey: modifierList.includes("alt"),
          ctrlKey: modifierList.includes("ctrl") || modifierList.includes("control"),
          metaKey: modifierList.includes("meta") || modifierList.includes("cmd") || modifierList.includes("command"),
          shiftKey: modifierList.includes("shift")
        };

        for (let index = 0; index < totalRepeat; index += 1) {
          target.dispatchEvent(
            new KeyboardEvent("keydown", {
              key: pressedKey,
              code: pressedKey,
              bubbles: true,
              cancelable: true,
              ...modifierFlags
            })
          );
          target.dispatchEvent(
            new KeyboardEvent("keyup", {
              key: pressedKey,
              code: pressedKey,
              bubbles: true,
              cancelable: true,
              ...modifierFlags
            })
          );

          if (perRepeatDelayMs > 0 && index < totalRepeat - 1) {
            await new Promise((resolve) => setTimeout(resolve, perRepeatDelayMs));
          }
        }
        return true;
      },
      [key, modifiers.map((value) => value.toLowerCase()), repeat, delayMs]
    );

    return {
      key,
      code: key,
      repeat,
      modifiers
    };
  }

  async function commandScroll(args, allowedHosts) {
    const tabId = await BG.resolveTabId(args.tabId);
    await BG.getAllowedTab(tabId, allowedHosts);
    const deltaX = Number.parseFloat(String(args.deltaX ?? 0));
    const deltaY = Number.parseFloat(String(args.deltaY ?? 600));
    const selector =
      typeof args.selector === "string" && args.selector.trim().length > 0 ? args.selector.trim() : null;

    const result = await BG.runInTab(
      tabId,
      (x, y, sel) => {
        if (sel) {
          const element = document.querySelector(sel);
          if (!element) {
            return { ok: false, error: "selector_not_found" };
          }
          element.scrollBy({ left: x, top: y, behavior: "instant" });
          const rect = element.getBoundingClientRect();
          return {
            ok: true,
            x: Math.round(rect.left + rect.width / 2),
            y: Math.round(rect.top + rect.height / 2),
            deltaX: x,
            deltaY: y
          };
        }

        window.scrollBy({ left: x, top: y, behavior: "instant" });
        return {
          ok: true,
          x: Math.round((window.innerWidth || 1200) / 2),
          y: Math.round((window.innerHeight || 900) / 2),
          deltaX: x,
          deltaY: y
        };
      },
      [deltaX, deltaY, selector]
    );

    if (!result?.ok) {
      throw new Error(`scroll failed${selector ? `: ${selector}` : ""}`);
    }
    return result;
  }

  async function commandHighlight(args, allowedHosts) {
    const tabId = await BG.resolveTabId(args.tabId);
    await BG.getAllowedTab(tabId, allowedHosts);
    const locator = args.locator && typeof args.locator === "object"
      ? BG.normalizeLocator(args.locator, { defaultVisible: true })
      : null;
    const result = await BG.runInTab(tabId, runHighlightTaskInPage, [{
      locator,
      text: typeof args.text === "string" ? args.text : "",
      scroll: args.scroll !== false,
      durationMs: Number.isInteger(args.durationMs) ? args.durationMs : 6000
    }]);
    return {
      ok: Boolean(result?.ok),
      tabId,
      highlighted: Boolean(result?.highlighted),
      strategy: result?.strategy || (locator ? "locator" : "text"),
      preview_text: typeof result?.preview_text === "string" ? result.preview_text : "",
      duration_ms: Number.isInteger(result?.duration_ms) ? result.duration_ms : 6000
    };
  }

  async function commandGetContent(args, allowedHosts) {
    const tabId = await BG.resolveTabId(args.tabId);
    await BG.getAllowedTab(tabId, allowedHosts);
    const selector = typeof args.selector === "string" && args.selector.trim().length > 0 ? args.selector.trim() : null;
    const mode = typeof args.mode === "string" ? args.mode.trim().toLowerCase() : "navigation";
    const requestedMaxChars = Number.parseInt(String(args.maxChars ?? (mode === "raw_html" ? 6000 : 1200)), 10);
    const requestedMaxItems = Number.parseInt(String(args.maxItems ?? 10), 10);

    return await BG.runInTab(tabId, runGetContentTaskInPage, [
      {
        selector,
        mode,
        maxChars: requestedMaxChars,
        maxItems: requestedMaxItems
      }
    ]);
  }

  async function commandFindOne(args, allowedHosts) {
    const tabId = await BG.resolveTabId(args.tabId);
    await BG.getAllowedTab(tabId, allowedHosts);
    const locator = BG.normalizeLocator(args.locator, { defaultVisible: true });
    return await BG.runInTab(tabId, runLocatorTaskInPage, [{ kind: "find_one", locator }]);
  }

  async function commandFindElements(args, allowedHosts) {
    const tabId = await BG.resolveTabId(args.tabId);
    await BG.getAllowedTab(tabId, allowedHosts);
    const locator = BG.normalizeLocator(args.locator, { defaultVisible: true });
    const limit = BG.normalizePositiveInt(args.limit, 10, { min: 1, max: 20 });
    return await BG.runInTab(tabId, runLocatorTaskInPage, [{ kind: "find_elements", locator, limit }]);
  }

  async function commandWaitFor(args, allowedHosts) {
    const tabId = await BG.resolveTabId(args.tabId);
    await BG.getAllowedTab(tabId, allowedHosts);
    const locator = BG.normalizeLocator(args.locator, { defaultVisible: null, allowVisibility: false });
    const condition = BG.normalizeWaitCondition(args.condition);
    const timeoutMs = BG.normalizePositiveInt(args.timeoutMs, 10_000, { min: 100, max: 60_000 });
    const pollMs = BG.normalizePositiveInt(args.pollMs, 250, { min: 50, max: 5_000 });
    const startedAt = Date.now();
    let lastProbe = null;

    while (Date.now() - startedAt <= timeoutMs) {
      lastProbe = await BG.runInTab(tabId, runLocatorTaskInPage, [{ kind: "probe", locator }]);
      const totalCount = Number(lastProbe?.totalCount ?? 0);
      const visibleCount = Number(lastProbe?.visibleCount ?? 0);
      const satisfied =
        (condition === "present" && totalCount > 0) ||
        (condition === "visible" && visibleCount > 0) ||
        (condition === "hidden" && totalCount > 0 && visibleCount === 0) ||
        (condition === "gone" && totalCount === 0);

      if (satisfied) {
        const element =
          condition === "visible"
            ? lastProbe?.firstVisible || lastProbe?.firstMatch || null
            : condition === "gone"
              ? null
              : lastProbe?.firstMatch || lastProbe?.firstVisible || null;
        return {
          condition,
          satisfied: true,
          elapsedMs: Date.now() - startedAt,
          matchCount: totalCount,
          visibleCount,
          element
        };
      }

      await BG.delay(pollMs);
    }

    throw new Error(`wait_for timed out: ${condition}`);
  }

  async function commandGetElementState(args, allowedHosts) {
    const tabId = await BG.resolveTabId(args.tabId);
    await BG.getAllowedTab(tabId, allowedHosts);
    const locator = BG.normalizeLocator(args.locator, { defaultVisible: true });
    return await BG.runInTab(tabId, runLocatorTaskInPage, [{ kind: "get_state", locator }]);
  }

  async function commandSelectOption(args, allowedHosts) {
    const tabId = await BG.resolveTabId(args.tabId);
    await BG.getAllowedTab(tabId, allowedHosts);
    const locator = BG.normalizeLocator(args.locator, { defaultVisible: true });
    const selection = BG.normalizeSelectOptionRequest(args);
    const result = await BG.runInTab(tabId, runLocatorTaskInPage, [
      {
        kind: "select_option",
        locator,
        ...selection
      }
    ]);

    if (!result?.ok) {
      throw new Error(`select_option failed: ${result?.error || "unknown_error"}`);
    }

    return {
      matchCount: Number(result.matchCount ?? 1),
      selectedValue: result.selectedValue ?? null,
      selectedText: result.selectedText ?? null,
      selectedIndex: Number.isInteger(result.selectedIndex) ? result.selectedIndex : null,
      element: result.element ?? null
    };
  }

  function runLocatorTaskInPage(task) {
    const TEXT_PREVIEW_LIMIT = 160;

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
        return clipText(element.value, TEXT_PREVIEW_LIMIT);
      }
      if (element instanceof HTMLSelectElement) {
        const selected = element.selectedOptions?.[0] || null;
        return clipText(selected?.text || selected?.value || "", TEXT_PREVIEW_LIMIT);
      }
      if (element.isContentEditable) {
        return clipText(element.textContent || "", TEXT_PREVIEW_LIMIT);
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
        label: clipText(getLabelText(element), TEXT_PREVIEW_LIMIT) || null,
        name: clipText(element.getAttribute?.("name"), TEXT_PREVIEW_LIMIT) || null,
        placeholder: clipText(element.getAttribute?.("placeholder"), TEXT_PREVIEW_LIMIT) || null,
        enabled: isEnabled(element),
        editable: isEditable(element),
        x: Math.round(rect.left + rect.width / 2),
        y: Math.round(rect.top + rect.height / 2),
        width: Math.round(rect.width),
        height: Math.round(rect.height),
        pickedAt: new Date().toISOString()
      };
    }

    function dedupeBySignature(items) {
      const deduped = [];
      const seen = new Set();
      for (const item of items) {
        const signature = JSON.stringify([
          item.selector,
          item.tagName,
          item.role,
          item.textPreview,
          item.text,
          item.label,
          item.name,
          item.placeholder
        ]);
        if (seen.has(signature)) {
          continue;
        }
        seen.add(signature);
        deduped.push(item);
      }
      return deduped;
    }

    function interactiveSortKey(element) {
      const rect = element.getBoundingClientRect();
      return [Math.round(rect.top), Math.round(rect.left)];
    }

    function buildInteractiveItem(element) {
      const tagName = String(element.tagName || "").toLowerCase();
      const hrefRaw = typeof element.href === "string" ? element.href : "";
      const href = /^https?:/i.test(hrefRaw) ? hrefRaw : null;
      const type =
        element instanceof HTMLInputElement || element instanceof HTMLButtonElement
          ? normalizeText(element.getAttribute?.("type")) || null
          : null;

      return {
        selector: buildSelectorHint(element) || null,
        tagName,
        type,
        role: getRole(element) || null,
        textPreview: clipText(getElementText(element), TEXT_PREVIEW_LIMIT) || null,
        label: clipText(getLabelText(element), TEXT_PREVIEW_LIMIT) || null,
        name: clipText(element.getAttribute?.("name"), TEXT_PREVIEW_LIMIT) || null,
        placeholder: clipText(element.getAttribute?.("placeholder"), TEXT_PREVIEW_LIMIT) || null,
        valuePreview: getValuePreview(element) || null,
        href,
        enabled: isEnabled(element),
        editable: isEditable(element)
      };
    }

    function buildFormSummary(form, maxItems) {
      const fields = Array.from(form.querySelectorAll("input, textarea, select"))
        .filter((element) => isVisible(element))
        .slice(0, Math.min(maxItems, 6))
        .map((element) => ({
          selector: buildSelectorHint(element) || null,
          tagName: String(element.tagName || "").toLowerCase(),
          type: element instanceof HTMLInputElement ? normalizeText(element.type) || null : null,
          role: getRole(element) || null,
          label: clipText(getLabelText(element), TEXT_PREVIEW_LIMIT) || null,
          name: clipText(element.getAttribute?.("name"), TEXT_PREVIEW_LIMIT) || null,
          placeholder: clipText(element.getAttribute?.("placeholder"), TEXT_PREVIEW_LIMIT) || null,
          valuePreview: getValuePreview(element) || null,
          required: typeof element.required === "boolean" ? element.required : false
        }));

      return {
        selector: buildSelectorHint(form) || null,
        name: clipText(form.getAttribute?.("name"), TEXT_PREVIEW_LIMIT) || null,
        method: normalizeText(form.getAttribute?.("method") || "get").toLowerCase() || "get",
        action: clipText(form.getAttribute?.("action"), TEXT_PREVIEW_LIMIT) || null,
        fieldCount: Array.from(form.querySelectorAll("input, textarea, select")).length,
        fields
      };
    }

    const selector = typeof task?.selector === "string" && task.selector.trim().length > 0 ? task.selector.trim() : null;
    const mode = task?.mode === "raw_html" ? "raw_html" : "navigation";
    const rawMaxChars = toPositiveInt(task?.maxChars, 6_000, 1, 50_000);
    const navMaxChars = toPositiveInt(task?.maxChars, 1_200, 250, 6_000);
    const maxItems = toPositiveInt(task?.maxItems, 10, 1, 20);
    const node = selector ? document.querySelector(selector) : document.documentElement;
    const title = clipText(document.title, 200);
    const finalUrl = String(window.location.href || "");

    if (!node) {
      if (mode === "raw_html") {
        return {
          mode,
          html: "",
          fullLength: 0,
          truncated: false,
          finalUrl,
          title,
          selector,
          selectorMatched: false
        };
      }
      return {
        mode,
        title,
        finalUrl,
        selector,
        selectorMatched: false,
        textPreview: "",
        headings: [],
        interactive: [],
        forms: [],
        counts: {
          headingCount: 0,
          interactiveCount: 0,
          formCount: 0
        },
        truncated: {
          textPreview: false,
          headings: false,
          interactive: false,
          forms: false
        }
      };
    }

    if (mode === "raw_html") {
      const html = typeof node.outerHTML === "string" ? node.outerHTML : String(node.textContent ?? "");
      return {
        mode,
        html: html.slice(0, rawMaxChars),
        fullLength: html.length,
        truncated: html.length > rawMaxChars,
        finalUrl,
        title,
        selector,
        selectorMatched: selector ? true : null
      };
    }

    const root = node;
    const rootText =
      typeof root.innerText === "string"
        ? root.innerText
        : typeof root.textContent === "string"
          ? root.textContent
          : "";
    const normalizedText = normalizeText(rootText);
    const textPreview = clipText(normalizedText, navMaxChars);

    const headingNodes = [];
    if (root instanceof Element && /^h[1-6]$/i.test(root.tagName || "")) {
      headingNodes.push(root);
    }
    headingNodes.push(...Array.from(root.querySelectorAll?.("h1, h2, h3, h4, h5, h6") || []));
    const visibleHeadings = dedupeBySignature(
      headingNodes
        .filter((element) => element instanceof Element && isVisible(element))
        .map((element) => ({
          selector: buildSelectorHint(element) || null,
          level: Number.parseInt(String(element.tagName || "").replace(/^h/i, ""), 10) || null,
          text: clipText(getElementText(element), HEADING_PREVIEW_LIMIT) || null
        }))
        .filter((item) => item.text)
    );
    const headings = visibleHeadings.slice(0, Math.min(maxItems, 8));

    const interactiveSelector = [
      "a[href]",
      "button",
      "input",
      "select",
      "textarea",
      "[role='button']",
      "[role='link']",
      "[role='textbox']",
      "[role='combobox']",
      "[contenteditable='true']",
      "[contenteditable='']"
    ].join(", ");
    const interactiveNodes = [];
    if (root instanceof Element && root.matches?.(interactiveSelector)) {
      interactiveNodes.push(root);
    }
    interactiveNodes.push(...Array.from(root.querySelectorAll?.(interactiveSelector) || []));
    interactiveNodes.sort((left, right) => {
      const [leftTop, leftLeft] = interactiveSortKey(left);
      const [rightTop, rightLeft] = interactiveSortKey(right);
      if (leftTop !== rightTop) {
        return leftTop - rightTop;
      }
      return leftLeft - rightLeft;
    });
    const interactiveItems = dedupeBySignature(
      interactiveNodes
        .filter((element) => element instanceof Element && isVisible(element))
        .map((element) => buildInteractiveItem(element))
        .filter((item) => item.selector || item.textPreview || item.label || item.name || item.placeholder)
    );
    const interactive = interactiveItems.slice(0, maxItems);

    const formNodes = [];
    if (root instanceof HTMLFormElement) {
      formNodes.push(root);
    }
    formNodes.push(...Array.from(root.querySelectorAll?.("form") || []));
    const visibleForms = formNodes.filter((form) => form instanceof HTMLFormElement && isVisible(form));
    const forms = visibleForms.slice(0, Math.min(maxItems, 4)).map((form) => buildFormSummary(form, maxItems));

    return {
      mode,
      title,
      finalUrl,
      selector,
      selectorMatched: selector ? true : null,
      textPreview,
      headings,
      interactive,
      forms,
      counts: {
        headingCount: visibleHeadings.length,
        interactiveCount: interactiveItems.length,
        formCount: visibleForms.length
      },
      truncated: {
        textPreview: normalizedText.length > textPreview.length,
        headings: visibleHeadings.length > headings.length,
        interactive: interactiveItems.length > interactive.length,
        forms: visibleForms.length > forms.length
      }
    };
  }

  BG.normalizeGroupColor = normalizeGroupColor;
  BG.runGetContentTaskInPage = runGetContentTaskInPage;
  BG.runHighlightTaskInPage = runHighlightTaskInPage;
  BG.commandNavigate = commandNavigate;
  BG.commandOpenTab = commandOpenTab;
  BG.commandSwitchTab = commandSwitchTab;
  BG.commandFocusTab = commandFocusTab;
  BG.commandCloseTab = commandCloseTab;
  BG.commandGetTabs = commandGetTabs;
  BG.commandDescribeSessionTabs = commandDescribeSessionTabs;
  BG.commandGroupTabs = commandGroupTabs;
  BG.commandClick = commandClick;
  BG.commandType = commandType;
  BG.commandPressKey = commandPressKey;
  BG.commandScroll = commandScroll;
  BG.commandHighlight = commandHighlight;
  BG.commandGetContent = commandGetContent;
  BG.commandFindOne = commandFindOne;
  BG.commandFindElements = commandFindElements;
  BG.commandWaitFor = commandWaitFor;
  BG.commandGetElementState = commandGetElementState;
  BG.commandSelectOption = commandSelectOption;

  BG.executeBrokerCommand = async function executeBrokerCommand(method, args) {
    await BG.hostPolicyReady;
    const allowedHosts = BG.normalizeAllowedHosts(args.allowedHosts);
    switch (method) {
      case "navigate":
        return await commandNavigate(args, allowedHosts);
      case "open_tab":
        return await commandOpenTab(args, allowedHosts);
      case "switch_tab":
        return await commandSwitchTab(args, allowedHosts);
      case "focus_tab":
        return await commandFocusTab(args, allowedHosts);
      case "close_tab":
        return await commandCloseTab(args, allowedHosts);
      case "get_tabs":
        return await commandGetTabs(allowedHosts);
      case "describe_session_tabs":
        return await commandDescribeSessionTabs(allowedHosts);
      case "group_tabs":
        return await commandGroupTabs(args, allowedHosts);
      case "click":
        return await commandClick(args, allowedHosts);
      case "type":
        return await commandType(args, allowedHosts);
      case "press_key":
        return await commandPressKey(args, allowedHosts);
      case "scroll":
        return await commandScroll(args, allowedHosts);
      case "highlight":
        return await commandHighlight(args, allowedHosts);
      case "get_content":
        return await commandGetContent(args, allowedHosts);
      case "find_one":
        return await commandFindOne(args, allowedHosts);
      case "find_elements":
        return await commandFindElements(args, allowedHosts);
      case "wait_for":
        return await commandWaitFor(args, allowedHosts);
      case "get_element_state":
        return await commandGetElementState(args, allowedHosts);
      case "select_option":
        return await commandSelectOption(args, allowedHosts);
      default:
        throw new Error(`Unsupported command method: ${method}`);
    }
  };
})();
