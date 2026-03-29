(() => {
  const BG = self.PLLM_BG = self.PLLM_BG || {};

  BG.HOST_POLICY_STORAGE_KEY = "assistant.pageHostPolicy.v1";
  BG.MAX_POLICY_HOSTS = 256;
  BG.DEFAULT_ALLOWED_PAGE_HOSTS = Object.freeze([
    "127.0.0.1",
    "localhost",
    "google.com",
    "www.google.com",
    "arxiv.org",
    "www.arxiv.org"
  ]);

  BG.HIGH_RISK_PATTERN =
    /\b(delete|transfer|wire|bank|purchase|buy|checkout|submit|password|token|credential|2fa|otp|security code)\b/i;

  BG.hostPolicy = {
    allowedHosts: []
  };
  BG.hostPolicyReady = initializeHostPolicy();

  function normalizeHost(rawValue) {
    const input = String(rawValue || "").trim().toLowerCase();
    if (!input) {
      return "";
    }

    const wildcardTrimmed = input.replace(/^\*\./, "");
    let candidate = wildcardTrimmed;
    try {
      const parsed = wildcardTrimmed.includes("://")
        ? new URL(wildcardTrimmed)
        : new URL(`https://${wildcardTrimmed}`);
      candidate = String(parsed.hostname || "").trim().toLowerCase();
    } catch {
      candidate = wildcardTrimmed;
    }

    candidate = candidate
      .split("/")[0]
      .split("?")[0]
      .split("#")[0]
      .trim()
      .toLowerCase();

    if (candidate.includes(":")) {
      const colonIndex = candidate.lastIndexOf(":");
      const maybePort = candidate.slice(colonIndex + 1);
      if (/^\d+$/.test(maybePort)) {
        candidate = candidate.slice(0, colonIndex);
      }
    }

    candidate = candidate.replace(/^\.+|\.+$/g, "");
    if (!candidate || candidate.length > 253 || !/^[a-z0-9.-]+$/.test(candidate)) {
      return "";
    }
    return candidate;
  }

  function normalizeHostList(values, maxEntries = BG.MAX_POLICY_HOSTS) {
    const parts = Array.isArray(values) ? values : [];
    const deduped = [];
    for (const part of parts) {
      const host = normalizeHost(part);
      if (!host || deduped.includes(host)) {
        continue;
      }
      deduped.push(host);
      if (deduped.length >= maxEntries) {
        break;
      }
    }
    return deduped;
  }

  function normalizeAllowedHosts(allowedHosts = null) {
    const source =
      allowedHosts === null
        ? [...BG.DEFAULT_ALLOWED_PAGE_HOSTS, ...BG.hostPolicy.allowedHosts]
        : allowedHosts;
    return normalizeHostList(source);
  }

  function hostPermissionOrigins(host) {
    const normalized = normalizeHost(host);
    if (!normalized) {
      return [];
    }
    const origins = new Set([
      `http://${normalized}/*`,
      `https://${normalized}/*`
    ]);
    if (!normalized.startsWith("*.")) {
      origins.add(`http://*.${normalized}/*`);
      origins.add(`https://*.${normalized}/*`);
    }
    return [...origins];
  }

  async function ensureHostPermission(rawHost) {
    const host = normalizeHost(rawHost);
    if (!host) {
      return;
    }

    if (!chrome.permissions?.contains || !chrome.permissions?.request) {
      return;
    }

    const origins = hostPermissionOrigins(host);
    if (origins.length === 0) {
      return;
    }

    let allowed = false;
    try {
      allowed = await chrome.permissions.contains({ origins });
    } catch (error) {
      console.warn(`[secure-panel] permission contains check failed for ${host}:`, String(error?.message || error));
      return;
    }
    if (allowed) {
      return;
    }

    try {
      const granted = await chrome.permissions.request({ origins });
      if (granted) {
        return;
      }
    } catch (error) {
      console.warn(`[secure-panel] permission request failed for ${host}:`, String(error?.message || error));
    }

    const error = new Error(
      `Extension host permission for ${host} was not granted. Allow page access when Chrome prompts.`
    );
    error.code = "host_permission_not_granted";
    throw error;
  }

  async function ensureUrlHostPermission(rawUrl) {
    if (typeof rawUrl !== "string") {
      return;
    }
    try {
      const parsed = new URL(rawUrl);
      if (!urlProtocolAllowed(parsed)) {
        return;
      }
      await ensureHostPermission(parsed.hostname);
    } catch {
      return;
    }
  }

  function urlProtocolAllowed(parsedUrl) {
    return ["http:", "https:"].includes(String(parsedUrl?.protocol || ""));
  }

  function extractUrlHost(rawUrl) {
    try {
      const parsed = new URL(String(rawUrl || ""));
      return normalizeHost(parsed.hostname || "");
    } catch {
      return "";
    }
  }

  function hostMatchesAllowedList(host, hosts) {
    return hosts.some((allowed) => host === allowed || host.endsWith(`.${allowed}`));
  }

  function isHostAllowed(rawUrl, allowedHosts = null) {
    const host = extractUrlHost(rawUrl);
    if (!host) {
      return false;
    }
    return hostMatchesAllowedList(host, normalizeAllowedHosts(allowedHosts));
  }

  function createHostNotAllowlistedError(rawUrl, message, allowedHosts = null) {
    const error = new Error(message || "Host is not in the extension allowlist.");
    error.code = "host_not_allowlisted";
    error.data = {
      host: extractUrlHost(rawUrl),
      url: typeof rawUrl === "string" ? rawUrl : "",
      effective_allowed_hosts: normalizeAllowedHosts(allowedHosts)
    };
    return error;
  }

  async function initializeHostPolicy() {
    let needsMigration = false;
    try {
      const stored = await chrome.storage.local.get(BG.HOST_POLICY_STORAGE_KEY);
      const raw = stored?.[BG.HOST_POLICY_STORAGE_KEY];
      if (raw && typeof raw === "object") {
        BG.hostPolicy.allowedHosts = normalizeHostList(raw.allowed_hosts ?? raw.allowedHosts ?? []);
        needsMigration =
          Object.prototype.hasOwnProperty.call(raw, "blocked_hosts")
          || Object.prototype.hasOwnProperty.call(raw, "blockedHosts");
      }
    } catch (error) {
      console.warn("[secure-panel] failed to load host policy:", String(error?.message || error));
      return;
    }
    if (needsMigration) {
      await persistHostPolicy();
    }
  }

  async function persistHostPolicy() {
    try {
      await chrome.storage.local.set({
        [BG.HOST_POLICY_STORAGE_KEY]: {
          allowed_hosts: [...BG.hostPolicy.allowedHosts]
        }
      });
    } catch (error) {
      console.warn("[secure-panel] failed to persist host policy:", String(error?.message || error));
    }
  }

  function getHostPolicySnapshot() {
    const defaultHosts = normalizeHostList([...BG.DEFAULT_ALLOWED_PAGE_HOSTS]);
    const customAllowedHosts = normalizeHostList(BG.hostPolicy.allowedHosts);
    const effectiveAllowedHosts = normalizeAllowedHosts([...defaultHosts, ...customAllowedHosts]);
    return {
      default_hosts: defaultHosts,
      custom_allowed_hosts: customAllowedHosts,
      effective_allowed_hosts: effectiveAllowedHosts
    };
  }

  function parseHostForPolicy(rawHost) {
    const host = normalizeHost(rawHost);
    if (host) {
      return host;
    }
    const error = new Error("host is required and must be a valid hostname.");
    error.code = "invalid_host";
    throw error;
  }

  async function updateHostPolicyState(nextAllowedHosts) {
    BG.hostPolicy.allowedHosts = normalizeHostList(nextAllowedHosts);
    await persistHostPolicy();
    return getHostPolicySnapshot();
  }

  async function allowHost(rawHost) {
    await BG.hostPolicyReady;
    const host = parseHostForPolicy(rawHost);
    const defaultHosts = new Set(normalizeHostList([...BG.DEFAULT_ALLOWED_PAGE_HOSTS]));
    const nextAllowed = BG.hostPolicy.allowedHosts.filter((value) => value !== host);
    if (!defaultHosts.has(host)) {
      nextAllowed.push(host);
    }
    return await updateHostPolicyState(nextAllowed);
  }

  async function removeAllowedHost(rawHost) {
    await BG.hostPolicyReady;
    const host = parseHostForPolicy(rawHost);
    const nextAllowed = BG.hostPolicy.allowedHosts.filter((value) => value !== host);
    return await updateHostPolicyState(nextAllowed);
  }

  BG.normalizeHost = normalizeHost;
  BG.normalizeHostList = normalizeHostList;
  BG.normalizeAllowedHosts = normalizeAllowedHosts;
  BG.hostPermissionOrigins = hostPermissionOrigins;
  BG.ensureHostPermission = ensureHostPermission;
  BG.ensureUrlHostPermission = ensureUrlHostPermission;
  BG.urlProtocolAllowed = urlProtocolAllowed;
  BG.extractUrlHost = extractUrlHost;
  BG.hostMatchesAllowedList = hostMatchesAllowedList;
  BG.isHostAllowed = isHostAllowed;
  BG.createHostNotAllowlistedError = createHostNotAllowlistedError;
  BG.persistHostPolicy = persistHostPolicy;
  BG.getHostPolicySnapshot = getHostPolicySnapshot;
  BG.parseHostForPolicy = parseHostForPolicy;
  BG.updateHostPolicyState = updateHostPolicyState;
  BG.allowHost = allowHost;
  BG.removeAllowedHost = removeAllowedHost;
})();
