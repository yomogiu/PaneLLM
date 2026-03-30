window.PLLM_SP = window.PLLM_SP || {};
const sidepanelNamespace = window.PLLM_SP;

const $ = (id) => document.getElementById(id);
const SAFE_LINK_PROTOCOLS = new Set(["http:", "https:"]);
const EXPLAIN_SELECTION_DEFAULT_PROMPT = "Explain the selected passage in plain language.";
const HIGHLIGHTS_TAB_GLOW_MS = 1_600;
const FONT_SCALE_STORAGE_KEY = "ui_font_scale";
const THEME_STORAGE_KEY = "ui_theme";
const DEFAULT_FONT_SCALE = 1;
const MIN_FONT_SCALE = 0.9;
const MAX_FONT_SCALE = 1.3;
const FONT_SCALE_STEP = 0.1;
const DEFAULT_THEME = "dark";
const AVAILABLE_THEMES = Object.freeze(["vaporwave", "light", "dark"]);
const GUIDE_PAGE_CONTEXT_FOLLOWUP = [
  "Treat the current page as the subject of the user's request.",
  "Interpret references like 'it', 'this', and 'the paper' as the current page unless the user explicitly says otherwise.",
  "Answer using only the captured page context. If the context is insufficient, say so instead of switching to repository or prior-chat context."
].join("\n");
const SHOW_ME_WHERE_FOLLOWUP =
  "After answering, use browser tools to scroll to and temporarily highlight the section of the current page that best answers the request above.";
const LEGACY_BROWSER_PROFILE_STORAGE_KEY = "browser_profiles";
const LEGACY_BROWSER_PROFILE_META_STORAGE_KEY = "browser_profile_meta";
const BROWSER_PROFILE_STEP_PREVIEW_LIMIT = 3;
const BROWSER_PROFILE_MAX_STEPS = 24;

function normalizeThemeId(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return AVAILABLE_THEMES.includes(normalized) ? normalized : DEFAULT_THEME;
}

const state = {
  sessionId: crypto.randomUUID(),
  pendingConfirmation: false,
  pendingRequest: null,
  conversationList: [],
  historyOpen: false,
  historyPinned: false,
  pinnedConversationIds: [],
  brokerHealth: null,
  availableBackends: [],
  busy: false,
  stopping: false,
  activeRunId: "",
  actionConfirmResolver: null,
  runUi: new Map(),
  pollingRuns: new Set(),
  highlightAutosaveRunIds: new Set(),
  highlightsTabGlowTimer: 0,
  rewriteTargetIndex: null,
  composerExplainSelection: "",
  composerShowMeWhere: false,
  composerGuidePageContext: false,
  activeMainTab: "chat",
  activePaperTab: "chat",
  fontScale: DEFAULT_FONT_SCALE,
  theme: DEFAULT_THEME,
  currentConversationPaper: null,
  activeBrowserPaper: null,
  pendingPaperConversationRestoreKey: "",
  restoringPaperConversation: false,
  paperState: null,
  paperMemoryResults: [],
  paperMemoryQuery: "",
  paperMemoryVersion: "",
  paperMemoryLoading: false,
  paperMemoryError: "",
  paperMemoryRequestKey: "",
  browserActionArmed: false,
  browserActionArmedReason: "",
  browserPickerActive: false,
  browserPickerRequestId: "",
  browserPickerLatest: null,
  browserPickerHistory: [],
  browserPickerError: "",
  browserElementContextForNextMessage: null,
  browserProfiles: [],
  browserProfileSelectedId: "",
  browserProfileAttached: null,
  browserProfileRecordingId: "",
  browserProfileError: "",
  browserAutomationStatus: {
    label: "Idle",
    text: "Browser actions are idle.",
    detail: ""
  },
  browserAutomationLastToolSummary: "",
  browserAutomationLastToolResult: "",
  browserAutomationLastToolError: "",
  browserPendingApprovals: new Map(),
  // TODO: deprecate/remove after Browser tab migration. These names still back the Browser workspace
  // because they also drive read-assistant and paper-context behavior.
  toolsBusy: false,
  toolsPolicy: null,
  toolsActiveTab: null,
  toolsBrowserConfig: null,
  readContext: null,
  pollTimers: {
    activeTabRefresh: 0
  }
};


sidepanelNamespace.state = state;
sidepanelNamespace.getElementById = $;
