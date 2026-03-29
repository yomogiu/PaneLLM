function normalizeBrowserProfileStep(value) {
  if (!value || typeof value !== "object") {
    return null;
  }
  const raw = value;
  const url = normalizeLink(String(raw.url || raw.pageUrl || raw.page_url || "")) || normalizeComparableUrl(String(raw.url || "")) || "";
  if (!url) {
    return null;
  }
  return {
    id: String(raw.id || raw.step_id || "").trim() || crypto.randomUUID(),
    profileId: String(raw.profileId || raw.profile_id || "").trim(),
    title: compactInlineText(raw.title || raw.pageTitle || raw.page_title || String(raw.page || ""), 180),
    url,
    host: compactInlineText(raw.host || "", 120),
    attachedElement: compactInlineText(raw.attachedElement || raw.selector || raw.element || "", 220),
    summary: compactInlineText(raw.summary || "", 320),
    createdAt: String(raw.createdAt || raw.created_at || new Date().toISOString()).trim()
  };
}

function normalizeBrowserProfile(value) {
  if (!value || typeof value !== "object") {
    return null;
  }
  const id = String(value.id || value.profile_id || value.profileId || "").trim();
  const name = compactInlineText(value.name || value.label || value.title || "", 160);
  if (!id || !name) {
    return null;
  }
  const steps = Array.isArray(value.steps) ? value.steps.map(normalizeBrowserProfileStep).filter(Boolean) : [];
  return {
    id,
    name,
    createdAt: String(value.createdAt || value.created_at || new Date().toISOString()).trim(),
    updatedAt: String(value.updatedAt || value.updated_at || new Date().toISOString()).trim(),
    steps
  };
}

function cloneBrowserProfile(profile) {
  if (!profile || typeof profile !== "object") {
    return null;
  }
  return {
    ...profile,
    steps: (profile.steps || []).map((step) => ({ ...step }))
  };
}

function normalizeBrowserProfileAttachment(value) {
  if (!value || typeof value !== "object") {
    return null;
  }
  const profileId = String(value.profileId || value.profile_id || "").trim();
  if (!profileId) {
    return null;
  }
  return {
    profileId,
    stepId: String(value.stepId || value.step_id || "").trim()
  };
}

function makeBrowserProfileId() {
  return `profile_${crypto.randomUUID().replace(/-/g, "").slice(0, 12)}`;
}

function makeBrowserProfileStepId() {
  return `step_${crypto.randomUUID().replace(/-/g, "").slice(0, 12)}`;
}

function readChromeLocalStorage(keys) {
  return new Promise((resolve) => {
    if (!chrome?.storage?.local) {
      resolve({});
      return;
    }
    chrome.storage.local.get(keys, (stored) => {
      if (chrome.runtime?.lastError) {
        resolve({});
        return;
      }
      resolve(stored || {});
    });
  });
}

function removeChromeLocalStorage(keys) {
  return new Promise((resolve) => {
    if (!chrome?.storage?.local) {
      resolve(false);
      return;
    }
    chrome.storage.local.remove(keys, () => {
      resolve(!chrome.runtime?.lastError);
    });
  });
}

function normalizeStoredBrowserProfiles(value) {
  const rawProfiles = Array.isArray(value)
    ? value
    : Array.isArray(value?.profiles)
      ? value.profiles
      : [];
  return rawProfiles.map((profile) => normalizeBrowserProfile(profile)).filter(Boolean);
}

function normalizeStoredBrowserProfileMeta(value) {
  const raw = value && typeof value === "object" ? value : {};
  return {
    selectedProfileId: String(raw.selected_profile_id || raw.selectedProfileId || "").trim(),
    attachedProfile: normalizeBrowserProfileAttachment(raw.attached_profile || raw.attachedProfile)
  };
}

function normalizeStoredBrowserProfileState(value) {
  const raw = value && typeof value === "object" ? value : {};
  const meta = normalizeStoredBrowserProfileMeta(raw);
  return {
    profiles: normalizeStoredBrowserProfiles(raw),
    selectedProfileId: meta.selectedProfileId,
    attachedProfile: meta.attachedProfile
  };
}

function buildBrowserProfileStorePayload() {
  return {
    profiles: state.browserProfiles
      .map((profile) => cloneBrowserProfile(profile))
      .filter(Boolean),
    selected_profile_id: String(state.browserProfileSelectedId || "").trim(),
    attached_profile: state.browserProfileAttached
      ? {
          profile_id: String(state.browserProfileAttached.profileId || "").trim(),
          step_id: String(state.browserProfileAttached.stepId || "").trim()
        }
      : null
  };
}

function applyStoredBrowserProfileState(storedState) {
  const normalizedState = normalizeStoredBrowserProfileState(storedState);
  state.browserProfiles = normalizedState.profiles;
  state.browserProfileSelectedId = normalizedState.selectedProfileId || "";
  state.browserProfileAttached = normalizedState.attachedProfile;
  reconcileBrowserProfileState();
}

async function fetchPersistedBrowserProfileState() {
  const response = await sendRuntimeMessage({ type: "assistant.tools.browser_profiles.get" });
  return normalizeStoredBrowserProfileState(
    response?.browser_profiles || response?.browserProfiles || response
  );
}

async function savePersistedBrowserProfileState(payload) {
  const response = await sendRuntimeMessage({
    type: "assistant.tools.browser_profiles.update",
    browserProfiles: payload
  });
  return normalizeStoredBrowserProfileState(
    response?.browser_profiles || response?.browserProfiles || response
  );
}

async function readLegacyBrowserProfileState() {
  const stored = await readChromeLocalStorage([
    LEGACY_BROWSER_PROFILE_STORAGE_KEY,
    LEGACY_BROWSER_PROFILE_META_STORAGE_KEY
  ]);
  const storedProfiles = normalizeStoredBrowserProfiles(stored?.[LEGACY_BROWSER_PROFILE_STORAGE_KEY]);
  const fallbackMeta = normalizeStoredBrowserProfileMeta(stored?.[LEGACY_BROWSER_PROFILE_STORAGE_KEY]);
  const storedMeta = normalizeStoredBrowserProfileMeta(stored?.[LEGACY_BROWSER_PROFILE_META_STORAGE_KEY]);
  return {
    profiles: storedProfiles,
    selectedProfileId: storedMeta.selectedProfileId || fallbackMeta.selectedProfileId || "",
    attachedProfile: storedMeta.attachedProfile || fallbackMeta.attachedProfile
  };
}

async function clearLegacyBrowserProfileState() {
  await removeChromeLocalStorage([
    LEGACY_BROWSER_PROFILE_STORAGE_KEY,
    LEGACY_BROWSER_PROFILE_META_STORAGE_KEY
  ]);
}

function reconcileBrowserProfileState() {
  state.browserProfiles = Array.isArray(state.browserProfiles)
    ? state.browserProfiles.map((profile) => normalizeBrowserProfile(profile)).filter(Boolean)
    : [];

  if (!getBrowserProfileById(state.browserProfileSelectedId)) {
    state.browserProfileSelectedId = state.browserProfiles[0]?.id || "";
  }

  const attached = normalizeBrowserProfileAttachment(state.browserProfileAttached);
  if (!attached) {
    state.browserProfileAttached = null;
  } else {
    const attachedProfile = getBrowserProfileById(attached.profileId);
    if (!attachedProfile) {
      state.browserProfileAttached = null;
    } else {
      const attachedStep = getBrowserProfileStepById(attachedProfile, attached.stepId);
      state.browserProfileAttached = {
        profileId: attachedProfile.id,
        stepId: attachedStep ? attachedStep.id : ""
      };
    }
  }

  if (!getBrowserProfileById(state.browserProfileRecordingId)) {
    state.browserProfileRecordingId = "";
  }
}

function getBrowserProfileById(profileId) {
  const normalizedId = String(profileId || "").trim();
  if (!normalizedId) {
    return null;
  }
  return state.browserProfiles.find((profile) => String(profile.id) === normalizedId) || null;
}

function getBrowserProfileSelected() {
  return getBrowserProfileById(state.browserProfileSelectedId);
}

function getBrowserProfileStepById(profile, stepId) {
  if (!profile || !Array.isArray(profile.steps)) {
    return null;
  }
  const normalizedStepId = String(stepId || "").trim();
  if (!normalizedStepId) {
    return null;
  }
  return profile.steps.find((step) => String(step.id) === normalizedStepId) || null;
}

function getBrowserProfileStatusLines() {
  const selected = getBrowserProfileSelected();
  const attached = getAttachedBrowserProfileState();
  const lines = [];

  if (state.browserProfileError) {
    lines.push(`Profile error: ${state.browserProfileError}`);
  }

  if (state.browserProfiles.length === 0) {
    lines.push("No saved profiles. Save your first page step to begin.");
    return lines;
  }

  lines.push(`Selected profile: ${selected ? selected.name : "(none)"}`);

  if (state.browserProfileRecordingId) {
    const recording = getBrowserProfileById(state.browserProfileRecordingId);
    lines.push(`Recording: ${recording ? recording.name : "(stopped)"}`);
  }

  if (attached.profile) {
    const attachedStepLabel = attached.step ? `Step ${attached.stepIndex + 1}` : "No step";
    lines.push(`Attached for next run: ${attached.profile.name} · ${attachedStepLabel}`);
  }

  if (!lines.length) {
    lines.push("No profile attachment set.");
  }
  return lines;
}

function getAttachedBrowserProfileState() {
  const selectedProfile = getBrowserProfileById(state.browserProfileAttached?.profileId);
  if (!selectedProfile) {
    if (state.browserProfileAttached) {
      state.browserProfileAttached = null;
    }
    return { profile: null, step: null, stepIndex: -1 };
  }
  const stepId = String(state.browserProfileAttached?.stepId || "").trim();
  const stepIndex = Array.isArray(selectedProfile.steps)
    ? selectedProfile.steps.findIndex((step) => String(step.id) === stepId)
    : -1;
  const activeStep = stepIndex >= 0
    ? selectedProfile.steps[stepIndex]
    : selectedProfile.steps[selectedProfile.steps.length - 1] || null;
  return {
    profile: selectedProfile,
    step: activeStep,
    stepId: activeStep ? activeStep.id : "",
    stepIndex: activeStep ? (stepIndex >= 0 ? stepIndex : selectedProfile.steps.length - 1) : -1
  };
}

function formatBrowserProfileStepSummary(step) {
  if (!step) {
    return "Step data unavailable";
  }
  const title = compactInlineText(step.title || "Untitled page", 72);
  const host = compactInlineText(step.host || "", 70);
  const base = host ? `${title} (${host})` : title;
  if (!base.trim()) {
    return compactInlineText(step.url || "", 92);
  }
  if (step.attachedElement) {
    return `${base} · ${step.attachedElement}`;
  }
  return base;
}

function formatBrowserProfilePromptUrl(rawUrl, limit = 96) {
  try {
    const parsed = new URL(String(rawUrl || "").trim());
    if (!SAFE_LINK_PROTOCOLS.has(parsed.protocol)) {
      return "";
    }
    const segments = parsed.pathname
      .split("/")
      .map((segment) => String(segment || "").trim())
      .filter(Boolean);
    const topLevelPath = segments.length ? `/${segments[0]}` : "";
    return compactInlineText(`${parsed.host}${topLevelPath}`, limit);
  } catch {
    return "";
  }
}

function formatBrowserProfilePromptStepSummary(step) {
  if (!step) {
    return "Step data unavailable";
  }
  const title = compactInlineText(step.title || "Untitled page", 72);
  const coarseUrl = formatBrowserProfilePromptUrl(step.url, 92);
  const base = [title, coarseUrl ? `(${coarseUrl})` : ""]
    .filter((part) => String(part || "").trim())
    .join(" ")
    .trim();
  if (!base) {
    return compactInlineText(step.attachedElement || "Step data unavailable", 92);
  }
  if (step.attachedElement) {
    return `${base} · ${step.attachedElement}`;
  }
  return base;
}

function buildBrowserProfileContextPayload(attachedProfile = getAttachedBrowserProfileState()) {
  if (!attachedProfile.profile) {
    return null;
  }
  const profile = attachedProfile.profile;
  const attachedStep = attachedProfile.step;
  return {
    profile_id: profile.id,
    profile_name: profile.name,
    step_id: attachedStep ? attachedStep.id : "",
    attached_step_summary: attachedStep ? formatBrowserProfileStepSummary(attachedStep) : "",
    steps: (profile.steps || []).map((step) => ({
      id: step.id,
      title: step.title,
      host: step.host,
      url: step.url,
      summary: step.summary,
      attached_element: step.attachedElement,
      created_at: step.createdAt
    }))
  };
}

function buildProfilePromptSuffix(attachedProfile = getAttachedBrowserProfileState()) {
  if (!attachedProfile.profile) {
    return "";
  }

  const profile = attachedProfile.profile;
  const steps = Array.isArray(profile.steps) ? profile.steps : [];
  const stepLines = [];
  const maxSteps = Math.max(1, steps.length);
  if (!steps.length) {
    stepLines.push("- No recorded steps yet.");
  }
  for (let index = 0; index < steps.length; index += 1) {
    const step = steps[index];
    if (!step) {
      continue;
    }
    if (maxSteps >= 1) {
      stepLines.push(`${index + 1}. ${formatBrowserProfilePromptStepSummary(step)}`.trim());
    }
  }

  const attachedLabel = attachedProfile.step
    ? `Attached step ${attachedProfile.stepIndex + 1}: ${formatBrowserProfilePromptStepSummary(attachedProfile.step)}`
    : "No attached step.";

  return [
    `Browser workflow profile: ${profile.name} [${profile.id}]`,
    "Steps:",
    ...stepLines,
    attachedLabel
  ].join("\n");
}

function buildRequestPromptSuffix() {
  return [buildComposerPromptSuffix(), buildProfilePromptSuffix()]
    .filter((part) => String(part || "").trim())
    .join("\n\n")
    .trim();
}

function syncBrowserProfileNameInput(force = false) {
  if (!browserProfileNameEl) {
    return;
  }
  const selected = getBrowserProfileSelected();
  if (!selected) {
    if (force) {
      browserProfileNameEl.value = "";
    }
    return;
  }
  if (force || document.activeElement !== browserProfileNameEl || !String(browserProfileNameEl.value || "").trim()) {
    browserProfileNameEl.value = selected.name;
  }
}

function buildDefaultBrowserProfileName() {
  const draft = compactInlineText(browserProfileNameEl?.value || "", 160);
  if (draft) {
    return draft;
  }
  const pageTitle = compactInlineText(state.toolsActiveTab?.title || "", 120);
  if (pageTitle) {
    return pageTitle;
  }
  return `Profile ${state.browserProfiles.length + 1}`;
}

function createBrowserProfile(name = buildDefaultBrowserProfileName()) {
  const now = new Date().toISOString();
  return {
    id: makeBrowserProfileId(),
    name: compactInlineText(name, 160) || `Profile ${state.browserProfiles.length + 1}`,
    createdAt: now,
    updatedAt: now,
    steps: []
  };
}

function getCurrentBrowserProfileElementContext() {
  const attached = normalizeBrowserElementPayload(state.browserElementContextForNextMessage);
  if (attached && browserElementMatchesCurrentPage(attached)) {
    return attached;
  }
  const latest = normalizeBrowserElementPayload(state.browserPickerLatest);
  if (latest && browserElementMatchesCurrentPage(latest)) {
    return latest;
  }
  return null;
}

function buildBrowserProfileStepFromCurrentContext() {
  const activeTab = state.toolsActiveTab && typeof state.toolsActiveTab === "object" ? state.toolsActiveTab : null;
  const rawUrl = String(activeTab?.url || "").trim();
  const url = normalizeLink(rawUrl) || normalizeComparableUrl(rawUrl);
  if (!url) {
    return null;
  }
  const element = getCurrentBrowserProfileElementContext();
  const attachedElement = element ? describeBrowserElementTarget(element) : "";
  const title = compactInlineText(activeTab?.title || activeTab?.host || "Untitled page", 180);
  const host = compactInlineText(activeTab?.host || "", 120);
  const summary = attachedElement
    ? `Saved ${attachedElement} on ${title || host || url}.`
    : `Saved ${title || host || url}.`;
  return normalizeBrowserProfileStep({
    id: makeBrowserProfileStepId(),
    title,
    url,
    host,
    attachedElement,
    summary,
    createdAt: new Date().toISOString()
  });
}

function browserProfileStepsMatch(left, right) {
  const leftUrl = normalizeComparableUrl(left?.url || "");
  const rightUrl = normalizeComparableUrl(right?.url || "");
  if (!leftUrl || leftUrl !== rightUrl) {
    return false;
  }
  return String(left?.attachedElement || "").trim() === String(right?.attachedElement || "").trim();
}

function upsertBrowserProfileStep(profile, step) {
  const normalizedProfile = cloneBrowserProfile(profile);
  const normalizedStep = normalizeBrowserProfileStep(step);
  if (!normalizedProfile || !normalizedStep) {
    return { profile: normalizedProfile, step: null, changed: false };
  }

  const nextSteps = Array.isArray(normalizedProfile.steps) ? normalizedProfile.steps.map((entry) => ({ ...entry })) : [];
  const lastStep = nextSteps[nextSteps.length - 1] || null;
  let savedStep = normalizedStep;
  let changed = true;

  if (lastStep && browserProfileStepsMatch(lastStep, normalizedStep)) {
    savedStep = {
      ...lastStep,
      title: normalizedStep.title,
      host: normalizedStep.host,
      summary: normalizedStep.summary,
      attachedElement: normalizedStep.attachedElement
    };
    changed = JSON.stringify(lastStep) !== JSON.stringify(savedStep);
    nextSteps[nextSteps.length - 1] = savedStep;
  } else {
    nextSteps.push({
      ...normalizedStep,
      profileId: normalizedProfile.id
    });
    if (nextSteps.length > BROWSER_PROFILE_MAX_STEPS) {
      nextSteps.splice(0, nextSteps.length - BROWSER_PROFILE_MAX_STEPS);
    }
    savedStep = nextSteps[nextSteps.length - 1] || normalizedStep;
  }

  return {
    profile: {
      ...normalizedProfile,
      updatedAt: changed ? new Date().toISOString() : normalizedProfile.updatedAt,
      steps: nextSteps
    },
    step: savedStep,
    changed
  };
}

function saveBrowserProfileLocally(profile) {
  const normalized = normalizeBrowserProfile(profile);
  if (!normalized) {
    return null;
  }
  state.browserProfiles = [
    normalized,
    ...state.browserProfiles.filter((entry) => String(entry.id) !== String(normalized.id))
  ];
  state.browserProfileSelectedId = normalized.id;
  reconcileBrowserProfileState();
  return normalized;
}

async function persistBrowserProfileState() {
  reconcileBrowserProfileState();
  const storedState = await savePersistedBrowserProfileState(buildBrowserProfileStorePayload());
  applyStoredBrowserProfileState(storedState);
  try {
    await clearLegacyBrowserProfileState();
  } catch {}
}

async function persistBrowserProfileStateSafely() {
  try {
    await persistBrowserProfileState();
    state.browserProfileError = "";
  } catch (error) {
    state.browserProfileError = `Could not save profile state: ${String(error.message || error)}`;
  }
  renderBrowserProfilePanel();
}

async function restoreBrowserProfileState() {
  state.browserProfiles = [];
  state.browserProfileSelectedId = "";
  state.browserProfileAttached = null;
  state.browserProfileRecordingId = "";
  state.browserProfileError = "";
  syncBrowserProfileNameInput(true);

  try {
    applyStoredBrowserProfileState(await fetchPersistedBrowserProfileState());
  } catch (error) {
    state.browserProfileError = `Could not load saved profiles: ${String(error.message || error)}`;
  }

  const brokerHasProfiles = state.browserProfiles.length > 0;
  const legacyState = await readLegacyBrowserProfileState();
  const legacyHasProfiles = legacyState.profiles.length > 0;

  if (legacyHasProfiles && !brokerHasProfiles) {
    state.browserProfiles = legacyState.profiles;
    state.browserProfileSelectedId = legacyState.selectedProfileId || "";
    state.browserProfileAttached = legacyState.attachedProfile;
    reconcileBrowserProfileState();
    try {
      await persistBrowserProfileState();
      state.browserProfileError = "";
    } catch (error) {
      state.browserProfiles = [];
      state.browserProfileSelectedId = "";
      state.browserProfileAttached = null;
      reconcileBrowserProfileState();
      state.browserProfileError = `Could not move saved profiles into broker storage: ${String(error.message || error)}`;
    }
  } else if (legacyHasProfiles || legacyState.selectedProfileId || legacyState.attachedProfile) {
    try {
      await clearLegacyBrowserProfileState();
    } catch {}
  }

  renderBrowserProfilePanel({ forceNameSync: true });
}

function setSelectedBrowserProfile(profileId, { forceNameSync = false } = {}) {
  state.browserProfileSelectedId = String(profileId || "").trim();
  reconcileBrowserProfileState();
  renderBrowserProfilePanel({ forceNameSync });
  void persistBrowserProfileStateSafely();
}

function attachBrowserProfile(profileId, stepId = "") {
  const profile = getBrowserProfileById(profileId);
  if (!profile) {
    state.browserProfileAttached = null;
    renderBrowserProfilePanel();
    return;
  }
  const step = getBrowserProfileStepById(profile, stepId) || profile.steps[profile.steps.length - 1] || null;
  state.browserProfileAttached = {
    profileId: profile.id,
    stepId: step ? step.id : ""
  };
  renderBrowserProfilePanel();
  renderBrowserNextActionIndicator();
  void persistBrowserProfileStateSafely();
}

function clearAttachedBrowserProfile() {
  if (!state.browserProfileAttached) {
    return;
  }
  state.browserProfileAttached = null;
  renderBrowserProfilePanel();
  renderBrowserNextActionIndicator();
  void persistBrowserProfileStateSafely();
}

function ensureActiveBrowserProfile(name = buildDefaultBrowserProfileName()) {
  const selected = getBrowserProfileSelected();
  const draftName = compactInlineText(name, 160);
  if (selected && (!draftName || draftName === selected.name)) {
    return selected;
  }
  const created = saveBrowserProfileLocally(createBrowserProfile(draftName));
  renderBrowserProfilePanel({ forceNameSync: true });
  void persistBrowserProfileStateSafely();
  return created;
}

function recordBrowserProfileStep(profile, step) {
  const targetProfile = normalizeBrowserProfile(profile);
  const normalizedStep = normalizeBrowserProfileStep(step);
  if (!targetProfile || !normalizedStep) {
    return { profile: targetProfile, step: null, changed: false };
  }
  const next = upsertBrowserProfileStep(targetProfile, normalizedStep);
  if (!next.profile || !next.step) {
    return { profile: targetProfile, step: null, changed: false };
  }
  const savedProfile = next.changed
    ? saveBrowserProfileLocally(next.profile)
    : getBrowserProfileById(targetProfile.id) || targetProfile;
  const attachedProfileId = String(state.browserProfileAttached?.profileId || "").trim();
  const attachedStepId = String(state.browserProfileAttached?.stepId || "").trim();
  if (savedProfile && next.step && (attachedProfileId !== savedProfile.id || attachedStepId !== next.step.id)) {
    attachBrowserProfile(savedProfile.id, next.step.id);
  }
  return {
    profile: savedProfile,
    step: next.step,
    changed: next.changed
  };
}

async function maybeRecordActiveBrowserProfileStep() {
  const recordingProfile = getBrowserProfileById(state.browserProfileRecordingId);
  if (!recordingProfile) {
    return;
  }
  const step = buildBrowserProfileStepFromCurrentContext();
  if (!step) {
    return;
  }
  const result = recordBrowserProfileStep(recordingProfile, step);
  if (result.changed) {
    state.browserProfileError = "";
    renderBrowserProfilePanel();
    await persistBrowserProfileStateSafely();
  }
}

async function toggleBrowserProfileRecording() {
  if (state.browserProfileRecordingId) {
    state.browserProfileRecordingId = "";
    state.browserProfileError = "";
    renderBrowserProfilePanel({ forceNameSync: true });
    await persistBrowserProfileStateSafely();
    return;
  }

  const profile = ensureActiveBrowserProfile(buildDefaultBrowserProfileName());
  if (!profile) {
    state.browserProfileError = "Profile name is required to start recording.";
    renderBrowserProfilePanel();
    return;
  }
  state.browserProfileRecordingId = profile.id;
  state.browserProfileError = "";
  renderBrowserProfilePanel({ forceNameSync: true });
  await maybeRecordActiveBrowserProfileStep();
  await persistBrowserProfileStateSafely();
}

async function saveLatestBrowserProfileStep() {
  const step = buildBrowserProfileStepFromCurrentContext();
  if (!step) {
    state.browserProfileError = "Open a normal browser page before saving a profile step.";
    renderBrowserProfilePanel();
    return;
  }
  const targetProfile = state.browserProfileRecordingId
    ? getBrowserProfileById(state.browserProfileRecordingId)
    : ensureActiveBrowserProfile(buildDefaultBrowserProfileName());
  if (!targetProfile) {
    state.browserProfileError = "Create or select a profile before saving a step.";
    renderBrowserProfilePanel();
    return;
  }
  const result = recordBrowserProfileStep(targetProfile, step);
  if (!result.profile || !result.step) {
    state.browserProfileError = "Could not save the latest step.";
    renderBrowserProfilePanel();
    return;
  }
  state.browserProfileError = "";
  renderBrowserProfilePanel({ forceNameSync: true });
  await persistBrowserProfileStateSafely();
}

async function toggleSelectedBrowserProfileAttachment() {
  const selected = getBrowserProfileSelected();
  if (!selected) {
    state.browserProfileError = "Select a profile to attach it.";
    renderBrowserProfilePanel();
    return;
  }
  const attached = getAttachedBrowserProfileState();
  if (attached.profile && attached.profile.id === selected.id) {
    clearAttachedBrowserProfile();
    return;
  }
  state.browserProfileError = "";
  attachBrowserProfile(selected.id, attached.stepId || selected.steps[selected.steps.length - 1]?.id || "");
}

async function deleteSelectedBrowserProfile() {
  const selected = getBrowserProfileSelected();
  if (!selected) {
    state.browserProfileError = "Select a profile to delete it.";
    renderBrowserProfilePanel();
    return;
  }
  const accepted = await requestActionConfirm({
    title: "Delete Browser Profile?",
    text: `Delete "${selected.name}" and its recorded steps? This cannot be undone.`,
    confirmLabel: "Delete"
  });
  if (!accepted) {
    return;
  }
  state.browserProfiles = state.browserProfiles.filter((profile) => profile.id !== selected.id);
  if (state.browserProfileRecordingId === selected.id) {
    state.browserProfileRecordingId = "";
  }
  if (state.browserProfileAttached?.profileId === selected.id) {
    state.browserProfileAttached = null;
  }
  state.browserProfileSelectedId = state.browserProfiles[0]?.id || "";
  state.browserProfileError = "";
  renderBrowserProfilePanel({ forceNameSync: true });
  renderBrowserNextActionIndicator();
  await persistBrowserProfileStateSafely();
}

async function commitSelectedBrowserProfileName() {
  const selected = getBrowserProfileSelected();
  const nextName = compactInlineText(browserProfileNameEl?.value || "", 160);
  if (!selected) {
    renderBrowserProfilePanel();
    return;
  }
  if (!nextName || nextName === selected.name) {
    renderBrowserProfilePanel({ forceNameSync: true });
    return;
  }
  const savedProfile = saveBrowserProfileLocally({
    ...selected,
    name: nextName,
    updatedAt: new Date().toISOString()
  });
  if (!savedProfile) {
    return;
  }
  state.browserProfileError = "";
  renderBrowserProfilePanel({ forceNameSync: true });
  await persistBrowserProfileStateSafely();
}

function renderBrowserProfilePanel(options = {}) {
  reconcileBrowserProfileState();
  const forceNameSync = options.forceNameSync === true;
  const selected = getBrowserProfileSelected();
  const attached = getAttachedBrowserProfileState();
  const recordingProfile = getBrowserProfileById(state.browserProfileRecordingId);

  if (browserProfileStatusEl) {
    browserProfileStatusEl.textContent = getBrowserProfileStatusLines().join(" · ");
  }

  if (browserProfileSelectEl) {
    browserProfileSelectEl.textContent = "";
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = state.browserProfiles.length ? "Select a profile" : "No saved profiles";
    browserProfileSelectEl.appendChild(placeholder);
    for (const profile of state.browserProfiles) {
      const option = document.createElement("option");
      option.value = profile.id;
      option.textContent = profile.name;
      browserProfileSelectEl.appendChild(option);
    }
    browserProfileSelectEl.value = selected ? selected.id : "";
    browserProfileSelectEl.disabled = state.toolsBusy || Boolean(state.activeRunId) || state.browserProfiles.length === 0;
  }

  syncBrowserProfileNameInput(forceNameSync);

  if (browserProfileNameEl) {
    browserProfileNameEl.disabled = state.toolsBusy || Boolean(state.activeRunId);
  }

  if (browserProfileRecordBtn) {
    browserProfileRecordBtn.textContent = state.browserProfileRecordingId ? "Stop Recording" : "Record";
    browserProfileRecordBtn.classList.toggle("active", Boolean(state.browserProfileRecordingId));
    browserProfileRecordBtn.disabled = state.toolsBusy || Boolean(state.activeRunId);
  }

  if (browserProfileSaveLatestBtn) {
    browserProfileSaveLatestBtn.disabled = state.toolsBusy || Boolean(state.activeRunId) || !state.toolsActiveTab?.url;
  }

  if (browserProfileUseBtn) {
    const selectedIsAttached = Boolean(selected && attached.profile && attached.profile.id === selected.id);
    browserProfileUseBtn.textContent = selectedIsAttached ? "Detach Profile" : "Use Profile";
    browserProfileUseBtn.classList.toggle("active", selectedIsAttached);
    browserProfileUseBtn.disabled = state.toolsBusy || Boolean(state.activeRunId) || !selected;
  }

  if (browserProfileDeleteBtn) {
    browserProfileDeleteBtn.disabled = state.toolsBusy || Boolean(state.activeRunId) || !selected;
  }

  if (!browserProfileListEl) {
    return;
  }

  browserProfileListEl.textContent = "";
  if (!state.browserProfiles.length) {
    const empty = document.createElement("p");
    empty.className = "tools-empty";
    empty.textContent = "Create a profile, then record or save steps from the current page.";
    browserProfileListEl.appendChild(empty);
    return;
  }

  for (const profile of state.browserProfiles) {
    const interactionsLocked = state.toolsBusy || Boolean(state.activeRunId);
    const card = document.createElement("article");
    card.className = "browser-picker-card browser-profile-item";
    if (selected && profile.id === selected.id) {
      card.classList.add("is-selected");
    }
    if (attached.profile && attached.profile.id === profile.id) {
      card.classList.add("is-attached");
    }
    if (recordingProfile && recordingProfile.id === profile.id) {
      card.classList.add("is-recording");
    }

    card.tabIndex = interactionsLocked ? -1 : 0;
    card.setAttribute("aria-disabled", String(interactionsLocked));
    card.addEventListener("click", () => {
      if (interactionsLocked) {
        return;
      }
      setSelectedBrowserProfile(profile.id, { forceNameSync: true });
    });
    card.addEventListener("keydown", (event) => {
      if (interactionsLocked) {
        return;
      }
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        setSelectedBrowserProfile(profile.id, { forceNameSync: true });
      }
    });

    const title = document.createElement("p");
    title.className = "browser-picker-card-title";
    title.textContent = profile.name;
    card.appendChild(title);

    const chips = document.createElement("div");
    chips.className = "tools-item-tags";
    if (selected && profile.id === selected.id) {
      const chip = document.createElement("span");
      chip.className = "status-badge browser-profile-chip selected";
      chip.textContent = "Selected";
      chips.appendChild(chip);
    }
    if (attached.profile && attached.profile.id === profile.id) {
      const chip = document.createElement("span");
      chip.className = "status-badge browser-profile-chip attached";
      chip.textContent = attached.step ? `Attached · Step ${attached.stepIndex + 1}` : "Attached";
      chips.appendChild(chip);
    }
    if (recordingProfile && recordingProfile.id === profile.id) {
      const chip = document.createElement("span");
      chip.className = "status-badge browser-profile-chip recording";
      chip.textContent = "Recording";
      chips.appendChild(chip);
    }
    if (chips.childNodes.length > 0) {
      card.appendChild(chips);
    }

    const meta = document.createElement("p");
    meta.className = "browser-picker-card-meta";
    meta.textContent = `${profile.steps.length} step${profile.steps.length === 1 ? "" : "s"} · Updated ${formatTime(profile.updatedAt)}`;
    card.appendChild(meta);

    const stepList = document.createElement("div");
    stepList.className = "browser-profile-step-list";
    if (!profile.steps.length) {
      const line = document.createElement("p");
      line.className = "browser-profile-step-line error";
      line.textContent = "No steps recorded yet.";
      stepList.appendChild(line);
    } else {
      const previewSteps = profile.steps.slice(-BROWSER_PROFILE_STEP_PREVIEW_LIMIT);
      const previewOffset = profile.steps.length - previewSteps.length;
      for (let index = 0; index < previewSteps.length; index += 1) {
        const step = previewSteps[index];
        const line = document.createElement("p");
        line.className = "browser-profile-step-line";
        const label = document.createElement("span");
        label.textContent = `${previewOffset + index + 1}. ${formatBrowserProfileStepSummary(step)}`;
        line.appendChild(label);
        if (attached.step && attached.step.id === step.id) {
          const chip = document.createElement("em");
          chip.textContent = " Attached";
          line.appendChild(chip);
        }
        stepList.appendChild(line);
      }
    }
    card.appendChild(stepList);
    browserProfileListEl.appendChild(card);
  }
}

