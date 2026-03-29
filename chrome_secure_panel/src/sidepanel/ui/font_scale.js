function clampFontScale(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return DEFAULT_FONT_SCALE;
  }
  const rounded = Math.round(numeric * 100) / 100;
  return Math.min(MAX_FONT_SCALE, Math.max(MIN_FONT_SCALE, rounded));
}

function applyFontScale(scale) {
  const next = clampFontScale(scale);
  state.fontScale = next;
  document.documentElement.style.setProperty("--font-scale", String(next));
  if (fontScaleDownBtn) {
    fontScaleDownBtn.disabled = next <= MIN_FONT_SCALE;
  }
  if (fontScaleUpBtn) {
    fontScaleUpBtn.disabled = next >= MAX_FONT_SCALE;
  }
}

function persistFontScalePreference(scale) {
  if (!chrome?.storage?.local) {
    return;
  }
  chrome.storage.local.set({ [FONT_SCALE_STORAGE_KEY]: clampFontScale(scale) });
}

function restoreFontScalePreference() {
  applyFontScale(DEFAULT_FONT_SCALE);
  if (!chrome?.storage?.local) {
    return;
  }
  chrome.storage.local.get([FONT_SCALE_STORAGE_KEY], (stored) => {
    if (chrome.runtime?.lastError) {
      return;
    }
    applyFontScale(stored?.[FONT_SCALE_STORAGE_KEY]);
  });
}

function adjustFontScale(direction) {
  const delta = direction > 0 ? FONT_SCALE_STEP : -FONT_SCALE_STEP;
  const next = clampFontScale(state.fontScale + delta);
  if (next === state.fontScale) {
    return;
  }
  applyFontScale(next);
  persistFontScalePreference(next);
}

function initializeFontScaleControls() {
  fontScaleDownBtn?.addEventListener("click", () => {
    adjustFontScale(-1);
  });

  fontScaleUpBtn?.addEventListener("click", () => {
    adjustFontScale(1);
  });

  restoreFontScalePreference();
}
