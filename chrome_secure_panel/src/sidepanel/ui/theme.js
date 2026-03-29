function applyTheme(theme) {
  const next = normalizeThemeId(theme);
  state.theme = next;
  document.documentElement.dataset.theme = next;
  if (themeSelectEl) {
    themeSelectEl.value = next;
  }
}

function persistThemePreference(theme) {
  if (!chrome?.storage?.local) {
    return;
  }
  chrome.storage.local.set({ [THEME_STORAGE_KEY]: normalizeThemeId(theme) });
}

function restoreThemePreference() {
  applyTheme(DEFAULT_THEME);
  if (!chrome?.storage?.local) {
    return;
  }
  chrome.storage.local.get([THEME_STORAGE_KEY], (stored) => {
    if (chrome.runtime?.lastError) {
      return;
    }
    applyTheme(stored?.[THEME_STORAGE_KEY]);
  });
}

function initializeThemeControls() {
  themeSelectEl?.addEventListener("change", () => {
    const next = normalizeThemeId(themeSelectEl.value);
    applyTheme(next);
    persistThemePreference(next);
  });

  restoreThemePreference();
}
