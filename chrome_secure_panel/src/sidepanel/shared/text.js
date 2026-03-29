function compactInlineText(value, limit = 160) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (!text) {
    return "";
  }
  if (text.length <= limit) {
    return text;
  }
  return `${text.slice(0, Math.max(0, limit - 1)).trimEnd()}…`;
}

function normalizeComparableUrl(rawUrl) {
  try {
    const parsed = new URL(String(rawUrl || "").trim());
    if (!SAFE_LINK_PROTOCOLS.has(parsed.protocol)) {
      return "";
    }
    return `${parsed.origin}${parsed.pathname}${parsed.search}`;
  } catch {
    return "";
  }
}

function sameDocumentUrl(left, right) {
  const leftNormalized = normalizeComparableUrl(left);
  const rightNormalized = normalizeComparableUrl(right);
  return Boolean(leftNormalized) && leftNormalized === rightNormalized;
}

function normalizeLink(rawUrl) {
  try {
    const parsed = new URL(String(rawUrl || "").trim());
    if (!SAFE_LINK_PROTOCOLS.has(parsed.protocol)) {
      return "";
    }
    return parsed.toString();
  } catch {
    return "";
  }
}

function getMessageText(node) {
  return node?.dataset?.rawText || "";
}
