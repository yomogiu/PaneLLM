function renderConversationMessages(messages) {
  clearMessages();
  const normalizedMessages = Array.isArray(messages) ? messages : [];
  normalizedMessages.forEach((message, index) => {
    if (message.role === "user" || message.role === "assistant") {
      appendMessage(
        message.role,
        String(message.content || ""),
        false,
        "",
        index,
        message.reasoning_blocks || message.reasoningBlocks
      );
    }
  });
  if (!normalizedMessages.length) {
    appendMessage("system", "Conversation is empty.");
  }
  syncRewriteTargetHighlight();
}

function formatTime(raw) {
  if (!raw) {
    return "unknown";
  }
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) {
    return "unknown";
  }
  return date.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit"
  });
}

function formatTraceTime(raw) {
  if (!raw) {
    return "";
  }
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return date.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit"
  });
}

function appendMessage(
  role,
  text,
  pending = false,
  extraClass = "",
  messageIndex = null,
  reasoningBlocks = null
) {
  if (emptyStateEl) {
    emptyStateEl.classList.add("hidden");
  }
  const item = document.createElement("div");
  const body = document.createElement("div");
  body.className = "message-body";
  item.appendChild(body);
  if (Number.isInteger(messageIndex)) {
    item.dataset.messageIndex = String(messageIndex);
  }
  updateMessage(item, role, text, pending, extraClass, reasoningBlocks);
  if (role === "user" && Number.isInteger(messageIndex) && !pending) {
    attachUserMessageActions(item, messageIndex);
  }
  messagesEl.appendChild(item);
  messagesEl.scrollTop = messagesEl.scrollHeight;
  return item;
}

function collapseThinkBlocks(text) {
  const raw = String(text || "");
  if (!raw) {
    return { visible: "", hiddenChars: 0, reasoningBlocks: [] };
  }

  let source = raw;
  let visible = "";
  let hiddenChars = 0;
  const reasoningBlocks = [];
  const openTagPattern = /<(?:think|thinking)\b[^>]*>/i;
  const closeTagPattern = /<\/(?:think|thinking)\b[^>]*>/i;

  while (source.length > 0) {
    const openMatch = openTagPattern.exec(source);
    if (!openMatch) {
      visible += source;
      break;
    }

    const openStart = openMatch.index;
    visible += source.slice(0, openStart);
    const openEnd = openStart + openMatch[0].length;
    if (openEnd <= openStart) {
      break;
    }

    const remaining = source.slice(openEnd);
    const closeMatch = closeTagPattern.exec(remaining);
    if (!closeMatch) {
      // If a reasoning block is not closed, keep it hidden instead of rendering it.
      hiddenChars += Math.max(0, source.length - openStart);
      source = source.slice(0, openStart);
      break;
    }

    const closeStart = openEnd + closeMatch.index;
    const closeEnd = closeStart + closeMatch[0].length;
    const reasoningText = source.slice(openEnd, closeStart).trim();
    if (reasoningText) {
      reasoningBlocks.push(reasoningText);
    }
    hiddenChars += Math.max(0, closeStart - openEnd);
    source = source.slice(closeEnd);
  }

  visible = visible.replace(/<\/(?:think|thinking)\b[^>]*>/gi, "");
  visible = visible.replace(/\n{3,}/g, "\n\n").trim();
  return { visible, hiddenChars, reasoningBlocks };
}

function createReasoningDisclosure(reasoning, existing = null) {
  const normalized = normalizeReasoningPayload(reasoning);
  const blocks = normalized.blocks;
  const text = normalized.pending ? normalized.text : normalized.text || blocks.join("\n\n");
  if (!text.trim() && !blocks.length) {
    return null;
  }

  const details = document.createElement("details");
  details.className = `reasoning-disclosure${normalized.pending ? " pending" : ""}`;
  details.open = existing ? Boolean(existing.open) : normalized.pending;

  const summary = document.createElement("summary");
  if (normalized.pending) {
    summary.textContent = "Thinking...";
  } else {
    const totalChars = blocks.reduce((sum, block) => sum + block.length, 0);
    summary.textContent = `Reasoning (${blocks.length} block${blocks.length === 1 ? "" : "s"}, ${totalChars} chars)`;
  }
  details.appendChild(summary);

  const content = document.createElement("div");
  content.className = "reasoning-content";
  const combined = normalized.pending
    ? text
    : blocks
      .map((block, index) => (blocks.length > 1 ? `Block ${index + 1}\n\n${block}` : block))
      .join("\n\n");
  content.appendChild(renderMarkdownFragment(combined));
  details.appendChild(content);
  return details;
}

function attachUserMessageActions(node, messageIndex) {
  const actions = document.createElement("div");
  actions.className = "message-actions";

  const button = document.createElement("button");
  button.type = "button";
  button.className = "ghost message-edit-btn";
  button.setAttribute("aria-label", "Edit prompt");
  button.title = "Edit prompt";
  button.textContent = "✎";
  button.addEventListener("click", () => {
    startRewriteFromMessage(messageIndex, getMessageText(node));
  });

  actions.appendChild(button);
  node.appendChild(actions);
}

function updateMessage(node, role, text, pending = false, extraClass = "", reasoningBlocks = null) {
  node.className = `message ${role}${pending ? " pending" : ""}${extraClass ? ` ${extraClass}` : ""}`;
  const rawText = String(text || "");
  node.dataset.rawText = rawText;
  const body = node.querySelector(".message-body") || document.createElement("div");
  if (!body.parentNode) {
    body.className = "message-body";
    node.appendChild(body);
  }
  let displayText = rawText;
  let reasoningDisclosure = null;
  let reasoningState = null;
  if (role === "assistant") {
    const existingReasoningDisclosure = body.querySelector("details.reasoning-disclosure");
    reasoningState = normalizeReasoningPayload(reasoningBlocks);
    if (reasoningState.text.trim() || reasoningState.blocks.length) {
      displayText = rawText;
      reasoningDisclosure = createReasoningDisclosure(reasoningState, existingReasoningDisclosure);
    } else {
      const collapsed = collapseThinkBlocks(rawText);
      displayText = collapsed.visible;
      reasoningDisclosure = createReasoningDisclosure(collapsed.reasoningBlocks, existingReasoningDisclosure);
    }
  }
  body.textContent = "";
  if (displayText.trim()) {
    body.appendChild(renderMarkdownFragment(displayText));
  } else if (role === "assistant" && reasoningDisclosure) {
    const note = document.createElement("p");
    note.className = "reasoning-note";
    note.textContent = reasoningState?.pending
      ? "No final answer yet. Thinking below."
      : "No final answer text. Expand reasoning below.";
    body.appendChild(note);
  }
  if (role === "assistant" && reasoningDisclosure) {
    body.appendChild(reasoningDisclosure);
  }
  messagesEl.scrollTop = messagesEl.scrollHeight;
}

function clearMessages() {
  messagesEl.textContent = "";
  if (emptyStateEl) {
    emptyStateEl.classList.remove("hidden");
    messagesEl.appendChild(emptyStateEl);
  }
}

