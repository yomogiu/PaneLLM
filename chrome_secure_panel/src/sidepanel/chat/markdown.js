function renderMarkdownFragment(source) {
  const text = String(source || "").replace(/\r\n?/g, "\n");
  const lines = text.split("\n");
  const fragment = document.createDocumentFragment();
  let index = 0;

  while (index < lines.length) {
    const line = lines[index];

    if (!line.trim()) {
      index += 1;
      continue;
    }

    const fenceMatch = line.match(/^```([\w-]+)?\s*$/);
    if (fenceMatch) {
      index += 1;
      const codeLines = [];
      while (index < lines.length && !/^```\s*$/.test(lines[index])) {
        codeLines.push(lines[index]);
        index += 1;
      }
      if (index < lines.length) {
        index += 1;
      }
      const pre = document.createElement("pre");
      const code = document.createElement("code");
      if (fenceMatch[1]) {
        code.dataset.lang = fenceMatch[1];
      }
      code.textContent = codeLines.join("\n");
      pre.appendChild(code);
      fragment.appendChild(pre);
      continue;
    }

    if (/^\s*>\s?/.test(line)) {
      const quoteLines = [];
      while (index < lines.length && /^\s*>\s?/.test(lines[index])) {
        quoteLines.push(lines[index].replace(/^\s*>\s?/, ""));
        index += 1;
      }
      const blockquote = document.createElement("blockquote");
      appendParagraphBlocks(blockquote, quoteLines.join("\n"));
      fragment.appendChild(blockquote);
      continue;
    }

    if (/^\s*[-*]\s+/.test(line)) {
      const list = document.createElement("ul");
      while (index < lines.length && /^\s*[-*]\s+/.test(lines[index])) {
        const item = document.createElement("li");
        appendInlineContent(item, lines[index].replace(/^\s*[-*]\s+/, ""));
        list.appendChild(item);
        index += 1;
      }
      fragment.appendChild(list);
      continue;
    }

    if (/^\s*\d+\.\s+/.test(line)) {
      const list = document.createElement("ol");
      while (index < lines.length && /^\s*\d+\.\s+/.test(lines[index])) {
        const item = document.createElement("li");
        appendInlineContent(item, lines[index].replace(/^\s*\d+\.\s+/, ""));
        list.appendChild(item);
        index += 1;
      }
      fragment.appendChild(list);
      continue;
    }

    const paragraphLines = [];
    while (
      index < lines.length &&
      lines[index].trim() &&
      !/^```/.test(lines[index]) &&
      !/^\s*>\s?/.test(lines[index]) &&
      !/^\s*[-*]\s+/.test(lines[index]) &&
      !/^\s*\d+\.\s+/.test(lines[index])
    ) {
      paragraphLines.push(lines[index]);
      index += 1;
    }
    appendParagraphBlocks(fragment, paragraphLines.join("\n"));
  }

  if (!fragment.childNodes.length) {
    fragment.appendChild(document.createTextNode(""));
  }
  return fragment;
}

function appendParagraphBlocks(container, text) {
  const chunks = text.split(/\n{2,}/).filter((chunk) => chunk.trim());
  for (const chunk of chunks) {
    const paragraph = document.createElement("p");
    appendInlineContent(paragraph, chunk);
    container.appendChild(paragraph);
  }
}

function appendInlineContent(container, text, depth = 0) {
  if (!text) {
    return;
  }
  if (depth > 8) {
    appendTextWithBreaks(container, text);
    return;
  }

  const nextToken = findNextInlineToken(text);
  if (!nextToken) {
    appendTextWithBreaks(container, text);
    return;
  }

  if (nextToken.index > 0) {
    appendTextWithBreaks(container, text.slice(0, nextToken.index));
  }

  const matchedText = text.slice(nextToken.index, nextToken.index + nextToken.length);
  switch (nextToken.type) {
    case "code": {
      const code = document.createElement("code");
      code.textContent = nextToken.content;
      container.appendChild(code);
      break;
    }
    case "link": {
      const href = normalizeLink(nextToken.url);
      if (href) {
        const link = document.createElement("a");
        link.href = href;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        appendInlineContent(link, nextToken.label, depth + 1);
        container.appendChild(link);
      } else {
        appendTextWithBreaks(container, matchedText);
      }
      break;
    }
    case "strong": {
      const strong = document.createElement("strong");
      appendInlineContent(strong, nextToken.content, depth + 1);
      container.appendChild(strong);
      break;
    }
    case "em": {
      const em = document.createElement("em");
      appendInlineContent(em, nextToken.content, depth + 1);
      container.appendChild(em);
      break;
    }
    case "url": {
      const href = normalizeLink(nextToken.url);
      if (href) {
        const link = document.createElement("a");
        link.href = href;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.textContent = nextToken.url;
        container.appendChild(link);
      } else {
        appendTextWithBreaks(container, matchedText);
      }
      break;
    }
    default:
      appendTextWithBreaks(container, matchedText);
      break;
  }

  const remaining = text.slice(nextToken.index + nextToken.length);
  if (remaining) {
    appendInlineContent(container, remaining, depth + 1);
  }
}

function appendTextWithBreaks(container, text) {
  const parts = String(text || "").split("\n");
  parts.forEach((part, index) => {
    if (index > 0) {
      container.appendChild(document.createElement("br"));
    }
    if (part) {
      container.appendChild(document.createTextNode(part));
    }
  });
}

function findNextInlineToken(text) {
  const patterns = [
    {
      type: "code",
      regex: /`([^`\n]+)`/g,
      build: (match) => ({ content: match[1] })
    },
    {
      type: "link",
      regex: /\[([^\]\n]+)\]\((https?:\/\/[^\s)]+)\)/g,
      build: (match) => ({ label: match[1], url: match[2] })
    },
    {
      type: "strong",
      regex: /\*\*([^*\n][\s\S]*?)\*\*/g,
      build: (match) => ({ content: match[1] })
    },
    {
      type: "em",
      regex: /\*([^*\n]+)\*/g,
      build: (match) => ({ content: match[1] })
    },
    {
      type: "url",
      regex: /https?:\/\/[^\s<]+[^\s<.,:;"')\]]/g,
      build: (match) => ({ url: match[0] })
    }
  ];

  let next = null;
  for (const pattern of patterns) {
    pattern.regex.lastIndex = 0;
    const match = pattern.regex.exec(text);
    if (!match) {
      continue;
    }
    const candidate = {
      type: pattern.type,
      index: match.index,
      length: match[0].length,
      ...pattern.build(match)
    };
    if (!next || candidate.index < next.index) {
      next = candidate;
    }
  }
  return next;
}
