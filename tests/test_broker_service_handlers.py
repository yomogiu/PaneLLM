import atexit
import os
import shutil
import tempfile
import unittest


IMPORT_DATA_DIR = tempfile.mkdtemp(prefix="assist-test-import-broker-services-")
atexit.register(shutil.rmtree, IMPORT_DATA_DIR, ignore_errors=True)
os.environ["BROKER_DATA_DIR"] = IMPORT_DATA_DIR

from broker import local_broker

class ReadAssistantBrokerTest(unittest.TestCase):
    def test_normalize_page_context_accepts_rich_read_context(self) -> None:
        normalized = local_broker.normalize_page_context(
            {
                "title": "Attention Is All You Need",
                "url": "https://arxiv.org/html/1706.03762v7",
                "content_kind": "html",
                "selection": "Scaled dot-product attention",
                "text_excerpt": "Transformers replace recurrence with attention.",
                "heading_path": ["Abstract", "Model Architecture"],
                "selection_context": {
                    "before": "The encoder maps an input sequence",
                    "focus": "Scaled dot-product attention",
                    "after": "uses queries, keys, and values"
                }
            }
        )
        self.assertEqual("Attention Is All You Need", normalized["title"])
        self.assertEqual("html", normalized["content_kind"])
        self.assertEqual(["Abstract", "Model Architecture"], normalized["heading_path"])
        self.assertEqual("Scaled dot-product attention", normalized["selection_context"]["focus"])

    def test_format_page_context_renders_reading_fields(self) -> None:
        formatted = local_broker.format_page_context(
            {
                "title": "Example Page",
                "url": "https://example.com/read",
                "selection": "Important sentence",
                "heading_path": ["Intro", "Method"],
                "selection_context": {
                    "before": "Earlier context",
                    "focus": "Important sentence",
                    "after": "Later context"
                },
                "text_excerpt": "Bounded page excerpt"
            }
        )
        self.assertIn("Title: Example Page", formatted)
        self.assertIn("URL: https://example.com/read", formatted)
        self.assertIn("Section: Intro > Method", formatted)
        self.assertIn("Selected text:\nImportant sentence", formatted)
        self.assertIn("Local context:\nEarlier context\nImportant sentence\nLater context", formatted)
        self.assertIn("Page excerpt:\nBounded page excerpt", formatted)

    def test_browser_highlight_registered_in_broker_tool_maps(self) -> None:
        self.assertIn("browser.highlight", local_broker.BROWSER_TOOL_NAMES)
        self.assertEqual("highlight", local_broker.BROWSER_COMMAND_METHODS["browser.highlight"])
        self.assertIn("browser.highlight", local_broker.CODEX_AUTO_APPROVE_TOOLS)
        tool_names = [tool["function"]["name"] for tool in local_broker.LLAMA_BROWSER_TOOLS]
        self.assertIn("browser.highlight", tool_names)


if __name__ == "__main__":
    unittest.main()
