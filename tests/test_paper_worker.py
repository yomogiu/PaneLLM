import unittest
from unittest.mock import patch

from broker import paper_worker
from broker.paper_worker import WorkerError, html_to_sections


class HtmlToSectionsTest(unittest.TestCase):
    def test_ignores_head_title_when_body_has_h1(self) -> None:
        sections, headings, full_text = html_to_sections(
            """
            <html>
              <head>
                <title>Example Paper</title>
              </head>
              <body>
                <header>
                  <h1>Example Paper</h1>
                </header>
                <p>Abstract body.</p>
              </body>
            </html>
            """
        )

        self.assertEqual(["Example Paper"], [section["heading"] for section in sections])
        self.assertEqual(["Example Paper"], [heading["heading"] for heading in headings])
        self.assertIn("Abstract body.", sections[0]["text"])
        self.assertNotIn("Document", [section["heading"] for section in sections])
        self.assertIn("Abstract body.", full_text)

    def test_groups_complex_blocks_under_following_heading(self) -> None:
        sections, _, _ = html_to_sections(
            """
            <body>
              <h1>Main Result</h1>
              <p>Overview paragraph.</p>
              <div>Key finding</div>
              <h2>Method</h2>
              <ul>
                <li>Collect data</li>
                <li>Train model</li>
              </ul>
            </body>
            """
        )

        self.assertEqual(["Main Result", "Method"], [section["heading"] for section in sections])
        self.assertIn("Overview paragraph.", sections[0]["text"])
        self.assertIn("Key finding", sections[0]["text"])
        self.assertIn("Collect data", sections[1]["text"])
        self.assertIn("Train model", sections[1]["text"])

    def test_preserves_long_document_preamble_before_first_heading(self) -> None:
        intro = " ".join(["long-intro"] * 40)
        sections, _, _ = html_to_sections(
            f"""
            <body>
              <p>{intro}</p>
              <h1>Results</h1>
              <p>Measured outcome.</p>
            </body>
            """
        )

        self.assertGreater(len(intro), 280)
        self.assertEqual("Document", sections[0]["heading"])
        self.assertEqual("Results", sections[1]["heading"])
        self.assertIn("Measured outcome.", sections[1]["text"])


class ExtractSourceTest(unittest.TestCase):
    def test_promotes_url_html_sources_to_discovered_pdf(self) -> None:
        source = {
            "source_type": "url",
            "url": "https://arxiv.org/abs/1234.5678",
            "content_type": "text/html",
            "raw_bytes": b"""
            <html>
              <head>
                <meta name="citation_title" content="Example Paper" />
                <meta name="citation_author" content="Jane Doe" />
                <meta name="citation_abstract" content="Landing abstract." />
                <meta name="citation_pdf_url" content="https://arxiv.org/pdf/1234.5678.pdf" />
              </head>
              <body>
                <nav>Download PDF</nav>
                <p>Landing chrome that should not be extracted.</p>
              </body>
            </html>
            """,
            "display_name": "1234.5678",
        }

        with (
            patch.object(
                paper_worker,
                "fetch_url",
                return_value=(b"%PDF-1.4 stub", "application/pdf", "https://arxiv.org/pdf/1234.5678.pdf"),
            ) as fetch_mock,
            patch.object(
                paper_worker,
                "extract_pdf_text",
                return_value="Paper body paragraph.\n\nMethods section body.",
            ),
        ):
            artifact = paper_worker.extract_source(source)

        fetch_mock.assert_called_once_with("https://arxiv.org/pdf/1234.5678.pdf", max_bytes=paper_worker.MAX_EXTRACT_BYTES)
        self.assertEqual("pdf", artifact["source_format"])
        self.assertEqual("promoted_pdf_url", artifact["extraction_path"])
        self.assertEqual("https://arxiv.org/pdf/1234.5678.pdf", artifact["url"])
        self.assertEqual("https://arxiv.org/abs/1234.5678", artifact["requested_url"])
        self.assertEqual("https://arxiv.org/pdf/1234.5678.pdf", artifact["pdf_url"])
        self.assertEqual(["Jane Doe"], artifact["authors"])
        self.assertEqual("Landing abstract.", artifact["abstract"])
        self.assertIn("Paper body paragraph.", artifact["sections"][0]["text"])
        self.assertNotIn("Landing chrome", artifact["text_preview"])

    def test_keeps_html_extraction_when_no_pdf_url_is_available(self) -> None:
        source = {
            "source_type": "url",
            "url": "https://example.com/paper",
            "content_type": "text/html",
            "raw_bytes": b"""
            <html>
              <body>
                <h1>Wrapper Title</h1>
                <p>HTML page body that should remain the extracted source.</p>
              </body>
            </html>
            """,
            "display_name": "paper",
        }

        with patch.object(paper_worker, "fetch_url") as fetch_mock:
            artifact = paper_worker.extract_source(source)

        fetch_mock.assert_not_called()
        self.assertEqual("html", artifact["source_format"])
        self.assertEqual("html_source", artifact["extraction_path"])
        self.assertEqual("https://example.com/paper", artifact["url"])
        self.assertEqual("", artifact["extraction_fallback_reason"])
        self.assertIn("HTML page body", artifact["sections"][0]["text"])

    def test_pdf_promotion_failures_raise_without_explicit_html_fallback(self) -> None:
        source = {
            "source_type": "url",
            "url": "https://arxiv.org/abs/9999.0001",
            "content_type": "text/html",
            "raw_bytes": b"""
            <html>
              <head>
                <meta name="citation_pdf_url" content="https://arxiv.org/pdf/9999.0001.pdf" />
              </head>
              <body>
                <p>Fallback body.</p>
              </body>
            </html>
            """,
            "display_name": "9999.0001",
        }

        with (
            patch.object(
                paper_worker,
                "fetch_url",
                return_value=(b"%PDF-1.4 broken", "application/pdf", "https://arxiv.org/pdf/9999.0001.pdf"),
            ),
            patch.object(
                paper_worker,
                "extract_pdf_text",
                side_effect=WorkerError("pdf_extract_failed", "No extractable text."),
            ),
        ):
            with self.assertRaises(WorkerError) as raised:
                paper_worker.extract_source(source)

        self.assertEqual("pdf_extract_failed", raised.exception.code)

    def test_pdf_extract_failures_can_fallback_to_html_when_explicitly_allowed(self) -> None:
        source = {
            "source_type": "url",
            "url": "https://arxiv.org/abs/9999.0002",
            "content_type": "text/html",
            "raw_bytes": b"""
            <html>
              <head>
                <meta name="citation_title" content="Fallback Paper" />
                <meta name="citation_pdf_url" content="https://arxiv.org/pdf/9999.0002.pdf" />
              </head>
              <body>
                <h1>Fallback Paper</h1>
                <p>Fallback HTML body.</p>
              </body>
            </html>
            """,
            "display_name": "9999.0002",
        }

        with (
            patch.object(
                paper_worker,
                "fetch_url",
                return_value=(b"%PDF-1.4 broken", "application/pdf", "https://arxiv.org/pdf/9999.0002.pdf"),
            ),
            patch.object(
                paper_worker,
                "extract_pdf_text",
                side_effect=WorkerError("pdf_extract_failed", "No extractable text."),
            ),
        ):
            artifact = paper_worker.extract_source(source, allow_html_fallback=True)

        self.assertEqual("html", artifact["source_format"])
        self.assertEqual("html_fallback_after_pdf_extract_error", artifact["extraction_path"])
        self.assertIn("pdf_extract_failed", artifact["extraction_fallback_reason"])
        self.assertEqual("https://arxiv.org/abs/9999.0002", artifact["url"])
        self.assertIn("Fallback HTML body.", artifact["sections"][0]["text"])


if __name__ == "__main__":
    unittest.main()
