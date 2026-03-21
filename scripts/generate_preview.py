#!/usr/bin/env python3
"""
Generate docs/preview.png by loading the app locally with ?export=1 and
intercepting the PNG download that the export button triggers.

The resulting image is committed to docs/ and used as the og:image social
preview for the GitHub Pages site.

Usage:
    pip install playwright
    playwright install --with-deps chromium
    python scripts/generate_preview.py
"""

import http.server
import shutil
import threading
from pathlib import Path

from playwright.sync_api import sync_playwright

DOCS = Path(__file__).parent.parent / "docs"
PORT = 8765
OUT  = DOCS / "preview.png"


class _Handler(http.server.SimpleHTTPRequestHandler):
    """Minimal static server rooted at docs/."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(DOCS), **kwargs)

    def log_message(self, *args):
        pass  # suppress access log noise


def main() -> None:
    server = http.server.HTTPServer(("127.0.0.1", PORT), _Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    print(f"Serving {DOCS} on http://127.0.0.1:{PORT}")

    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(
            viewport={"width": 1400, "height": 700},
            accept_downloads=True,
            color_scheme="dark",   # match the app's default dark theme
        )
        page = ctx.new_page()

        # Surface any JS errors or console warnings to stdout for debugging.
        page.on("console", lambda msg: print(f"  [{msg.type}] {msg.text}") if msg.type != "log" else None)
        page.on("pageerror", lambda err: print(f"  [pageerror] {err}"))

        # ?export=1 triggers btn-export.click() after a 4-second delay that
        # allows map data to load.  page.expect_download() is the correct
        # page-level API for intercepting downloads in Playwright.
        print("Loading page and waiting for export download…")
        with page.expect_download(timeout=60_000) as dl:
            page.goto(
                f"http://127.0.0.1:{PORT}/?export=light",
                wait_until="domcontentloaded",
                timeout=30_000,
            )

        tmp = dl.value.path()
        if not tmp:
            raise RuntimeError("Download completed but no file path was returned.")
        shutil.copy(tmp, OUT)
        print(f"Preview saved → {OUT}")

        browser.close()

    server.shutdown()


if __name__ == "__main__":
    main()
