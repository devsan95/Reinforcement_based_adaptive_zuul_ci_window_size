#!/usr/bin/env python3
"""Inject RL UI overlay into stock Zuul index.html (keeps correct JS bundle paths).

Re-applies on every start so updated rl-inject.html (image rebuild or bind-mount)
always wins over a previously injected fragment.
"""
from pathlib import Path
import re

INDEX = Path("/usr/local/lib/python3.11/site-packages/zuul/web/static/index.html")
FRAG = Path("/overlay/rl-inject.html")
BEGIN = "<!-- zuul-rl-overlay-begin -->"
END = "<!-- zuul-rl-overlay-end -->"


def _strip_existing(text: str) -> str:
    """Remove a previously injected overlay (marked or legacy style+script)."""
    if BEGIN in text and END in text:
        pattern = re.compile(
            re.escape(BEGIN) + r".*?" + re.escape(END),
            re.DOTALL,
        )
        return pattern.sub("", text)
    # Legacy inject (pre-marker): style#zuul-rl-window-style through its script.
    pattern = re.compile(
        r'\s*<style id="zuul-rl-window-style">.*?</script>\s*',
        re.DOTALL,
    )
    return pattern.sub("\n", text, count=1)


def main():
    if not INDEX.is_file():
        raise SystemExit(f"missing index: {INDEX}")
    if not FRAG.is_file():
        raise SystemExit(f"missing overlay: {FRAG}")
    text = _strip_existing(INDEX.read_text(encoding="utf-8"))
    frag = FRAG.read_text(encoding="utf-8").strip()
    if BEGIN not in frag:
        frag = f"{BEGIN}\n{frag}\n{END}"
    if "</body>" not in text:
        raise SystemExit("index.html has no </body>")
    INDEX.write_text(
        text.replace("</body>", frag + "\n</body>", 1),
        encoding="utf-8",
    )
    print("RL UI overlay injected into", INDEX)


if __name__ == "__main__":
    main()
