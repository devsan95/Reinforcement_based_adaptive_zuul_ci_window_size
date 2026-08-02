#!/usr/bin/env python3
"""Inject RL UI overlay into stock Zuul index.html (keeps correct JS bundle paths)."""
from pathlib import Path

INDEX = Path("/usr/local/lib/python3.11/site-packages/zuul/web/static/index.html")
FRAG = Path("/overlay/rl-inject.html")

def main():
    if not INDEX.is_file():
        raise SystemExit(f"missing index: {INDEX}")
    text = INDEX.read_text(encoding="utf-8")
    if "zuul-rl-window-style" in text:
        return
    if not FRAG.is_file():
        raise SystemExit(f"missing overlay: {FRAG}")
    frag = FRAG.read_text(encoding="utf-8")
    if "</body>" not in text:
        raise SystemExit("index.html has no </body>")
    INDEX.write_text(text.replace("</body>", frag + "\n</body>", 1), encoding="utf-8")
    print("RL UI overlay injected into", INDEX)

if __name__ == "__main__":
    main()
