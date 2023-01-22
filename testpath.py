from pathlib import Path

title = 'A'
raw_text = 'Ahmed'
text_path = Path("random/results").resolve()
text_path.mkdir(parents=True, exist_ok=True)
(text_path / f"{title}.txt").write_text(raw_text)
