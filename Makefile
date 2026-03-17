VENV = .venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip

.PHONY: install ui poll classify

install:
	python3 -m venv $(VENV)
	$(PIP) install -r requirements.txt

ui:
	$(VENV)/bin/streamlit run ui.py

poll:
	$(PYTHON) -c "from reply_listener import poll_replies; n=poll_replies(); print(f'Logged {n} replies')"

classify:
	$(PYTHON) -c "from storage.db import connect; from classifier import run_classifier; n=run_classifier(connect()); print(f'Classified {n} replies')"
