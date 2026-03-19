VENV = .venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip

.PHONY: install ui poll classify generate preview review review-status poll-reviews schedule process-send-queue queue reply-queue prepare-replies preview-replies send-replies internal-reply-test internal-reply-test-status smoke

install:
	python3 -m venv $(VENV)
	$(PIP) install -r requirements.txt

ui:
	$(VENV)/bin/streamlit run ui.py

poll:
	$(PYTHON) -c "from reply_listener import poll_replies; n=poll_replies(); print(f'Logged {n} replies')"

classify:
	$(PYTHON) -c "from storage.db import connect; from classifier import run_classifier; n=run_classifier(connect()); print(f'Classified {n} replies')"

generate:
	$(PYTHON) cli.py generate -n $${N:-5} --niche "$${NICHE}"

preview:
	$(PYTHON) cli.py preview -n $${N:-5}

review:
	$(PYTHON) cli.py review -n $${N:-5} --to "$${TO:-egorbrusnyak@gmail.com}"

review-status:
	$(PYTHON) cli.py review-status -n $${N:-10}

poll-reviews:
	$(PYTHON) cli.py poll-reviews -n $${N:-10}

schedule:
	$(PYTHON) cli.py schedule -n $${N:-5}

process-send-queue:
	$(PYTHON) cli.py process-send-queue -n $${N:-5}

queue:
	$(PYTHON) cli.py queue -n $${N:-10}

reply-queue:
	$(PYTHON) cli.py reply-queue -n $${N:-10}

prepare-replies:
	$(PYTHON) cli.py prepare-replies -n $${N:-10}

preview-replies:
	$(PYTHON) cli.py preview-replies -n $${N:-5}

send-replies:
	$(PYTHON) cli.py send-replies -n $${N:-5}

internal-reply-test:
	$(PYTHON) cli.py internal-reply-test

internal-reply-test-status:
	$(PYTHON) cli.py internal-reply-test-status -n $${N:-5}

smoke:
	$(PYTHON) smoke_tests.py $${ARGS}
