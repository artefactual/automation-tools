.DEFAULT_GOAL := help

.PHONY: pip-compile pip-upgrade

pip-compile:  ## Compile pip requirements
	pip-compile --allow-unsafe --output-file=requirements.txt requirements.in
	pip-compile --allow-unsafe --output-file=requirements-dev.txt requirements-dev.in

pip-upgrade:  ## Upgrade pip requirements
	pip-compile --allow-unsafe --upgrade --output-file=requirements.txt requirements.in
	pip-compile --allow-unsafe --upgrade --output-file=requirements-dev.txt requirements-dev.in

help:  ## Print this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
