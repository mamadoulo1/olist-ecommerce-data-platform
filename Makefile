.DEFAULT_GOAL := help
PYTHON := python
PIP    := pip

# ─────────────────────────────────────────────
#  help  — liste toutes les cibles disponibles
# ─────────────────────────────────────────────
.PHONY: help
help:
	@echo ""
	@echo "  Commandes disponibles :"
	@echo ""
	@echo "  make install       Installe les dépendances dev"
	@echo "  make format        Formate le code (black + isort)"
	@echo "  make lint          Vérifie le style (black + isort + flake8)"
	@echo "  make test          Lance les tests avec couverture"
	@echo "  make build         Crée le wheel .whl pour Databricks"
	@echo "  make clean         Supprime les fichiers temporaires"
	@echo "  make deploy-dev    Déploie sur Databricks dev"
	@echo "  make deploy-prod   Déploie sur Databricks prod"
	@echo ""

# ─────────────────────────────────────────────
#  install
# ─────────────────────────────────────────────
.PHONY: install
install:
	$(PIP) install -r requirements-dev.txt
	$(PIP) install -e .

# ─────────────────────────────────────────────
#  format  — modifie les fichiers directement
# ─────────────────────────────────────────────
.PHONY: format
format:
	black src/ tests/
	isort src/ tests/

# ─────────────────────────────────────────────
#  lint  — vérifie sans modifier
# ─────────────────────────────────────────────
.PHONY: lint
lint:
	black --check src/ tests/
	isort --check-only src/ tests/
	flake8 src/ tests/

# ─────────────────────────────────────────────
#  test
# ─────────────────────────────────────────────
.PHONY: test
test:
	$(PYTHON) -m pytest tests/ -v --cov=src --cov-report=term-missing

# ─────────────────────────────────────────────
#  build  — crée le wheel deployable sur Databricks
# ─────────────────────────────────────────────
.PHONY: build
build: clean
	$(PYTHON) -m build --wheel

# ─────────────────────────────────────────────
#  clean
# ─────────────────────────────────────────────
.PHONY: clean
clean:
	rm -rf dist/ build/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# ─────────────────────────────────────────────
#  deploy
# ─────────────────────────────────────────────
.PHONY: deploy-dev
deploy-dev: build
	databricks bundle deploy --target dev

.PHONY: deploy-prod
deploy-prod: build
	databricks bundle deploy --target prod
