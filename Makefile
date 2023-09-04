# Define the name of the virtual environment
VENV_NAME = pyvenv_310

# Define the path to Python 3.10 executable
PYTHON = python3.10

# Define the requirements file
REQUIREMENTS_FILE = requirements.txt

# Target for creating the virtual environment
venv:
	@echo "Creating Python virtual environment: $(VENV_NAME)"
	@$(PYTHON) -m venv $(VENV_NAME)

# Target for installing requirements
install-requirements: venv
	@echo "Installing requirements from $(REQUIREMENTS_FILE)"
	@$(VENV_NAME)/bin/pip install -r $(REQUIREMENTS_FILE)

# Target for cleaning up the virtual environment
clean:
	@echo "Cleaning up virtual environment: $(VENV_NAME)"
	@rm -rf $(VENV_NAME)

# Default target
all: install-requirements

# PHONY targets (targets that don't represent files)
.PHONY: venv install-requirements clean all
