# CI/CD Configuration

This folder contains the CI/CD pipeline configuration for the project.

**Note on GitHub Actions:**
For GitHub Actions to function, the workflow file must be located in `.github/workflows/`. A copy has been placed there automatically.

### Pipeline Steps:
1. **Dependency Installation**: Pulls all required libraries from `requirements.txt`.
2. **Linting**: Uses `flake8` to enforce PEP 8 standards and catch syntax errors.
3. **Unit Testing**: Runs `pytest` to ensure core logic remains sound.
