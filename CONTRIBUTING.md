# Contributing to BQ2PG

Thank you for your interest in contributing to BQ2PG! This document provides guidelines for contributing to the project.

## 🌟 How to Contribute

### Reporting Bugs

1. Check if the bug has already been reported in [Issues](https://github.com/yourusername/BQ2PG/issues)
2. If not, create a new issue with:
   - Clear title and description
   - Steps to reproduce
   - Expected vs actual behavior
   - Screenshots if applicable
   - Your environment (OS, Python version, etc.)

### Suggesting Features

1. Check [Discussions](https://github.com/yourusername/BQ2PG/discussions) for similar ideas
2. Create a new discussion with:
   - Clear use case
   - Proposed solution
   - Why it benefits the community

### Submitting Code

1. **Fork the repository**
   ```bash
   git clone https://github.com/yourusername/BQ2PG.git
   cd BQ2PG
   ```

2. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make your changes**
   - Write clean, readable code
   - Follow existing code style
   - Add tests for new features
   - Update documentation

4. **Test your changes**
   ```bash
   pytest tests/
   ```

5. **Commit your changes**
   ```bash
   git commit -m "feat: add your feature description"
   ```

   Use conventional commits:
   - `feat:` - New feature
   - `fix:` - Bug fix
   - `docs:` - Documentation changes
   - `style:` - Code style changes
   - `refactor:` - Code refactoring
   - `test:` - Test changes
   - `chore:` - Build/tooling changes

6. **Push and create a pull request**
   ```bash
   git push origin feature/your-feature-name
   ```

## 📋 Development Setup

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Git

### Local Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/BQ2PG.git
cd BQ2PG

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Start PostgreSQL
docker-compose up -d postgres

# Run the app
python api/server.py
```

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov=api

# Run specific test file
pytest tests/test_api.py
```

## 📝 Code Style

- Follow PEP 8
- Use type hints where possible
- Write docstrings for functions and classes
- Keep functions small and focused
- Use meaningful variable names

### Example

```python
def migrate_data(source: str, destination: str, limit: int = None) -> dict:
    """
    Migrate data from BigQuery to PostgreSQL.
    
    Args:
        source: BigQuery table reference (project.dataset.table)
        destination: PostgreSQL table name
        limit: Optional row limit
        
    Returns:
        dict: Migration statistics
    """
    # Implementation
    pass
```

## 🎯 Project Goals

Keep these in mind when contributing:

1. **Simplicity** - Easy for anyone to use
2. **Free** - No paid services required
3. **Open Source** - Community-driven
4. **Reliable** - Well-tested and documented

## 🚫 What We Don't Accept

- Paid service dependencies
- Overly complex features
- Breaking changes without discussion
- Code without tests
- Undocumented features

## 📜 Code of Conduct

### Our Pledge

We pledge to make participation in our project a harassment-free experience for everyone.

### Our Standards

- Be respectful and inclusive
- Accept constructive criticism
- Focus on what's best for the community
- Show empathy towards others

### Enforcement

Violations can be reported to the project maintainers. All complaints will be reviewed and investigated.

## 📞 Getting Help

- **Questions**: [GitHub Discussions](https://github.com/yourusername/BQ2PG/discussions)
- **Bugs**: [GitHub Issues](https://github.com/yourusername/BQ2PG/issues)
- **Chat**: [Discord](https://discord.gg/bq2pg) (coming soon)

## 🏆 Recognition

Contributors will be:
- Listed in README.md
- Mentioned in release notes
- Given credit in documentation

Thank you for contributing to BQ2PG! 🎉
