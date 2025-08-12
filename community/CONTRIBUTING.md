# Contributing to GeaFlow

Thank you for your interest in contributing to GeaFlow! We welcome all kinds of contributions and appreciate your support.

## How to Contribute

### 1. Prerequisites

Before you start, make sure you have:
- Java 11 or above
- Maven 3.6+
- Git
You may also need Docker for some modules.

### 2. Fork the Repository
Click the "Fork" button on the top right of the [GeaFlow GitHub page](https://github.com/apache/geaflow).

### 3. Clone Your Fork
Clone your fork to your local machine:
```bash
git clone https://github.com/<your-username>/geaflow.git
cd geaflow
```

### 4. Create a Branch
Create a new branch for your work:
```bash
git checkout -b my-feature-branch
```

### 5. Make Changes
Edit the code. Please follow the coding style (see below) and add tests if needed.

### 6. Run Tests
Run all tests to make sure your changes work:
```bash
mvn clean install
```
Fix any issues before submitting your changes.

### 7. Commit Changes
Write a clear commit message:
```bash
git add .
git commit -m "[Module] Brief description of your changes"
```

### 8. Push Changes
Push your branch to your fork:
```bash
git push origin my-feature-branch
```

### 9. Create a Pull Request
Go to your fork on GitHub and click "New Pull Request". Fill in a clear title and description. Link related issues if any.

### 10. Review Process
Project maintainers will review your pull request. Please respond to feedback and make changes if needed.

## Code of Conduct

Please follow our [Code of Conduct](../CODE_OF_CONDUCT.md) in all project interactions.


## Coding Style
Please follow the Java style in `tools/intellij-java-style.xml` and checkstyle rules in `tools/checkstyle.xml`. You can use IDE plugins to auto-format code.

## Reporting Issues
If you find a bug or have a feature request, please open an issue on [GitHub Issues](https://github.com/apache/geaflow/issues) with clear steps to reproduce or describe your suggestion.

## Documentation
See the [docs folder](../docs) for user and developer documentation.

## Community
- [Issue Tracker](https://github.com/apache/geaflow/issues)
- [Mailing List](https://geaflow.apache.org/community.html)
- [Website](https://geaflow.apache.org/)

Thank you for helping improve GeaFlow!
