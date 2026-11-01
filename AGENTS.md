# GeaFlow Agent Instructions

## Code Style

- Follow the Java style scheme in `tools/intellij-java-style.xml` and the
  repository Checkstyle configuration in `tools/checkstyle.xml`, as required
  by `CONTRIBUTING.md`.
- Treat Checkstyle as authoritative when configurations disagree: it runs in
  Maven's `validate` phase with `failOnViolation=true`. In particular, Java
  code uses four-space indentation, no tab characters, and a 200-character
  maximum line length. The root `.editorconfig` remains the default for other
  files (UTF-8, LF, trailing-whitespace removal, final newline, four spaces,
  and a 120-character preferred line length).
- Do not rely on the IntelliJ scheme's global two-space default for Java;
  configure Java indentation to four spaces or verify the result with Maven.
- Keep Java imports alphabetized within groups, with static imports before a
  blank line and non-static imports. Do not leave unused imports.
- Preserve Checkstyle requirements for braces, whitespace, one statement per
  line, a single top-level class, Java naming, Javadoc, and non-empty catch
  blocks. Do not add suppressions unless a concrete exception is necessary.
- Generated and proto sources are excluded. Files named `*Test.java` are
  suppressed from Checkstyle, but should follow these rules unless a test
  needs a justified exception.
- New source files should retain the repository's Apache license header.

## Verification

- For Java changes, run the relevant Maven `validate` or test command; Maven
  `validate` executes Checkstyle for the main build and Kubernetes operator.
- For changes under `geaflow/geaflow-dashboard/web-dashboard` or
  `geaflow-kubernetes-operator/geaflow-kubernetes-operator-web/web-dashboard`,
  run `npm run lint`. It runs ESLint, Prettier, and `tsc --noEmit`.
- The dashboard Prettier configuration requires single quotes, trailing
  commas, 100-column print width, and LF line endings. Its local `.editorconfig`
  uses two spaces.
