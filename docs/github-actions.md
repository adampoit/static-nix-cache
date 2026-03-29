---
sidebar_position: 5
---

# GitHub Actions

static-nix-cache provides composable actions for integrating Nix binary caching into your CI workflows. Use **setup** + **deploy** for simple builds, or add **save** when you need to defer deployment (e.g. matrix builds with a final aggregation job). Store paths are always auto-detected — no need to manually capture build output.

## Standalone (Single Build)

Use **setup** before your build and **deploy** after. New store paths are auto-detected. Add the GitHub Pages deploy steps to publish the generated static cache site:

```yaml
jobs:
  build:
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.pages.outputs.page_url }}
    steps:
      - uses: actions/checkout@v4
      - uses: DeterminateSystems/nix-installer-action@main

      - uses: randymarsh77/static-nix-cache/setup@v1

      - name: Build
        run: nix build

      - uses: randymarsh77/static-nix-cache/deploy@v1
        with:
          github-token: ${{ secrets.GITHUB_TOKEN }}
          static: ./site

      - uses: actions/upload-pages-artifact@v3
        with:
          path: ./site

      - uses: actions/deploy-pages@v4
        id: pages
```

## Matrix Builds (deploy per-job)

Each matrix job runs **setup** + **deploy** independently — store paths and narinfo are pushed directly to the backend from every job. During static site generation, narinfo from all previous deploys (including other matrix jobs) is fetched from the release, so the generated site is always a complete manifest:

```yaml
jobs:
  build:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: DeterminateSystems/nix-installer-action@main

      - uses: randymarsh77/static-nix-cache/setup@v1

      - name: Build
        run: nix build

      - uses: randymarsh77/static-nix-cache/deploy@v1
        with:
          github-token: ${{ secrets.GITHUB_TOKEN }}
```

## Matrix Builds (deferred deploy)

For workflows that aggregate other static content or prefer a single deploy step, use **save** in each matrix job to export store paths as workflow artifacts, then **deploy** in a final job. Deploy automatically restores saved artifacts — no explicit restore step needed. If your build creates extra derivations you do not want to publish, pass an explicit `paths-file` to `save`:

```yaml
jobs:
  build:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: DeterminateSystems/nix-installer-action@main

      - name: Build
        run: nix build .#app --print-out-paths | tee /tmp/store-paths.txt

      - uses: randymarsh77/static-nix-cache/save@v1
        with:
          name: ${{ matrix.os }}
          paths-file: /tmp/store-paths.txt

  deploy:
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.pages.outputs.page_url }}
    steps:
      - uses: randymarsh77/static-nix-cache/deploy@v1
        with:
          github-token: ${{ secrets.GITHUB_TOKEN }}
          static: ./site

      - uses: actions/upload-pages-artifact@v3
        with:
          path: ./site

      - uses: actions/deploy-pages@v4
        id: pages
```

## With magic-nix-cache

If you use [DeterminateSystems/magic-nix-cache-action](https://github.com/DeterminateSystems/magic-nix-cache-action), the deploy action auto-detects the running daemon and discovers built paths — no extra configuration needed:

```yaml
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: DeterminateSystems/nix-installer-action@main
      - uses: DeterminateSystems/magic-nix-cache-action@main

      - name: Build
        run: nix build

      - uses: randymarsh77/static-nix-cache/deploy@v1
        with:
          github-token: ${{ secrets.GITHUB_TOKEN }}
          static: ./site

      - uses: actions/upload-pages-artifact@v3
        with:
          path: ./site

      - uses: actions/deploy-pages@v4
```

## Matrix with magic-nix-cache (deferred deploy)

For matrix builds using magic-nix-cache, use **save** in each job. The save action detects new paths using the daemon's startup timestamp. Deploy in a final job auto-restores the artifacts:

```yaml
jobs:
  build:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: DeterminateSystems/nix-installer-action@main
      - uses: DeterminateSystems/magic-nix-cache-action@main

      - name: Build
        run: nix build

      - uses: randymarsh77/static-nix-cache/save@v1
        with:
          name: ${{ matrix.os }}

  deploy:
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.pages.outputs.page_url }}
    steps:
      - uses: randymarsh77/static-nix-cache/deploy@v1
        with:
          github-token: ${{ secrets.GITHUB_TOKEN }}
          static: ./site

      - uses: actions/upload-pages-artifact@v3
        with:
          path: ./site

      - uses: actions/deploy-pages@v4
        id: pages
```

## Action Reference

### `setup`

Snapshots the current Nix store so new paths can be auto-detected by the save or deploy actions.

| Input       | Required | Default      | Description                     |
| ----------- | -------- | ------------ | ------------------------------- |
| `store-dir` | no       | `/nix/store` | Path to the Nix store directory |

| Output          | Description                                            |
| --------------- | ------------------------------------------------------ |
| `snapshot-path` | Path to the file containing the initial store snapshot |

### `save`

Exports Nix store paths (including closures) as a workflow artifact. Supports explicit roots via `paths-file`, or auto-detects new paths via the setup snapshot or magic-nix-cache's daemon timestamp.

| Input           | Required | Default                                              | Description                                       |
| --------------- | -------- | ---------------------------------------------------- | ------------------------------------------------- |
| `name`          | no       | `default`                                            | Unique artifact name suffix (e.g. `linux-x86_64`) |
| `paths-file`    | no       |                                                      | File listing store paths to export explicitly     |
| `signing-key`   | no       |                                                      | Nix signing key for signing exported binaries     |
| `snapshot-path` | no       | `/tmp/static-nix-cache-setup/store-paths-before.txt` | Path to the store snapshot (from `setup`)         |
| `store-dir`     | no       | `/nix/store`                                         | Path to the Nix store directory                   |

Save resolves store paths in this priority order:

1. Explicit `paths-file` input
2. Setup snapshot (diffing current store against snapshot)
3. magic-nix-cache daemon timestamp

### `deploy`

Pushes store paths to the configured backend and optionally generates a static site. Auto-detects paths via setup snapshot, saved artifacts (auto-restored), or a running magic-nix-cache daemon.

| Input                | Required | Default                                              | Description                                                                                          |
| -------------------- | -------- | ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `paths-file`         | no       |                                                      | File listing store paths (for advanced use)                                                          |
| `export-dir`         | no       |                                                      | Binary cache export dir. When set, NARs are read from this directory instead of the local nix store. |
| `snapshot-path`      | no       | `/tmp/static-nix-cache-setup/store-paths-before.txt` | Store snapshot from `setup` (for auto-detection)                                                     |
| `store-dir`          | no       | `/nix/store`                                         | Path to the Nix store directory                                                                      |
| `artifact-pattern`   | no       | `static-nix-cache-*`                                 | Artifact name pattern for auto-restore                                                               |
| `backend`            | no       | `github-releases`                                    | Storage backend                                                                                      |
| `github-token`       | no       |                                                      | GitHub token (required for `github-releases`)                                                        |
| `github-owner`       | no       | _current owner_                                      | Repository owner                                                                                     |
| `github-repo`        | no       | _current repo_                                       | Repository name                                                                                      |
| `github-release-tag` | no       | `nix-cache`                                          | Release tag for NAR storage                                                                          |
| `signing-key`        | no       |                                                      | Nix signing key                                                                                      |
| `upload-secret`      | no       |                                                      | Bearer token for upload auth                                                                         |
| `static`             | no       |                                                      | Output dir for static site generation                                                                |
| `port`               | no       | `18734`                                              | Temporary server port                                                                                |
| `compression`        | no       | `none`                                               | Compression for `nix copy`                                                                           |

Deploy auto-detects store paths in this priority order:

1. Explicit `paths-file` input
2. Saved artifacts (auto-restored from `static-nix-cache-*` pattern)
3. Setup snapshot (diffing current store against snapshot)
4. magic-nix-cache daemon at default address (`127.0.0.1:37515`)
