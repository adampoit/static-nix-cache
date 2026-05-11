#!/usr/bin/env node
'use strict';

const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const { generateStaticSite } = require('./src/generate-static');
const config = require('./src/config');

async function main() {
  const outputDir = process.env.OUTPUT_DIR || './static-cache';
  const localStoragePath = config.localStoragePath;
  const narinfoDirPath = path.join(localStoragePath, 'narinfo');

  const { owner, repo, releaseTag, token } = config.github;
  const releaseTags = config.github.releaseTags?.length > 0
    ? config.github.releaseTags
    : [releaseTag];

  if (!owner || !repo) {
    console.error('Error: GITHUB_OWNER and GITHUB_REPO must be set.');
    console.error('');
    console.error('Usage:');
    console.error('  GITHUB_OWNER=<owner> GITHUB_REPO=<repo> node generate-static.js');
    console.error('');
    console.error('Environment variables:');
    console.error('  GITHUB_OWNER         GitHub repository owner (required)');
    console.error('  GITHUB_REPO          GitHub repository name (required)');
    console.error('  GITHUB_RELEASE_TAG   Release tag for NAR files (default: nix-cache)');
    console.error('  GITHUB_RELEASE_TAGS  Comma-separated list of release tags to aggregate');
    console.error('  LOCAL_STORAGE_PATH   Path to static-nix-cache local storage (default: ./cache)');
    console.error('  OUTPUT_DIR           Output directory for static site (default: ./static-cache)');
    console.error('  STORE_DIR            Nix store directory (default: /nix/store)');
    console.error('  CACHE_PRIORITY       Cache priority (default: 30)');
    process.exit(1);
  }

  let narReleaseMap;

  // When using the github-releases backend, fetch all narinfo from the release(s)
  // so the generated site includes paths from all previous deploys (including
  // other matrix jobs and per-package releases).
  if (config.storageBackend === 'github-releases' && token) {
    const GitHubReleasesStorage = require('./src/storage/github-releases');
    narReleaseMap = new Map();

    fs.mkdirSync(narinfoDirPath, { recursive: true });

    for (const tag of releaseTags) {
      const tagLocalPath = path.join(localStoragePath, 'releases', tag);
      const storage = new GitHubReleasesStorage({
        token,
        owner,
        repo,
        releaseTag: tag,
        localPath: tagLocalPath,
      });

      await storage.fetchAllNarinfo();

      // Parse fetched narinfo to map NAR filenames back to this release tag
      const tagNarinfoDir = path.join(tagLocalPath, 'narinfo');
      let entries = [];
      try {
        entries = await fsp.readdir(tagNarinfoDir);
      } catch {
        entries = [];
      }

      for (const entry of entries) {
        if (!entry.endsWith('.narinfo')) continue;

        const src = path.join(tagNarinfoDir, entry);
        const dst = path.join(narinfoDirPath, entry);
        const content = await fsp.readFile(src, 'utf8');

        const match = content.match(/^URL:\s*nar\/(.+)$/m);
        if (match) {
          narReleaseMap.set(match[1], tag);
        }

        await fsp.writeFile(dst, content, 'utf8');
      }
    }
  }

  const displayTag = releaseTags.length > 1
    ? `${releaseTags.length} releases (${releaseTags.join(', ')})`
    : releaseTags[0];

  console.log('Generating static Nix binary cache site...');
  console.log(`  Source narinfo dir: ${narinfoDirPath}`);
  console.log(`  Output dir:        ${outputDir}`);
  console.log(`  GitHub:            ${owner}/${repo} @ ${displayTag}`);

  const result = await generateStaticSite({
    narinfoDirPath,
    outputDir,
    storeDir: config.storeDir,
    priority: config.priority,
    githubOwner: owner,
    githubRepo: repo,
    githubReleaseTag: releaseTag,
    narReleaseMap,
  });

  console.log('');
  console.log(`Generated ${result.narinfoCount} narinfo file(s)`);
  if (narReleaseMap && narReleaseMap.size > 0) {
    console.log(`Mapped ${narReleaseMap.size} NAR file(s) to specific releases`);
  }
  console.log(`NAR redirects point to: ${result.narBaseUrl}`);
  console.log(`Static site ready at: ${result.outputDir}`);
  console.log('');
  console.log('Deploy this directory to your static hosting provider.');
  console.log('For Cloudflare Pages: npx wrangler pages deploy ' + result.outputDir);
}

main().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
