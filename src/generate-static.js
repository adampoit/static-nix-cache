'use strict';

const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');

/**
 * Generate a static Nix binary cache site from locally stored narinfo files.
 *
 * The generated directory can be deployed to any static hosting provider
 * (Cloudflare Pages, GitHub Pages, Netlify, etc.) and will serve as a
 * Nix binary cache substituter.
 *
 * NAR binary files are expected to be hosted on GitHub Releases. Narinfo
 * entries are rewritten to point directly at those release download URLs so
 * the generated cache works on static hosts that do not support redirects.
 * The generated site also includes a `_redirects` file for hosts that do.
 *
 * When `narReleaseMap` is provided, each narinfo's `URL:` is rewritten to
 * point at the specific release tag that owns the NAR file. This allows a
 * single static cache to aggregate NARs spread across many per-package or
 * per-platform releases.
 *
 * @param {object} options
 * @param {string} options.narinfoDirPath   - path to the directory containing narinfo files
 * @param {string} options.outputDir        - directory to write generated static files
 * @param {string} options.storeDir         - Nix store directory (default: /nix/store)
 * @param {number} options.priority         - cache priority (default: 30)
 * @param {string} options.githubOwner      - GitHub repo owner
 * @param {string} options.githubRepo       - GitHub repo name
 * @param {string} options.githubReleaseTag - default GitHub release tag name (fallback)
 * @param {Map<string,string>} [options.narReleaseMap] - map of NAR filename to release tag
 */
async function generateStaticSite(options) {
  const {
    narinfoDirPath,
    outputDir,
    storeDir = '/nix/store',
    priority = 30,
    githubOwner,
    githubRepo,
    githubReleaseTag,
    narReleaseMap,
  } = options;

  // Create output directory
  await fsp.mkdir(outputDir, { recursive: true });

  // 1. Generate nix-cache-info
  const cacheInfo =
    `StoreDir: ${storeDir}\n` +
    `WantMassQuery: 1\n` +
    `Priority: ${priority}\n`;
  await fsp.writeFile(path.join(outputDir, 'nix-cache-info'), cacheInfo, 'utf8');

  // 2. Copy narinfo files to output directory
  const narinfoDir = narinfoDirPath;
  let entries;
  try {
    entries = await fsp.readdir(narinfoDir);
  } catch {
    entries = [];
  }

  const narinfoFiles = entries.filter(f => f.endsWith('.narinfo'));
  const defaultNarBaseUrl = `https://github.com/${githubOwner}/${githubRepo}/releases/download/${encodeURIComponent(githubReleaseTag)}`;

  for (const filename of narinfoFiles) {
    const content = await fsp.readFile(path.join(narinfoDir, filename), 'utf8');
    const rewritten = content.replace(/^URL:\s*nar\/(.+)$/m, (_, nar) => {
      const tag = narReleaseMap?.get(nar) || githubReleaseTag;
      const baseUrl = `https://github.com/${githubOwner}/${githubRepo}/releases/download/${encodeURIComponent(tag)}`;
      return `URL: ${baseUrl}/${nar}`;
    });
    await fsp.writeFile(path.join(outputDir, filename), rewritten, 'utf8');
  }

  // 3. Generate _redirects file for static hosts that support it
  const redirects = `/nar/:filename ${defaultNarBaseUrl}/:filename 302\n`;
  await fsp.writeFile(path.join(outputDir, '_redirects'), redirects, 'utf8');

  return {
    narinfoCount: narinfoFiles.length,
    outputDir,
    narBaseUrl: defaultNarBaseUrl,
  };
}

module.exports = { generateStaticSite };
