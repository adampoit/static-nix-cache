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
 * NAR binary files are expected to be hosted on GitHub Releases. The
 * generated site includes a `_redirects` file (Cloudflare Pages compatible)
 * that redirects `/nar/*` requests to the GitHub Release download URLs.
 *
 * @param {object} options
 * @param {string} options.narinfoDirPath   - path to the directory containing narinfo files
 * @param {string} options.outputDir        - directory to write generated static files
 * @param {string} options.storeDir         - Nix store directory (default: /nix/store)
 * @param {number} options.priority         - cache priority (default: 30)
 * @param {string} options.githubOwner      - GitHub repo owner
 * @param {string} options.githubRepo       - GitHub repo name
 * @param {string} options.githubReleaseTag - GitHub release tag name
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
  const narBaseUrl = `https://github.com/${githubOwner}/${githubRepo}/releases/download/${encodeURIComponent(githubReleaseTag)}`;
  for (const filename of narinfoFiles) {
    const content = await fsp.readFile(path.join(narinfoDir, filename), 'utf8');
    const rewritten = content.replace(/^URL:\s*nar\/(.+)$/m, (_, nar) => `URL: ${narBaseUrl}/${nar}`);
    await fsp.writeFile(path.join(outputDir, filename), rewritten, 'utf8');
  }

  // 3. Generate _redirects file for Cloudflare Pages
  // This redirects NAR download requests to GitHub Releases
  const redirects = `/nar/:filename ${narBaseUrl}/:filename 302\n`;
  await fsp.writeFile(path.join(outputDir, '_redirects'), redirects, 'utf8');

  return {
    narinfoCount: narinfoFiles.length,
    outputDir,
    narBaseUrl,
  };
}

module.exports = { generateStaticSite };
