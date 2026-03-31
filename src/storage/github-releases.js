'use strict';

const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const stream = require('stream');
const { promisify } = require('util');

const pipeline = promisify(stream.pipeline);
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const RETRYABLE = new Set([408, 409, 423, 425, 429, 500, 502, 503, 504]);

/**
 * GitHub Releases storage backend.
 *
 * NAR files are stored as release assets on a GitHub Release.
 * narinfo files are stored on the local filesystem (for later static export).
 *
 * Layout:
 *   GitHub Release assets:  <filename>        (NAR files)
 *   Local filesystem:       <localPath>/narinfo/<hash>.narinfo
 */
class GitHubReleasesStorage {
  constructor({ token, owner, repo, releaseTag, localPath }) {
    this.token = token;
    this.owner = owner;
    this.repo = repo;
    this.releaseTag = releaseTag;
    this.localPath = localPath;
    this._releaseId = null;
    this._releasePromise = null;
    this._assets = null;
    this._assetPromise = null;
    this._assetMap = null;

    // Ensure local narinfo directory exists
    fs.mkdirSync(path.join(this.localPath, 'narinfo'), { recursive: true });
  }

  /**
   * Get or create the GitHub Release and return its ID.
   */
  async _getReleaseId(force = false) {
    if (this._releaseId && !force) return this._releaseId;
    if (this._releasePromise && !force) return this._releasePromise;

    this._releasePromise = (async () => {
      const getUrl = `https://api.github.com/repos/${this.owner}/${this.repo}/releases/tags/${encodeURIComponent(this.releaseTag)}`;
      console.log(`[github-releases] Looking up release by tag: ${this.releaseTag}`);

      const getResp = await this._request(getUrl, {
        headers: this._headers(),
      }, { allow: [404], retries: 6 });

      if (getResp.status !== 404) {
        const release = await getResp.json();
        this._releaseId = release.id;
        console.log(`[github-releases] Found existing release id=${this._releaseId}`);
        return this._releaseId;
      }

      console.log('[github-releases] Release not found, creating new release');

      const createResp = await this._request(`https://api.github.com/repos/${this.owner}/${this.repo}/releases`, {
        method: 'POST',
        headers: { ...this._headers(), 'Content-Type': 'application/json' },
        body: JSON.stringify({
          tag_name: this.releaseTag,
          name: `Nix Binary Cache (${this.releaseTag})`,
          body: 'Nix binary cache NAR files managed by static-nix-cache.',
          draft: false,
          prerelease: false,
        }),
      }, { allow: [422], retries: 6 });

      if (createResp.status === 422) {
        console.warn('[github-releases] Release create returned 422, refetching existing release');
        const retryResp = await this._request(getUrl, {
          headers: this._headers(),
        }, { retries: 6 });
        const release = await retryResp.json();
        this._releaseId = release.id;
        return this._releaseId;
      }

      const release = await createResp.json();
      this._releaseId = release.id;
      console.log(`[github-releases] Created release id=${this._releaseId}`);
      return this._releaseId;
    })();

    try {
      return await this._releasePromise;
    } finally {
      this._releasePromise = null;
    }
  }

  _headers(accept = 'application/vnd.github.v3+json') {
    return {
      Authorization: `token ${this.token}`,
      Accept: accept,
      'User-Agent': 'static-nix-cache',
      'X-GitHub-Api-Version': '2022-11-28',
    };
  }

  _interestingHeaders(resp) {
    const getHeader = name => resp.headers?.get?.(name) ?? null;
    return {
      'x-github-request-id': getHeader('x-github-request-id'),
      'x-ratelimit-limit': getHeader('x-ratelimit-limit'),
      'x-ratelimit-remaining': getHeader('x-ratelimit-remaining'),
      'x-ratelimit-reset': getHeader('x-ratelimit-reset'),
      'retry-after': getHeader('retry-after'),
      'content-type': getHeader('content-type'),
    };
  }

  _formatHeaders(resp) {
    return Object.entries(this._interestingHeaders(resp))
      .filter(([, value]) => value !== null && value !== '')
      .map(([name, value]) => `${name}=${JSON.stringify(value)}`)
      .join(', ');
  }

  _truncateBody(body) {
    if (!body) return '';
    return body.length > 1000 ? `${body.slice(0, 1000)}...` : body;
  }

  _formatFailureDetails(resp, body) {
    const headers = this._formatHeaders(resp);
    const bodyText = this._truncateBody(body);
    const parts = [];
    if (headers) parts.push(`headers={${headers}}`);
    if (bodyText) parts.push(`body=${JSON.stringify(bodyText)}`);
    return parts.length > 0 ? ` ${parts.join(' ')}` : '';
  }

  _isRetryable(resp, body) {
    if (RETRYABLE.has(resp.status)) return true;
    if (resp.status !== 403) return false;
    if (resp.headers.get('x-ratelimit-remaining') === '0') return true;
    return /rate limit/i.test(body);
  }

  _delay(resp, attempt, body) {
    const retryAfter = Number.parseInt(resp.headers.get('retry-after') || '', 10);
    if (Number.isFinite(retryAfter) && retryAfter > 0) return retryAfter * 1000;

    if (this._isRetryable(resp, body)) {
      const reset = Number.parseInt(resp.headers.get('x-ratelimit-reset') || '', 10);
      if (Number.isFinite(reset) && reset > 0) {
        return Math.max(1000, Math.min(reset * 1000 - Date.now() + 1000, 5 * 60 * 1000));
      }
    }

    return Math.min(30000, 1000 * 2 ** attempt);
  }

  async _wait(ms) {
    await new Promise(resolve => setTimeout(resolve, ms));
  }

  async _request(url, options, { allow = [], retries = 5 } = {}) {
    const method = options?.method || 'GET';
    const allowed = new Set(allow);

    for (let attempt = 0; attempt < retries; attempt++) {
      let resp;
      try {
        resp = await fetch(url, options);
      } catch (err) {
        if (attempt + 1 >= retries) throw err;
        const delay = Math.min(30000, 1000 * 2 ** attempt);
        console.warn(`[github-releases] ${method} ${url} errored (${err.message}), retrying in ${Math.ceil(delay / 1000)}s`);
        await this._wait(delay);
        continue;
      }

      if (resp.ok || allowed.has(resp.status)) return resp;

      const body = await resp.text();
      const details = this._formatFailureDetails(resp, body);
      if (attempt + 1 < retries && this._isRetryable(resp, body)) {
        const delay = this._delay(resp, attempt, body);
        console.warn(
          `[github-releases] ${method} ${url} failed (${resp.status})${details}, retrying in ${Math.ceil(delay / 1000)}s`
        );
        await this._wait(delay);
        continue;
      }

      throw new Error(`${method} ${url} failed: ${resp.status}${details}`);
    }
  }

  _setAssets(assets) {
    this._assets = assets;
    this._assetMap = new Map(assets.map(asset => [asset.name, asset]));
  }

  _upsertAsset(asset) {
    if (!this._assets || !this._assetMap) {
      this._setAssets([asset]);
      return;
    }

    const index = this._assets.findIndex(item => item.name === asset.name);
    if (index === -1) this._assets.push(asset);
    else this._assets[index] = asset;
    this._assetMap.set(asset.name, asset);
  }

  _dropAsset(name) {
    if (!this._assets || !this._assetMap) return;
    this._assets = this._assets.filter(asset => asset.name !== name);
    this._assetMap.delete(name);
  }

  async _deleteAsset(asset) {
    console.log(`[github-releases] Deleting existing asset ${asset.name} (id=${asset.id})`);
    await this._request(
      `https://api.github.com/repos/${this.owner}/${this.repo}/releases/assets/${asset.id}`,
      { method: 'DELETE', headers: this._headers() },
      { allow: [404], retries: 6 }
    );
    this._dropAsset(asset.name);
  }

  async _uploadAsset(filename, body, contentType, warn = false) {
    const releaseId = await this._getReleaseId();
    const existing = await this._findAsset(filename);
    if (existing) await this._deleteAsset(existing);

    const uploadUrl = `https://uploads.github.com/repos/${this.owner}/${this.repo}/releases/${releaseId}/assets?name=${encodeURIComponent(filename)}`;

    for (let attempt = 0; attempt < 6; attempt++) {
      let resp;
      try {
        resp = await fetch(uploadUrl, {
          method: 'POST',
          headers: {
            ...this._headers(),
            'Content-Type': contentType,
            'Content-Length': String(body.length),
          },
          body,
        });
      } catch (err) {
        if (attempt + 1 >= 6) {
          if (warn) {
            console.warn(`[github-releases] Upload ${filename} failed: ${err.message}`);
            return null;
          }
          throw err;
        }

        const delay = Math.min(30000, 1000 * 2 ** attempt);
        console.warn(`[github-releases] Upload ${filename} errored (${err.message}), retrying in ${Math.ceil(delay / 1000)}s`);
        await this._wait(delay);
        const stale = await this._findAsset(filename, true);
        if (stale) await this._deleteAsset(stale);
        continue;
      }

      if (resp.ok) {
        const asset = await resp.json();
        this._upsertAsset(asset);
        console.log(`[github-releases] Uploaded asset ${filename} successfully`);
        return asset;
      }

      const bodyText = await resp.text();
      const details = this._formatFailureDetails(resp, bodyText);
      if (resp.status === 422 && /already exists/i.test(bodyText) && attempt + 1 < 6) {
        console.warn(`[github-releases] Asset ${filename} already exists, refreshing cache before retry`);
        const stale = await this._findAsset(filename, true);
        if (stale) await this._deleteAsset(stale);
      } else if (attempt + 1 >= 6 || !this._isRetryable(resp, bodyText)) {
        const message = `[github-releases] Failed to upload asset ${filename}: ${resp.status}${details}`;
        if (warn) {
          console.warn(message);
          return null;
        }
        throw new Error(message);
      }

      const delay = this._delay(resp, attempt, bodyText);
      console.warn(
        `[github-releases] Upload ${filename} failed (${resp.status})${details}, retrying in ${Math.ceil(delay / 1000)}s`
      );
      await this._wait(delay);
    }
  }

  // ── narinfo (local filesystem) ──────────────────────────────────────────────

  async _exists(filePath) {
    try {
      await fsp.access(filePath);
      return true;
    } catch {
      return false;
    }
  }

  async hasNarinfo(hash) {
    return this._exists(path.join(this.localPath, 'narinfo', `${hash}.narinfo`));
  }

  async getNarinfo(hash) {
    const filePath = path.join(this.localPath, 'narinfo', `${hash}.narinfo`);
    try {
      return await fsp.readFile(filePath, 'utf8');
    } catch {
      return null;
    }
  }

  async putNarinfo(hash, content) {
    console.log(`[github-releases] Storing narinfo ${hash}`);
    // Write locally for immediate use by the server and static site generation
    await fsp.writeFile(
      path.join(this.localPath, 'narinfo', `${hash}.narinfo`),
      content,
      'utf8'
    );

    // Also persist to GitHub Releases so other jobs (e.g. matrix builds) and
    // future static site generations can discover all narinfo across runs.
    const filename = `${hash}.narinfo`;
    await this._uploadAsset(filename, Buffer.from(content, 'utf8'), 'text/plain', true);
  }

  /**
   * Download all `.narinfo` release assets into the local narinfo directory.
   *
   * This ensures the local narinfo directory is a complete superset of all
   * narinfo ever pushed to the release (across matrix jobs, previous runs, etc.).
   * Called before static site generation so the generated site is complete.
   *
   * @returns {Promise<number>} number of narinfo files fetched from the release
   */
  async fetchAllNarinfo() {
    console.log('[github-releases] Fetching all narinfo from release...');
    const assets = await this._listAllAssets();
    const narinfoAssets = assets.filter(a => a.name.endsWith('.narinfo'));
    const narinfoDir = path.join(this.localPath, 'narinfo');
    let fetched = 0;

    for (const asset of narinfoAssets) {
      const localFile = path.join(narinfoDir, asset.name);
      // Skip if we already have this file locally (just written by this job)
      if (await this._exists(localFile)) continue;

      const resp = await this._request(asset.url, {
        headers: this._headers('application/octet-stream'),
        redirect: 'follow',
      }, { allow: [404], retries: 6 });

      if (resp.status === 404) {
        console.warn(`[github-releases] Warning: could not download narinfo asset ${asset.name}: 404`);
        continue;
      }

      const content = await resp.text();
      await fsp.writeFile(localFile, content, 'utf8');
      fetched++;
    }

    console.log(`[github-releases] Fetched ${fetched} narinfo file(s) from release (${narinfoAssets.length} total on release)`);
    return fetched;
  }

  // ── NAR files (GitHub Release assets) ───────────────────────────────────────

  async hasNar(filename) {
    const asset = await this._findAsset(filename);
    return asset !== null;
  }

  async getNarStream(filename) {
    const asset = await this._findAsset(filename);
    if (!asset) return null;

    const resp = await this._request(asset.url, {
      headers: this._headers('application/octet-stream'),
      redirect: 'follow',
    }, { allow: [404], retries: 6 });

    if (resp.status === 404) return null;

    const { Readable } = require('stream');
    return Readable.fromWeb(resp.body);
  }

  async putNarStream(filename, readableStream) {
    // Collect stream into buffer for upload.
    // Note: GitHub's upload API requires Content-Length, so the full NAR
    // must be buffered.  For very large NARs consider using S3 storage.
    const chunks = [];
    for await (const chunk of readableStream) {
      chunks.push(Buffer.from(chunk));
    }
    const body = Buffer.concat(chunks);

    console.log(`[github-releases] Uploading NAR asset ${filename} (${body.length} bytes)`);
    await this._uploadAsset(filename, body, 'application/octet-stream');
  }

  /**
   * Find a release asset by filename (paginates through all assets).
   * @param {string} filename
   * @returns {Promise<object|null>}
   */
  async _findAsset(filename, force = false) {
    const cached = Boolean(this._assets) && !force;
    await this._listAllAssets(force);
    const asset = this._assetMap?.get(filename) || null;
    if (asset || force || !cached) return asset;

    await this._listAllAssets(true);
    return this._assetMap?.get(filename) || null;
  }

  /**
   * List all release assets (paginates through all pages).
   * @returns {Promise<object[]>}
   */
  async _listAllAssets(force = false) {
    if (this._assets && !force) return this._assets;
    if (this._assetPromise && !force) return this._assetPromise;

    this._assetPromise = (async () => {
      const releaseId = await this._getReleaseId();
      const all = [];
      let page = 1;

      while (true) {
        const url = `https://api.github.com/repos/${this.owner}/${this.repo}/releases/${releaseId}/assets?per_page=100&page=${page}`;
        const resp = await this._request(url, { headers: this._headers() }, { retries: 6 });

        const assets = await resp.json();
        if (assets.length === 0) break;

        all.push(...assets);
        if (assets.length < 100) break;
        page++;
      }

      this._setAssets(all);
      return this._assets;
    })();

    try {
      return await this._assetPromise;
    } finally {
      this._assetPromise = null;
    }
  }

  /**
   * Read all local narinfo files and extract the NAR filenames they reference.
   * Narinfo files contain a `URL:` field like `nar/<filename>`.
   * @returns {Promise<Set<string>>}
   */
  async _getReferencedNarFilenames() {
    const narinfoDir = path.join(this.localPath, 'narinfo');
    const referenced = new Set();

    let entries;
    try {
      entries = await fsp.readdir(narinfoDir);
    } catch {
      return referenced;
    }

    for (const entry of entries) {
      if (!entry.endsWith('.narinfo')) continue;
      try {
        const content = await fsp.readFile(path.join(narinfoDir, entry), 'utf8');
        for (const line of content.split('\n')) {
          if (line.startsWith('URL:')) {
            const url = line.slice(4).trim();
            // URL is typically "nar/<filename>" – extract just the filename
            const filename = url.startsWith('nar/') ? url.slice(4) : url;
            if (filename) referenced.add(filename);
          }
        }
      } catch {
        // skip unreadable files
      }
    }

    return referenced;
  }

  /**
   * Remove release assets that are not referenced by any local narinfo file.
   *
   * When `retentionDays` is greater than 0, only orphaned assets whose
   * `created_at` timestamp is older than `retentionDays` days ago are deleted.
   * This avoids removing assets that were just uploaded but whose narinfo
   * has not yet been written or propagated.
   *
   * @param {object} [options]
   * @param {number} [options.retentionDays=0] - grace period in days before
   *   an orphaned asset is deleted.  0 means delete immediately.
   * @returns {Promise<{deleted: string[], kept: string[], referenced: string[]}>}
   */
  async pruneAssets({ retentionDays = 0 } = {}) {
    console.log(`[github-releases] Starting asset pruning (retentionDays=${retentionDays})`);

    const [assets, referenced] = await Promise.all([
      this._listAllAssets(),
      this._getReferencedNarFilenames(),
    ]);

    console.log(`[github-releases] Found ${assets.length} release asset(s), ${referenced.size} referenced NAR filename(s)`);

    const cutoff = retentionDays > 0
      ? new Date(Date.now() - retentionDays * MS_PER_DAY)
      : null;

    const deleted = [];
    const kept = [];
    const referencedNames = [];

    for (const asset of assets) {
      // Skip narinfo assets — they are metadata, not orphan candidates
      if (asset.name.endsWith('.narinfo')) continue;

      if (referenced.has(asset.name)) {
        referencedNames.push(asset.name);
        continue;
      }

      // Asset is orphaned – check retention period
      if (cutoff) {
        const createdAt = new Date(asset.created_at);
        if (createdAt >= cutoff) {
          console.log(`[github-releases] Keeping orphaned asset ${asset.name} (created ${asset.created_at}, within retention window)`);
          kept.push(asset.name);
          continue;
        }
      }

      try {
        await this._deleteAsset(asset);
        deleted.push(asset.name);
      } catch (err) {
        console.error(`[github-releases] Failed to delete asset ${asset.name}: ${err.message}`);
        kept.push(asset.name);
      }
    }

    console.log(`[github-releases] Pruning complete: ${deleted.length} deleted, ${kept.length} kept (orphaned), ${referencedNames.length} referenced`);

    return { deleted, kept, referenced: referencedNames };
  }

  /**
   * Return the public download URL for a NAR file on GitHub Releases.
   * This URL does not require authentication.
   * @param {string} filename
   * @returns {string}
   */
  narDownloadUrl(filename) {
    return `https://github.com/${this.owner}/${this.repo}/releases/download/${encodeURIComponent(this.releaseTag)}/${encodeURIComponent(filename)}`;
  }
}

module.exports = GitHubReleasesStorage;
