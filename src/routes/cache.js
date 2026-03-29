'use strict';

const express = require('express');
const { requireUploadSecret } = require('../middleware/auth');
const { signNarinfo } = require('../signing');

/**
 * Build the Express router for the Nix binary cache HTTP API.
 *
 * Endpoints implemented:
 *   GET  /nix-cache-info              - cache metadata
 *   HEAD /:hash.narinfo               - check presence of a store path
 *   GET  /:hash.narinfo               - fetch narinfo for a store path
 *   PUT  /:hash.narinfo               - upload narinfo
 *   GET  /nar/:filename               - download NAR file
 *   PUT  /nar/:filename               - upload NAR file
 *
 * @param {object} storage - storage backend instance
 * @param {object} config  - application config
 * @returns {express.Router}
 */
function createCacheRouter(storage, config) {
  const router = express.Router();
  const authMiddleware = requireUploadSecret(config.uploadSecret);
  const fail = (msg, err, res) => {
    console.error(`[static-nix-cache] ${msg}:`, err?.stack || err?.message || err);
    res.sendStatus(500);
  };

  // ---------------------------------------------------------------------------
  // GET /nix-cache-info
  // ---------------------------------------------------------------------------
  router.get('/nix-cache-info', (req, res) => {
    res.type('text/plain');
    res.send(
      `StoreDir: ${config.storeDir}\n` +
      `WantMassQuery: 1\n` +
      `Priority: ${config.priority}\n`
    );
  });

  // ---------------------------------------------------------------------------
  // HEAD /:hash.narinfo  – used by Nix to check cache presence
  // ---------------------------------------------------------------------------
  router.head('/:hash.narinfo', async (req, res) => {
    try {
      const exists = await storage.hasNarinfo(req.params.hash);
      res.sendStatus(exists ? 200 : 404);
    } catch (err) {
      fail(`HEAD /${req.params.hash}.narinfo`, err, res);
    }
  });

  // ---------------------------------------------------------------------------
  // GET /:hash.narinfo
  // ---------------------------------------------------------------------------
  router.get('/:hash.narinfo', async (req, res) => {
    try {
      const content = await storage.getNarinfo(req.params.hash);
      if (content === null) {
        return res.sendStatus(404);
      }
      res.type('text/x-nix-narinfo');
      res.send(content);
    } catch (err) {
      fail(`GET /${req.params.hash}.narinfo`, err, res);
    }
  });

  // ---------------------------------------------------------------------------
  // PUT /:hash.narinfo  – upload narinfo (optionally re-sign with our key)
  // ---------------------------------------------------------------------------
  router.put('/:hash.narinfo', authMiddleware, express.text({ type: '*/*', limit: '1mb' }), async (req, res) => {
    try {
      let content = req.body;
      if (typeof content !== 'string' || !content.trim()) {
        return res.status(400).json({ error: 'Empty body' });
      }

      if (config.signingKey) {
        content = addSignature(content, config.signingKey);
      }

      await storage.putNarinfo(req.params.hash, content);
      res.sendStatus(200);
    } catch (err) {
      fail(`PUT /${req.params.hash}.narinfo`, err, res);
    }
  });

  // ---------------------------------------------------------------------------
  // GET /nar/:filename  – download a NAR file
  // ---------------------------------------------------------------------------
  router.get('/nar/:filename', async (req, res) => {
    try {
      if (typeof storage.narDownloadUrl === 'function') {
        return res.redirect(302, storage.narDownloadUrl(req.params.filename));
      }

      const narStream = await storage.getNarStream(req.params.filename);
      if (narStream === null) {
        return res.sendStatus(404);
      }
      res.type('application/x-nix-nar');
      narStream.pipe(res);
      narStream.on('error', () => res.destroy());
    } catch (err) {
      fail(`GET /nar/${req.params.filename}`, err, res);
    }
  });

  // ---------------------------------------------------------------------------
  // PUT /nar/:filename  – upload a NAR file
  // ---------------------------------------------------------------------------
  router.put('/nar/:filename', authMiddleware, async (req, res) => {
    try {
      await storage.putNarStream(req.params.filename, req);
      res.sendStatus(200);
    } catch (err) {
      fail(`PUT /nar/${req.params.filename}`, err, res);
    }
  });

  return router;
}

/**
 * Parse a narinfo text body into an object for signing.
 * @param {string} text
 * @returns {object}
 */
function parseNarinfo(text) {
  const obj = {};
  const references = [];
  for (const line of text.split('\n')) {
    const idx = line.indexOf(':');
    if (idx === -1) continue;
    const key = line.slice(0, idx).trim();
    const value = line.slice(idx + 1).trim();
    if (key === 'StorePath') obj.storePath = value;
    else if (key === 'NarHash') obj.narHash = value;
    else if (key === 'NarSize') obj.narSize = parseInt(value, 10);
    else if (key === 'References') {
      references.push(...value.split(/\s+/).filter(Boolean));
    }
  }
  obj.references = references;
  return obj;
}

/**
 * Add (or replace) our signature in a narinfo text.
 * @param {string} content      - raw narinfo text
 * @param {string} signingKey   - "<keyname>:<base64-private-key>"
 * @returns {string}
 */
function addSignature(content, signingKey) {
  const narinfo = parseNarinfo(content);
  const sig = signNarinfo(narinfo, signingKey);

  // Remove any existing Sig lines, then append ours
  const lines = content.split('\n').filter(l => !l.startsWith('Sig:'));
  // Ensure no trailing blank line before we add ours
  const trimmed = lines.join('\n').trimEnd();
  return `${trimmed}\nSig: ${sig}\n`;
}

module.exports = { createCacheRouter };
