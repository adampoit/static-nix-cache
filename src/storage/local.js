'use strict';

const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const stream = require('stream');
const { promisify } = require('util');

const pipeline = promisify(stream.pipeline);
const SIGNING_PUBLIC_KEY_MARKER = 'static-nix-cache-signing-public-key';

/**
 * Local filesystem storage backend.
 *
 * Layout on disk:
 *   <root>/narinfo/<hash>.narinfo
 *   <root>/nar/<filename>
 */
class LocalStorage {
  constructor(rootPath) {
    this.root = rootPath;
    this._init();
  }

  _init() {
    fs.mkdirSync(path.join(this.root, 'narinfo'), { recursive: true });
    fs.mkdirSync(path.join(this.root, 'nar'), { recursive: true });
  }

  async _exists(filePath) {
    try {
      await fsp.access(filePath);
      return true;
    } catch {
      return false;
    }
  }

  async hasNarinfo(hash) {
    return this._exists(path.join(this.root, 'narinfo', `${hash}.narinfo`));
  }

  async getNarinfo(hash) {
    const filePath = path.join(this.root, 'narinfo', `${hash}.narinfo`);
    try {
      return await fsp.readFile(filePath, 'utf8');
    } catch {
      return null;
    }
  }

  async putNarinfo(hash, content) {
    await fsp.writeFile(path.join(this.root, 'narinfo', `${hash}.narinfo`), content, 'utf8');
  }

  async getSigningPublicKey() {
    try {
      return await fsp.readFile(path.join(this.root, SIGNING_PUBLIC_KEY_MARKER), 'utf8');
    } catch {
      return null;
    }
  }

  async putSigningPublicKey(content) {
    await fsp.writeFile(path.join(this.root, SIGNING_PUBLIC_KEY_MARKER), content, 'utf8');
  }

  async hasNar(filename) {
    return this._exists(path.join(this.root, 'nar', filename));
  }

  async getNarStream(filename) {
    const filePath = path.join(this.root, 'nar', filename);
    if (!(await this._exists(filePath))) return null;
    return fs.createReadStream(filePath);
  }

  async putNarStream(filename, readableStream) {
    const filePath = path.join(this.root, 'nar', filename);
    const writeStream = fs.createWriteStream(filePath);
    await pipeline(readableStream, writeStream);
  }
}

module.exports = LocalStorage;
