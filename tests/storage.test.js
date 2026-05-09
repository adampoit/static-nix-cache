'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const LocalStorage = require('../src/storage/local');
const { Readable } = require('stream');

function makeTempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'static-nix-cache-test-'));
}

async function streamToBuffer(readable) {
  const chunks = [];
  for await (const chunk of readable) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

describe('LocalStorage', () => {
  let tmpDir;
  let storage;

  beforeEach(() => {
    tmpDir = makeTempDir();
    storage = new LocalStorage(tmpDir);
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  test('hasNarinfo returns false for missing entry', async () => {
    expect(await storage.hasNarinfo('deadbeef')).toBe(false);
  });

  test('putNarinfo and getNarinfo round-trip', async () => {
    const content = 'StorePath: /nix/store/deadbeef-example\n';
    await storage.putNarinfo('deadbeef', content);
    expect(await storage.hasNarinfo('deadbeef')).toBe(true);
    expect(await storage.getNarinfo('deadbeef')).toBe(content);
  });

  test('putSigningPublicKey and getSigningPublicKey round-trip', async () => {
    const publicKey = 'cache-1:abc123\n';
    await storage.putSigningPublicKey(publicKey);
    expect(await storage.getSigningPublicKey()).toBe(publicKey);
  });

  test('getSigningPublicKey returns null when marker is missing', async () => {
    expect(await storage.getSigningPublicKey()).toBeNull();
  });

  test('getNarinfo returns null for missing entry', async () => {
    expect(await storage.getNarinfo('missing')).toBeNull();
  });

  test('hasNar returns false for missing file', async () => {
    expect(await storage.hasNar('missing.nar')).toBe(false);
  });

  test('putNarStream and getNarStream round-trip', async () => {
    const data = Buffer.from('fake NAR data for testing');
    const readable = Readable.from(data);
    await storage.putNarStream('test.nar', readable);

    expect(await storage.hasNar('test.nar')).toBe(true);
    const outStream = await storage.getNarStream('test.nar');
    const result = await streamToBuffer(outStream);
    expect(result).toEqual(data);
  });

  test('getNarStream returns null for missing file', async () => {
    expect(await storage.getNarStream('missing.nar')).toBeNull();
  });
});
