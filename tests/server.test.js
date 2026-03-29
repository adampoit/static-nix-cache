'use strict';

const request = require('supertest');
const crypto = require('crypto');
const { createApp } = require('../src/server');

// ─── In-memory storage mock ───────────────────────────────────────────────────
const { Readable } = require('stream');

class MemoryStorage {
  constructor() {
    this.narinfos = new Map();
    this.nars = new Map();
  }
  async hasNarinfo(hash) { return this.narinfos.has(hash); }
  async getNarinfo(hash) { return this.narinfos.get(hash) ?? null; }
  async putNarinfo(hash, content) { this.narinfos.set(hash, content); }
  async hasNar(filename) { return this.nars.has(filename); }
  async getNarStream(filename) {
    if (!this.nars.has(filename)) return null;
    return Readable.from(this.nars.get(filename));
  }
  async putNarStream(filename, readable) {
    const chunks = [];
    for await (const chunk of readable) chunks.push(Buffer.from(chunk));
    this.nars.set(filename, Buffer.concat(chunks));
  }
}

class RedirectStorage extends MemoryStorage {
  narDownloadUrl(filename) {
    return `https://example.com/releases/${filename}`;
  }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function makeApp(overrides = {}) {
  return createApp({ _storage: new MemoryStorage(), ...overrides });
}

function generateTestKeyPair() {
  const { privateKey, publicKey } = crypto.generateKeyPairSync('ed25519');
  const privDer = privateKey.export({ type: 'pkcs8', format: 'der' });
  const seed = privDer.slice(16);
  const pubDer = publicKey.export({ type: 'spki', format: 'der' });
  const pubBytes = pubDer.slice(12);
  return {
    nixPrivKey: `test-1:${Buffer.concat([seed, pubBytes]).toString('base64')}`,
    nixPubKey: `test-1:${pubBytes.toString('base64')}`,
  };
}

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('GET /nix-cache-info', () => {
  test('returns cache info', async () => {
    const app = makeApp({ storeDir: '/nix/store', priority: 30 });
    const res = await request(app).get('/nix-cache-info');
    expect(res.status).toBe(200);
    expect(res.text).toContain('StoreDir: /nix/store');
    expect(res.text).toContain('WantMassQuery: 1');
    expect(res.text).toContain('Priority: 30');
  });
});

describe('narinfo endpoints', () => {
  const hash = 'aaaabbbbccccdddd';
  const narinfo = 'StorePath: /nix/store/aaaabbbbccccdddd-pkg\nNarHash: sha256:abc\nNarSize: 100\nReferences: \n';

  test('HEAD /:hash.narinfo – 404 when missing', async () => {
    const app = makeApp();
    const res = await request(app).head(`/${hash}.narinfo`);
    expect(res.status).toBe(404);
  });

  test('GET /:hash.narinfo – 404 when missing', async () => {
    const app = makeApp();
    const res = await request(app).get(`/${hash}.narinfo`);
    expect(res.status).toBe(404);
  });

  test('PUT /:hash.narinfo – 401 without auth when secret is set', async () => {
    const app = makeApp({ uploadSecret: 'secret123' });
    const res = await request(app)
      .put(`/${hash}.narinfo`)
      .set('Content-Type', 'text/plain')
      .send(narinfo);
    expect(res.status).toBe(401);
  });

  test('PUT → HEAD → GET round-trip', async () => {
    const app = makeApp();
    await request(app)
      .put(`/${hash}.narinfo`)
      .set('Content-Type', 'text/plain')
      .send(narinfo)
      .expect(200);

    await request(app).head(`/${hash}.narinfo`).expect(200);

    const res = await request(app).get(`/${hash}.narinfo`);
    expect(res.status).toBe(200);
    expect(res.text).toContain('StorePath: /nix/store/aaaabbbbccccdddd-pkg');
  });

  test('PUT with auth succeeds when secret matches', async () => {
    const app = makeApp({ uploadSecret: 'mysecret' });
    await request(app)
      .put(`/${hash}.narinfo`)
      .set('Authorization', 'Bearer mysecret')
      .set('Content-Type', 'text/plain')
      .send(narinfo)
      .expect(200);
  });

  test('PUT adds signature when signing key is set', async () => {
    const { nixPrivKey } = generateTestKeyPair();
    const app = makeApp({ signingKey: nixPrivKey });
    await request(app)
      .put(`/${hash}.narinfo`)
      .set('Content-Type', 'text/plain')
      .send(narinfo)
      .expect(200);

    const res = await request(app).get(`/${hash}.narinfo`);
    expect(res.text).toMatch(/^Sig: test-1:/m);
  });
});

describe('NAR endpoints', () => {
  const filename = 'aaabbbccc.nar';
  const narData = Buffer.from('fake nar data');

  test('GET /nar/:filename – 404 when missing', async () => {
    const app = makeApp();
    const res = await request(app).get(`/nar/${filename}`);
    expect(res.status).toBe(404);
  });

  test('PUT /nar/:filename – 401 without auth when secret is set', async () => {
    const app = makeApp({ uploadSecret: 'secret' });
    const res = await request(app)
      .put(`/nar/${filename}`)
      .send(narData);
    expect(res.status).toBe(401);
  });

  test('PUT → GET round-trip', async () => {
    const app = makeApp();
    await request(app)
      .put(`/nar/${filename}`)
      .set('Content-Type', 'application/octet-stream')
      .send(narData)
      .expect(200);

    const res = await request(app).get(`/nar/${filename}`).buffer(true).parse((res, cb) => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => cb(null, Buffer.concat(chunks)));
    });
    expect(res.status).toBe(200);
    expect(res.body).toEqual(narData);
  });

  test('GET /nar/:filename redirects when storage exposes public URLs', async () => {
    const storage = new RedirectStorage();
    const app = createApp({ _storage: storage });

    const res = await request(app).get(`/nar/${filename}`).redirects(0);
    expect(res.status).toBe(302);
    expect(res.headers.location).toBe(`https://example.com/releases/${filename}`);
  });
});
