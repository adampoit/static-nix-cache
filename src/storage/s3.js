'use strict';

const { Readable } = require('stream');
const { S3Client, GetObjectCommand, PutObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const SIGNING_PUBLIC_KEY_MARKER = 'static-nix-cache-signing-public-key';

/**
 * S3-compatible storage backend.
 *
 * Supports AWS S3, Cloudflare R2, Backblaze B2, MinIO, etc.
 *
 * Layout in bucket:
 *   narinfo/<hash>.narinfo
 *   nar/<filename>
 */
class S3Storage {
  constructor({ bucket, region, endpoint, accessKeyId, secretAccessKey, forcePathStyle }) {
    this.bucket = bucket;
    this.client = new S3Client({
      region,
      endpoint,
      forcePathStyle: forcePathStyle || false,
      credentials: {
        accessKeyId,
        secretAccessKey,
      },
    });
  }

  async hasNarinfo(hash) {
    try {
      await this.client.send(new HeadObjectCommand({
        Bucket: this.bucket,
        Key: `narinfo/${hash}.narinfo`,
      }));
      return true;
    } catch (err) {
      if (err.name === 'NotFound' || err.$metadata?.httpStatusCode === 404) return false;
      throw err;
    }
  }

  async getNarinfo(hash) {
    try {
      const resp = await this.client.send(new GetObjectCommand({
        Bucket: this.bucket,
        Key: `narinfo/${hash}.narinfo`,
      }));
      const chunks = [];
      for await (const chunk of resp.Body) {
        chunks.push(chunk);
      }
      return Buffer.concat(chunks).toString('utf8');
    } catch (err) {
      if (err.name === 'NoSuchKey' || err.$metadata?.httpStatusCode === 404) return null;
      throw err;
    }
  }

  async putNarinfo(hash, content) {
    await this.client.send(new PutObjectCommand({
      Bucket: this.bucket,
      Key: `narinfo/${hash}.narinfo`,
      Body: content,
      ContentType: 'text/x-nix-narinfo',
    }));
  }

  async getSigningPublicKey() {
    try {
      const resp = await this.client.send(new GetObjectCommand({
        Bucket: this.bucket,
        Key: SIGNING_PUBLIC_KEY_MARKER,
      }));
      const chunks = [];
      for await (const chunk of resp.Body) {
        chunks.push(chunk);
      }
      return Buffer.concat(chunks).toString('utf8');
    } catch (err) {
      if (err.name === 'NoSuchKey' || err.$metadata?.httpStatusCode === 404) return null;
      throw err;
    }
  }

  async putSigningPublicKey(content) {
    await this.client.send(new PutObjectCommand({
      Bucket: this.bucket,
      Key: SIGNING_PUBLIC_KEY_MARKER,
      Body: content,
      ContentType: 'text/plain',
    }));
  }

  async hasNar(filename) {
    try {
      await this.client.send(new HeadObjectCommand({
        Bucket: this.bucket,
        Key: `nar/${filename}`,
      }));
      return true;
    } catch (err) {
      if (err.name === 'NotFound' || err.$metadata?.httpStatusCode === 404) return false;
      throw err;
    }
  }

  async getNarStream(filename) {
    try {
      const resp = await this.client.send(new GetObjectCommand({
        Bucket: this.bucket,
        Key: `nar/${filename}`,
      }));
      if (resp.Body instanceof Readable) {
        return resp.Body;
      }
      // AWS SDK v3 can return a web ReadableStream in some environments; convert it
      return Readable.from(resp.Body);
    } catch (err) {
      if (err.name === 'NoSuchKey' || err.$metadata?.httpStatusCode === 404) return null;
      throw err;
    }
  }

  async putNarStream(filename, readableStream) {
    await this.client.send(new PutObjectCommand({
      Bucket: this.bucket,
      Key: `nar/${filename}`,
      Body: readableStream,
      ContentType: 'application/x-nix-nar',
    }));
  }
}

module.exports = S3Storage;
