'use strict';

module.exports = {
  port: parseInt(process.env.PORT || '8080', 10),

  storeDir: process.env.STORE_DIR || '/nix/store',

  priority: parseInt(process.env.CACHE_PRIORITY || '30', 10),

  // Storage backend: 'local', 's3', or 'github-releases'
  storageBackend: process.env.STORAGE_BACKEND || 'local',

  // Local storage path
  localStoragePath: process.env.LOCAL_STORAGE_PATH || './cache',

  // S3-compatible storage config
  s3: {
    bucket: process.env.S3_BUCKET || '',
    region: process.env.S3_REGION || 'auto',
    endpoint: process.env.S3_ENDPOINT || undefined,
    accessKeyId: process.env.S3_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.S3_SECRET_ACCESS_KEY || '',
    // Set to true to force path-style URLs (needed for some S3-compatible stores)
    forcePathStyle: process.env.S3_FORCE_PATH_STYLE === 'true',
  },

  // GitHub Releases config (used by 'github-releases' storage backend)
  github: {
    token: process.env.GITHUB_TOKEN || '',
    owner: process.env.GITHUB_OWNER || '',
    repo: process.env.GITHUB_REPO || '',
    releaseTag: process.env.GITHUB_RELEASE_TAG || 'nix-cache',
    releaseTags: process.env.GITHUB_RELEASE_TAGS
      ? process.env.GITHUB_RELEASE_TAGS.split(',').map(t => t.trim()).filter(Boolean)
      : [],
    pruneRetentionDays: parseInt(process.env.GITHUB_PRUNE_RETENTION_DAYS || '0', 10),
  },

  // Signing key: '<keyname>:<base64-encoded-ed25519-private-key>'
  // Generate with: nix-store --generate-binary-cache-key <keyname> private.pem public.pem
  signingKey: process.env.SIGNING_KEY || '',

  // Upload secret token for write operations (PUT/POST)
  uploadSecret: process.env.UPLOAD_SECRET || '',
};
