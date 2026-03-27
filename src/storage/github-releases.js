"use strict"

const fs = require("fs")
const fsp = require("fs/promises")
const path = require("path")

const MS_PER_DAY = 24 * 60 * 60 * 1000
const RETRYABLE = new Set([408, 409, 423, 425, 429, 500, 502, 503, 504])

class GitHubReleasesStorage {
  constructor({ token, owner, repo, releaseTag, localPath }) {
    this.token = token
    this.owner = owner
    this.repo = repo
    this.releaseTag = releaseTag
    this.localPath = localPath
    this._release = null
    this._releasep = null
    this._assets = null
    this._assetp = null
    this._assetm = null

    fs.mkdirSync(path.join(this.localPath, "narinfo"), { recursive: true })
  }

  _headers(accept) {
    return {
      Authorization: `token ${this.token}`,
      Accept: accept || "application/vnd.github.v3+json",
      "User-Agent": "static-nix-cache",
      "X-GitHub-Api-Version": "2022-11-28",
    }
  }

  _retryable(resp, body) {
    if (RETRYABLE.has(resp.status)) return true
    if (resp.status !== 403) return false
    if (resp.headers.get("x-ratelimit-remaining") === "0") return true
    return /rate limit/i.test(body)
  }

  _wait(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms))
  }

  _delay(resp, n, body) {
    const retry = Number.parseInt(resp.headers.get("retry-after") || "", 10)
    if (Number.isFinite(retry) && retry > 0) return retry * 1000

    if (this._retryable(resp, body)) {
      const reset = Number.parseInt(resp.headers.get("x-ratelimit-reset") || "", 10)
      if (Number.isFinite(reset) && reset > 0) {
        return Math.max(1000, Math.min(reset * 1000 - Date.now() + 1000, 5 * 60 * 1000))
      }
    }

    return Math.min(30000, 1000 * 2 ** n)
  }

  async _fetch(url, opts, cfg) {
    const method = opts?.method || "GET"
    const allow = new Set(cfg?.allow || [])
    const retries = cfg?.retries || 5

    for (let n = 0; n < retries; n++) {
      let resp
      try {
        resp = await fetch(url, opts)
      } catch (err) {
        if (n + 1 >= retries) throw err
        const ms = Math.min(30000, 1000 * 2 ** n)
        console.warn(
          `[github-releases] ${method} ${url} errored (${err.message}), retrying in ${Math.ceil(ms / 1000)}s`,
        )
        await this._wait(ms)
        continue
      }

      if (resp.ok || allow.has(resp.status)) return resp

      const body = await resp.text()
      if (n + 1 < retries && this._retryable(resp, body)) {
        const ms = this._delay(resp, n, body)
        console.warn(`[github-releases] ${method} ${url} failed (${resp.status}), retrying in ${Math.ceil(ms / 1000)}s`)
        await this._wait(ms)
        continue
      }

      throw new Error(`${method} ${url} failed: ${resp.status} ${body}`)
    }
  }

  _setAsset(list) {
    this._assets = list
    this._assetm = new Map(list.map((asset) => [asset.name, asset]))
  }

  _upsert(asset) {
    if (!this._assets || !this._assetm) {
      this._setAsset([asset])
      return
    }

    const i = this._assets.findIndex((item) => item.name === asset.name)
    if (i === -1) this._assets.push(asset)
    else this._assets[i] = asset
    this._assetm.set(asset.name, asset)
  }

  _drop(name) {
    if (!this._assets || !this._assetm) return
    this._assets = this._assets.filter((asset) => asset.name !== name)
    this._assetm.delete(name)
  }

  async _exists(file) {
    try {
      await fsp.access(file)
      return true
    } catch {
      return false
    }
  }

  async _getRelease(force) {
    if (this._release && !force) return this._release
    if (this._releasep && !force) return this._releasep

    this._releasep = (async () => {
      const tag = `https://api.github.com/repos/${this.owner}/${this.repo}/releases/tags/${encodeURIComponent(this.releaseTag)}`
      console.log(`[github-releases] Looking up release by tag: ${this.releaseTag}`)
      const resp = await this._fetch(tag, { headers: this._headers() }, { allow: [404], retries: 6 })

      if (resp.status !== 404) {
        this._release = await resp.json()
        console.log(`[github-releases] Found existing release id=${this._release.id}`)
        return this._release
      }

      console.log("[github-releases] Release not found, creating new release")
      const create = await this._fetch(
        `https://api.github.com/repos/${this.owner}/${this.repo}/releases`,
        {
          method: "POST",
          headers: { ...this._headers(), "Content-Type": "application/json" },
          body: JSON.stringify({
            tag_name: this.releaseTag,
            name: `Nix Binary Cache (${this.releaseTag})`,
            body: "Nix binary cache NAR files managed by static-nix-cache.",
            draft: false,
            prerelease: false,
          }),
        },
        { allow: [422], retries: 6 },
      )

      if (create.status === 422) {
        console.warn("[github-releases] Release create returned 422, refetching existing release")
        const again = await this._fetch(tag, { headers: this._headers() }, { retries: 6 })
        this._release = await again.json()
        return this._release
      }

      this._release = await create.json()
      console.log(`[github-releases] Created release id=${this._release.id}`)
      return this._release
    })()

    try {
      return await this._releasep
    } finally {
      this._releasep = null
    }
  }

  async _getReleaseId() {
    return (await this._getRelease()).id
  }

  async _listAllAssets(force) {
    if (this._assets && !force) return this._assets
    if (this._assetp && !force) return this._assetp

    this._assetp = (async () => {
      const id = await this._getReleaseId()
      const list = []

      for (let page = 1; ; page++) {
        const resp = await this._fetch(
          `https://api.github.com/repos/${this.owner}/${this.repo}/releases/${id}/assets?per_page=100&page=${page}`,
          { headers: this._headers() },
          { retries: 6 },
        )
        const items = await resp.json()
        if (items.length === 0) break
        list.push(...items)
        if (items.length < 100) break
      }

      this._setAsset(list)
      return this._assets
    })()

    try {
      return await this._assetp
    } finally {
      this._assetp = null
    }
  }

  async _findAsset(name, force) {
    await this._listAllAssets(force)
    return this._assetm?.get(name) || null
  }

  async _deleteAsset(asset) {
    console.log(`[github-releases] Deleting existing asset ${asset.name} (id=${asset.id})`)
    await this._fetch(
      `https://api.github.com/repos/${this.owner}/${this.repo}/releases/assets/${asset.id}`,
      { method: "DELETE", headers: this._headers() },
      { allow: [404], retries: 6 },
    )
    this._drop(asset.name)
  }

  async _uploadAsset(name, body, type, warn) {
    const id = await this._getReleaseId()
    const stale = await this._findAsset(name)
    if (stale) await this._deleteAsset(stale)

    const url = `https://uploads.github.com/repos/${this.owner}/${this.repo}/releases/${id}/assets?name=${encodeURIComponent(name)}`

    for (let n = 0; n < 6; n++) {
      let resp
      try {
        resp = await fetch(url, {
          method: "POST",
          headers: {
            ...this._headers(),
            "Content-Type": type,
            "Content-Length": String(body.length),
          },
          body,
        })
      } catch (err) {
        if (n + 1 >= 6) {
          if (warn) {
            console.warn(`[github-releases] Upload ${name} failed: ${err.message}`)
            return null
          }
          throw err
        }

        const ms = Math.min(30000, 1000 * 2 ** n)
        console.warn(`[github-releases] Upload ${name} errored (${err.message}), retrying in ${Math.ceil(ms / 1000)}s`)
        await this._wait(ms)
        const asset = await this._findAsset(name, true)
        if (asset) await this._deleteAsset(asset)
        continue
      }

      if (resp.ok) {
        const asset = await resp.json()
        this._upsert(asset)
        console.log(`[github-releases] Uploaded asset ${name} successfully`)
        return asset
      }

      const text = await resp.text()
      if (resp.status === 422 && /already exists/i.test(text) && n + 1 < 6) {
        console.warn(`[github-releases] Asset ${name} already exists, refreshing cache before retry`)
        const asset = await this._findAsset(name, true)
        if (asset) await this._deleteAsset(asset)
      } else if (n + 1 >= 6 || !this._retryable(resp, text)) {
        const msg = `[github-releases] Failed to upload asset ${name}: ${resp.status} ${text}`
        if (warn) {
          console.warn(msg)
          return null
        }
        throw new Error(msg)
      }

      const ms = this._delay(resp, n, text)
      console.warn(`[github-releases] Upload ${name} failed (${resp.status}), retrying in ${Math.ceil(ms / 1000)}s`)
      await this._wait(ms)
    }
  }

  async hasNarinfo(hash) {
    return this._exists(path.join(this.localPath, "narinfo", `${hash}.narinfo`))
  }

  async getNarinfo(hash) {
    const file = path.join(this.localPath, "narinfo", `${hash}.narinfo`)
    try {
      return await fsp.readFile(file, "utf8")
    } catch {
      return null
    }
  }

  async putNarinfo(hash, content) {
    console.log(`[github-releases] Storing narinfo ${hash}`)
    await fsp.writeFile(path.join(this.localPath, "narinfo", `${hash}.narinfo`), content, "utf8")
    await this._uploadAsset(`${hash}.narinfo`, Buffer.from(content, "utf8"), "text/plain", true)
  }

  async fetchAllNarinfo() {
    console.log("[github-releases] Fetching all narinfo from release...")
    const assets = (await this._listAllAssets()).filter((asset) => asset.name.endsWith(".narinfo"))
    const dir = path.join(this.localPath, "narinfo")
    let fetched = 0

    for (const asset of assets) {
      const file = path.join(dir, asset.name)
      if (await this._exists(file)) continue

      const resp = await this._fetch(
        asset.url,
        { headers: this._headers("application/octet-stream"), redirect: "follow" },
        { retries: 6 },
      )
      await fsp.writeFile(file, await resp.text(), "utf8")
      fetched++
    }

    console.log(`[github-releases] Fetched ${fetched} narinfo file(s) from release (${assets.length} total on release)`)
    return fetched
  }

  async hasNar(name) {
    return (await this._findAsset(name)) !== null
  }

  async getNarStream(name) {
    const asset = await this._findAsset(name)
    if (!asset) return null

    const resp = await this._fetch(
      asset.url,
      { headers: this._headers("application/octet-stream"), redirect: "follow" },
      { retries: 6 },
    )

    const { Readable } = require("stream")
    return Readable.fromWeb(resp.body)
  }

  async putNarStream(name, input) {
    const chunks = []
    for await (const chunk of input) chunks.push(Buffer.from(chunk))
    const body = Buffer.concat(chunks)
    console.log(`[github-releases] Uploading NAR asset ${name} (${body.length} bytes)`)
    await this._uploadAsset(name, body, "application/octet-stream")
  }

  async _getReferencedNarFilenames() {
    const dir = path.join(this.localPath, "narinfo")
    const refs = new Set()

    let entries
    try {
      entries = await fsp.readdir(dir)
    } catch {
      return refs
    }

    for (const entry of entries) {
      if (!entry.endsWith(".narinfo")) continue
      try {
        const content = await fsp.readFile(path.join(dir, entry), "utf8")
        for (const line of content.split("\n")) {
          if (!line.startsWith("URL:")) continue
          const url = line.slice(4).trim()
          const name = url.startsWith("nar/") ? url.slice(4) : url
          if (name) refs.add(name)
        }
      } catch {}
    }

    return refs
  }

  async pruneAssets({ retentionDays = 0 } = {}) {
    console.log(`[github-releases] Starting asset pruning (retentionDays=${retentionDays})`)
    const [assets, refs] = await Promise.all([this._listAllAssets(true), this._getReferencedNarFilenames()])
    console.log(`[github-releases] Found ${assets.length} release asset(s), ${refs.size} referenced NAR filename(s)`)

    const cutoff = retentionDays > 0 ? new Date(Date.now() - retentionDays * MS_PER_DAY) : null
    const deleted = []
    const kept = []
    const referenced = []

    for (const asset of assets) {
      if (asset.name.endsWith(".narinfo")) continue

      if (refs.has(asset.name)) {
        referenced.push(asset.name)
        continue
      }

      if (cutoff) {
        const created = new Date(asset.created_at)
        if (created >= cutoff) {
          console.log(
            `[github-releases] Keeping orphaned asset ${asset.name} (created ${asset.created_at}, within retention window)`,
          )
          kept.push(asset.name)
          continue
        }
      }

      try {
        await this._deleteAsset(asset)
        deleted.push(asset.name)
      } catch (err) {
        console.error(`[github-releases] Failed to delete asset ${asset.name}: ${err.message}`)
        kept.push(asset.name)
      }
    }

    console.log(
      `[github-releases] Pruning complete: ${deleted.length} deleted, ${kept.length} kept (orphaned), ${referenced.length} referenced`,
    )
    return { deleted, kept, referenced }
  }

  narDownloadUrl(name) {
    return `https://github.com/${this.owner}/${this.repo}/releases/download/${encodeURIComponent(this.releaseTag)}/${encodeURIComponent(name)}`
  }
}

module.exports = GitHubReleasesStorage
