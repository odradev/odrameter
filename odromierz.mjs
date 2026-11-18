#!/usr/bin/env node
// Odromierz — measures how many Casper transactions touch a contract written in Odra.
//
//   node odromierz.mjs                 scan mainnet + testnet (last 30 days), then build site/index.html
//   node odromierz.mjs --window 24h    scan only the last 24h (also: 7d, 30d)
//   node odromierz.mjs --networks testnet --concurrency 4
//   node odromierz.mjs report          rebuild the page from the cache, no network access
//
// Scans are incremental: everything fetched lands in data/<network>/ and is never fetched twice.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const DATA = path.join(ROOT, 'data');
const SITE = path.join(ROOT, 'site');

const NETWORKS = {
  mainnet: {
    rpc: process.env.ODROMIERZ_MAINNET_RPC || 'https://node.mainnet.casper.network/rpc',
    explorer: 'https://cspr.live',
  },
  testnet: {
    rpc: process.env.ODROMIERZ_TESTNET_RPC || 'https://node.testnet.casper.network/rpc',
    explorer: 'https://testnet.cspr.live',
  },
};
// Optional Authorization header, e.g. a CSPR.cloud token when pointing the RPC URLs at node.cspr.cloud.
const AUTH = process.env.ODROMIERZ_AUTH;

const HOUR = 3600e3;
const WINDOWS = { '24h': 24 * HOUR, '7d': 7 * 24 * HOUR, '30d': 30 * 24 * HOUR };
const CHUNK = 240; // block heights per atomic unit of work
// Every contract installed by Odra (0.8+) carries the installer's `odra_cfg_*` argument names in its wasm.
const ODRA_MARKER = Buffer.from('odra_cfg_');
// Lanes 0 (native transfers) and 1 (native auction) never execute user wasm.
const isNativeLane = (lane) => lane === 0 || lane === 1;

// ---------- args ----------

function parseArgs(argv) {
  const opts = { cmd: 'scan', window: '30d', networks: Object.keys(NETWORKS), concurrency: 8 };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === 'scan' || a === 'report') opts.cmd = a;
    else if (a === '--window') opts.window = argv[++i];
    else if (a === '--networks') opts.networks = argv[++i].split(',');
    else if (a === '--concurrency') opts.concurrency = Number(argv[++i]);
    else fail(`Unknown argument: ${a}`);
  }
  if (!WINDOWS[opts.window]) fail(`--window must be one of ${Object.keys(WINDOWS).join(', ')}`);
  for (const n of opts.networks) if (!NETWORKS[n]) fail(`Unknown network: ${n}`);
  if (!(opts.concurrency >= 1)) fail('--concurrency must be a positive number');
  return opts;
}

function fail(msg) {
  console.error(msg);
  process.exit(1);
}

// ---------- RPC ----------

class RpcError extends Error {
  constructor(method, err) {
    super(`${method}: ${err.message}${err.data ? ` (${JSON.stringify(err.data).slice(0, 200)})` : ''}`);
    this.code = err.code;
  }
}

function limiter(max) {
  let active = 0;
  const queue = [];
  const next = () => {
    if (active >= max || !queue.length) return;
    active++;
    const { fn, resolve, reject } = queue.shift();
    fn().then(resolve, reject).finally(() => { active--; next(); });
  };
  return (fn) => new Promise((resolve, reject) => { queue.push({ fn, resolve, reject }); next(); });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function makeRpc(url, concurrency) {
  const limit = limiter(concurrency);
  const headers = { 'Content-Type': 'application/json', ...(AUTH ? { Authorization: AUTH } : {}) };
  return (method, params) => limit(async () => {
    const body = JSON.stringify({ jsonrpc: '2.0', id: 1, method, params });
    for (let attempt = 0; ; attempt++) {
      try {
        const res = await fetch(url, { method: 'POST', headers, body, signal: AbortSignal.timeout(90e3) });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const json = await res.json();
        if (json.error) throw new RpcError(method, json.error);
        return json.result;
      } catch (e) {
        // The public endpoints balance over several nodes, and one that is still syncing answers
        // "not found" for things the others have. So a JSON-RPC error is retried too, just fewer times.
        if (attempt >= (e instanceof RpcError ? 3 : 6)) throw e;
        await sleep(Math.min(30e3, 500 * 2 ** attempt));
      }
    }
  });
}

// ---------- cache files ----------

const netDir = (net) => path.join(DATA, net);
const readJson = (file, fallback) => (fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : fallback);
function writeJson(file, value) {
  fs.writeFileSync(file + '.tmp', JSON.stringify(value));
  fs.renameSync(file + '.tmp', file);
}

/** Non-empty scanned blocks, deduplicated by height (a re-done chunk may have appended twice). */
function loadBlocks(net) {
  const file = path.join(netDir(net), 'blocks.jsonl');
  const byHeight = new Map();
  if (fs.existsSync(file)) {
    for (const line of fs.readFileSync(file, 'utf8').split('\n')) {
      if (!line) continue;
      const b = JSON.parse(line);
      byHeight.set(b.h, b);
    }
  }
  return [...byHeight.values()];
}

// ---------- height intervals ----------

function addInterval(intervals, lo, hi) {
  const all = [...intervals, [lo, hi]].sort((a, b) => a[0] - b[0]);
  const out = [];
  for (const [l, h] of all) {
    const last = out[out.length - 1];
    if (last && l <= last[1] + 1) last[1] = Math.max(last[1], h);
    else out.push([l, h]);
  }
  return out;
}

function covered(intervals, lo, hi) {
  let n = 0;
  for (const [l, h] of intervals) n += Math.max(0, Math.min(h, hi) - Math.max(l, lo) + 1);
  return n;
}

/** Unscanned chunks of [lo, hi], newest first, so the short windows complete early. */
function missingChunks(intervals, lo, hi) {
  const gaps = [];
  let cursor = lo;
  for (const [l, h] of intervals) {
    if (h < cursor) continue;
    if (l > hi) break;
    if (l > cursor) gaps.push([cursor, l - 1]);
    cursor = h + 1;
  }
  if (cursor <= hi) gaps.push([cursor, hi]);
  const chunks = [];
  for (const [l, h] of gaps.reverse()) {
    for (let top = h; top >= l; top -= CHUNK) chunks.push([Math.max(l, top - CHUNK + 1), top]);
  }
  return chunks;
}

// ---------- chain access ----------

function parseBlock(result) {
  const block = result.block_with_signatures.block.Version2;
  if (!block) throw new Error('Pre-2.0 block encountered; this tool only understands Casper 2.x blocks');
  const txs = [];
  for (const [lane, ids] of Object.entries(block.body.transactions || {})) {
    for (const id of ids) txs.push({ id, lane: Number(lane) });
  }
  return { h: block.header.height, t: Date.parse(block.header.timestamp), txs };
}

function scannerFor(net, concurrency) {
  const rpc = makeRpc(NETWORKS[net].rpc, concurrency);
  const dir = netDir(net);
  fs.mkdirSync(dir, { recursive: true });
  const contractsFile = path.join(dir, 'contracts.json');
  const known = readJson(contractsFile, {}); // hash -> classification
  const pending = new Map(); // hash -> Promise, so concurrent txs share one lookup
  let dirty = false;

  const getBlock = async (height) =>
    parseBlock(await rpc('chain_get_block', height == null ? {} : { block_identifier: { Height: height } }));

  /** Height of the first block with timestamp >= ts. */
  async function heightAt(ts, tip) {
    let lo = Math.max(0, tip.h - Math.ceil((tip.t - ts) / 1000)); // blocks are never faster than 1/s
    let hi = tip.h;
    while (lo < hi) {
      // Aim where an 8s cadence would put it, which lands within a few steps on a healthy chain.
      const guess = Math.round(tip.h - (tip.t - ts) / 8000);
      const mid = guess > lo && guess < hi && hi - lo > 64 ? guess : Math.floor((lo + hi) / 2);
      const b = await getBlock(mid);
      if (b.t >= ts) hi = mid; else lo = mid + 1;
    }
    return lo;
  }

  async function queryHash(hash) {
    try {
      return (await rpc('query_global_state', { key: `hash-${hash}`, path: [] })).stored_value;
    } catch (e) {
      if (e instanceof RpcError) return null; // nothing stored under this hash
      throw e;
    }
  }

  /** What lives under `hash-<hash>`: a contract (with its Odra verdict), a package, or something irrelevant. */
  function classify(hash) {
    if (known[hash]) return Promise.resolve(known[hash]);
    if (!pending.has(hash)) {
      pending.set(hash, (async () => {
        const value = await queryHash(hash);
        let result = { k: 'other' };
        if (value?.Contract) {
          const c = value.Contract;
          const wasm = c.contract_wasm_hash.replace('contract-wasm-', '');
          const names = new Set(c.named_keys.map((n) => n.name));
          result = {
            k: 'contract',
            pkg: c.contract_package_hash.replace('contract-package-', ''),
            odra: (await classify(wasm)).odra === true,
            // Named-key fingerprint, kept only to audit the wasm marker against.
            nk: names.has('state') && names.has('__events'),
          };
        } else if (value?.ContractPackage) {
          const versions = value.ContractPackage.versions;
          const latest = versions[versions.length - 1]?.contract_hash.replace('contract-', '');
          result = { k: 'package', latest };
        } else if (value?.ContractWasm) {
          result = { k: 'wasm', odra: Buffer.from(value.ContractWasm.bytes, 'hex').includes(ODRA_MARKER) };
        }
        known[hash] = result;
        dirty = true;
        pending.delete(hash);
        return result;
      })());
    }
    return pending.get(hash);
  }

  async function namedKeyOf(publicKey, name) {
    try {
      const res = await rpc('state_get_account_info', { account_identifier: publicKey });
      const key = res.account.named_keys.find((n) => n.name === name)?.key;
      return key?.startsWith('hash-') ? key.slice(5) : null;
    } catch (e) {
      if (e instanceof RpcError) return null;
      throw e;
    }
  }

  /** Normalises a Deploy or a Version1 transaction into what the classifier needs. */
  function describe(transaction) {
    if (transaction.Deploy) {
      const d = transaction.Deploy;
      const [kind, s] = Object.entries(d.session)[0];
      return {
        initiator: d.header.account,
        hash: kind.endsWith('ByHash') ? s.hash : null,
        name: kind.endsWith('ByName') ? s.name : null,
        moduleBytes: kind === 'ModuleBytes' ? s.module_bytes : null,
      };
    }
    const p = transaction.Version1.payload;
    const stored = p.fields.target?.Stored?.id;
    const byHash = stored?.ByHash ?? stored?.ByPackageHash;
    const byName = stored?.ByName ?? stored?.ByPackageName;
    return {
      initiator: p.initiator_addr.PublicKey ?? null,
      hash: byHash ? (byHash.addr ?? byHash) : null,
      name: byName ? (byName.name ?? byName) : null,
      moduleBytes: p.fields.target?.Session?.module_bytes ?? null,
    };
  }

  function touchedHashes(executionResult) {
    const v2 = executionResult?.Version2;
    const effects = v2 ? v2.effects : executionResult?.Version1?.Success?.effect?.transforms ?? [];
    const hashes = new Set();
    for (const e of effects) if (e.key.startsWith('hash-')) hashes.add(e.key.slice(5));
    const ok = v2 ? v2.error_message == null : Boolean(executionResult?.Version1?.Success);
    return { hashes, ok };
  }

  async function processTx({ id, lane }) {
    const hash = id.Version1 ?? id.Deploy;
    let res;
    try {
      res = await rpc('info_get_transaction', { transaction_hash: id, finalized_approvals: false });
    } catch (e) {
      if (!(e instanceof RpcError)) throw e;
      return { x: hash, l: lane, err: 1 };
    }
    const tx = describe(res.transaction);
    const { hashes, ok } = touchedHashes(res.execution_info?.execution_result);
    // A reverted transaction leaves no trace of the contract in its effects, so start from its target.
    let target = tx.hash;
    if (!target && tx.name && !ok && tx.initiator) target = await namedKeyOf(tx.initiator, tx.name);
    if (target) {
      hashes.add(target);
      const t = await classify(target);
      if (t.k === 'package' && t.latest) hashes.add(t.latest);
    }
    const odraPackages = new Set();
    for (const c of await Promise.all([...hashes].map(classify))) {
      if (c.k === 'contract' && c.odra) odraPackages.add(c.pkg);
    }
    const out = { x: hash, l: lane, ok: ok ? 1 : 0 };
    if (odraPackages.size) out.o = [...odraPackages];
    // Session wasm built by Odra: a contract install or upgrade.
    if (tx.moduleBytes && Buffer.from(tx.moduleBytes, 'hex').includes(ODRA_MARKER)) out.s = 1;
    return out;
  }

  async function scanChunk([lo, hi]) {
    const heights = Array.from({ length: hi - lo + 1 }, (_, i) => lo + i);
    const blocks = await Promise.all(heights.map(getBlock));
    const lines = [];
    await Promise.all(blocks.filter((b) => b.txs.length).map(async (b) => {
      const native = b.txs.filter((t) => isNativeLane(t.lane)).length;
      const tx = await Promise.all(b.txs.filter((t) => !isNativeLane(t.lane)).map(processTx));
      lines.push(JSON.stringify({ h: b.h, t: b.t, n: native, tx }));
    }));
    if (lines.length) fs.appendFileSync(path.join(dir, 'blocks.jsonl'), lines.join('\n') + '\n');
    if (dirty) { writeJson(contractsFile, known); dirty = false; }
    return blocks[0].t;
  }

  return { getBlock, heightAt, scanChunk };
}

/** Two scans sharing a cache overwrite each other's state, so only one may run per network. */
function lockNetwork(net) {
  const file = path.join(ROOT, `.scan-${net}.lock`);
  const owner = Number(fs.existsSync(file) && fs.readFileSync(file, 'utf8'));
  if (owner && owner !== process.pid) {
    let alive = true;
    try { process.kill(owner, 0); } catch { alive = false; }
    if (alive) throw new Error(`another scan is already running (pid ${owner})`);
  }
  fs.writeFileSync(file, String(process.pid));
  process.on('exit', () => fs.rmSync(file, { force: true }));
}

async function scanNetwork(net, opts) {
  lockNetwork(net);
  const s = scannerFor(net, opts.concurrency);
  const stateFile = path.join(netDir(net), 'state.json');
  const state = readJson(stateFile, { intervals: [] });

  const tip = await s.getBlock();
  state.tip = { h: tip.h, t: tip.t };
  state.bounds = {};
  // Every window gets its bound, so the report can say how much of the wider ones earlier runs covered.
  for (const [name, ms] of Object.entries(WINDOWS)) state.bounds[name] = await s.heightAt(tip.t - ms, tip);
  const chunks = missingChunks(state.intervals, state.bounds[opts.window], tip.h);
  const total = chunks.reduce((a, [l, h]) => a + h - l + 1, 0);
  console.log(`[${net}] tip ${tip.h}, ${total} blocks to scan for the ${opts.window} window`);

  let done = 0;
  const started = Date.now();
  let lastLog = 0;
  for (const chunk of chunks) {
    const reached = await s.scanChunk(chunk);
    state.intervals = addInterval(state.intervals, chunk[0], chunk[1]);
    writeJson(stateFile, state);
    done += chunk[1] - chunk[0] + 1;
    if (Date.now() - lastLog > 30e3 || done === total) {
      lastLog = Date.now();
      const rate = done / ((Date.now() - started) / 1000);
      const eta = Math.round((total - done) / rate / 60);
      console.log(`[${net}] ${done}/${total} blocks (${(100 * done / total).toFixed(1)}%), back to ${new Date(reached).toISOString().slice(0, 16)}Z, ${rate.toFixed(0)} blocks/s, ~${eta} min left`);
    }
  }
  // Nothing older than the widest window is ever reported, so drop it to keep the cache bounded.
  if (opts.window === '30d') {
    const cut = state.bounds['30d'];
    const kept = loadBlocks(net).filter((b) => b.h >= cut).sort((a, b) => a.h - b.h);
    const blocksFile = path.join(netDir(net), 'blocks.jsonl');
    fs.writeFileSync(blocksFile + '.tmp', kept.map((b) => JSON.stringify(b) + '\n').join(''));
    fs.renameSync(blocksFile + '.tmp', blocksFile);
    state.intervals = state.intervals.filter(([, h]) => h >= cut).map(([l, h]) => [Math.max(l, cut), h]);
  }
  writeJson(stateFile, state);
}

// ---------- report ----------

const isOdra = (tx) => Boolean(tx.o || tx.s);

function summarize(net) {
  const state = readJson(path.join(netDir(net), 'state.json'), null);
  if (!state?.tip) return null;
  const blocks = loadBlocks(net);
  const now = state.tip.t;

  const windows = {};
  for (const [name, ms] of Object.entries(WINDOWS)) {
    const w = { total: 0, native: 0, contract: 0, odra: 0, odraFailed: 0, unknown: 0, coverage: null };
    for (const b of blocks) {
      if (b.t < now - ms || b.t > now) continue;
      w.native += b.n;
      w.contract += b.tx.length;
      for (const tx of b.tx) {
        if (tx.err) w.unknown++;
        if (isOdra(tx)) { w.odra++; if (!tx.ok) w.odraFailed++; }
      }
    }
    w.total = w.native + w.contract;
    const bound = state.bounds?.[name];
    // Without a bound for this window (never requested), nothing can be said about how much was scanned.
    if (bound != null) w.coverage = covered(state.intervals, bound, state.tip.h) / (state.tip.h - bound + 1);
    windows[name] = w;
  }

  const DAY = 24 * HOUR;
  const firstDay = Math.floor((now - WINDOWS['30d']) / DAY);
  const lastDay = Math.floor(now / DAY);
  const days = Array.from({ length: lastDay - firstDay + 1 }, (_, i) => ({
    day: new Date((firstDay + i) * DAY).toISOString().slice(0, 10), odra: 0, contract: 0, native: 0,
  }));
  const packages = new Map();
  for (const b of blocks) {
    if (b.t < now - WINDOWS['30d'] || b.t > now) continue;
    const d = days[Math.floor(b.t / DAY) - firstDay];
    d.native += b.n;
    for (const tx of b.tx) {
      if (isOdra(tx)) d.odra++; else d.contract++;
      for (const pkg of tx.o ?? []) {
        const p = packages.get(pkg) ?? { pkg, '24h': 0, '7d': 0, '30d': 0 };
        for (const [name, ms] of Object.entries(WINDOWS)) if (b.t >= now - ms) p[name]++;
        packages.set(pkg, p);
      }
    }
  }
  const top = [...packages.values()].sort((a, b) => b['30d'] - a['30d']);
  return {
    tip: state.tip, windows, days, explorer: NETWORKS[net].explorer,
    odraPackages: top.length, top: top.slice(0, 15),
  };
}

function buildSite() {
  const networks = {};
  for (const net of Object.keys(NETWORKS)) {
    const s = summarize(net);
    if (s) networks[net] = s;
  }
  const data = { generatedAt: Date.now(), networks };
  const template = fs.readFileSync(path.join(ROOT, 'template.html'), 'utf8');
  fs.mkdirSync(SITE, { recursive: true });
  // `<` is escaped so no string in the data can close the script element.
  const json = JSON.stringify(data).replace(/</g, '\\u003c');
  fs.writeFileSync(path.join(SITE, 'index.html'), template.replace('/*__DATA__*/null', () => json));
  return data;
}

// ---------- main ----------

const opts = parseArgs(process.argv.slice(2));
if (opts.cmd === 'scan') {
  // Exit through the normal path on Ctrl-C so the lock files are removed.
  for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, () => process.exit(130));
  // Refresh the page while a long backfill runs, so partial results are viewable.
  const timer = setInterval(() => { try { buildSite(); } catch (e) { console.error(`report: ${e.message}`); } }, 120e3);
  const results = await Promise.allSettled(opts.networks.map((n) => scanNetwork(n, opts)));
  clearInterval(timer);
  for (const [i, r] of results.entries()) {
    if (r.status === 'rejected') {
      console.error(`[${opts.networks[i]}] scan stopped: ${r.reason?.message ?? r.reason}`);
      process.exitCode = 1;
    }
  }
}
const data = buildSite();
for (const [net, s] of Object.entries(data.networks)) {
  for (const [name, w] of Object.entries(s.windows)) {
    const cov = w.coverage == null ? 'not scanned' : `${(100 * w.coverage).toFixed(1)}% scanned`;
    console.log(`${net.padEnd(8)} ${name.padEnd(4)} odra ${String(w.odra).padStart(7)} / ${String(w.total).padStart(8)} tx  (${cov})`);
  }
}
console.log(`Page: ${path.join(SITE, 'index.html')}`);
