// Vercel serverless function: /api/comments
// Комментарии: проверка админом (?comad) и/или кулдаун 12 часов с одного IP.
// Переменные окружения в Vercel:
//   ADMIN_CODE            — код администратора (придумайте сами)
//   COMMENT_AUTO_APPROVE  — "1", чтобы публиковать сразу, без проверки (необязательно)
const crypto = require('crypto');

const URL_ = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const ADMIN = process.env.ADMIN_CODE || '';
const AUTO = process.env.COMMENT_AUTO_APPROVE === '1';
const COOLDOWN = 12 * 60 * 60; // 12 часов
const MAX_TEXT = 300, MAX_NAME = 24, MAX_WAIT = 200, SHOW = 30, ADMIN_LIST = 100;

async function pipe(cmds) {
  const r = await fetch(URL_.replace(/\/$/, '') + '/pipeline', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmds),
  });
  if (!r.ok) throw new Error('redis http ' + r.status);
  return (await r.json()).map((x) => {
    if (x.error) throw new Error(x.error);
    return x.result;
  });
}

function ipOf(req) {
  const h = req.headers || {};
  const raw = h['x-vercel-forwarded-for'] || h['x-forwarded-for'] || h['x-real-ip'] ||
    (req.socket && req.socket.remoteAddress) || '';
  return String(raw).split(',')[0].trim();
}
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');
const hip = (ip) => sha(ip + '|' + (process.env.REP_SALT || 'meepo')).slice(0, 32);
const clean = (s, max) => String(s == null ? '' : s).replace(/[\u0000-\u001f\u007f]+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, max);

function same(a, b) {
  return crypto.timingSafeEqual(Buffer.from(sha(String(a)), 'hex'), Buffer.from(sha(String(b)), 'hex'));
}

async function readBody(req) {
  if (req.body !== undefined && req.body !== null) {
    if (typeof req.body === 'string') { try { return JSON.parse(req.body); } catch (e) { return {}; } }
    return req.body;
  }
  const chunks = [];
  for await (const c of req) chunks.push(c);
  try { return JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}'); } catch (e) { return {}; }
}

function send(res, status, data) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.end(JSON.stringify(data));
}

async function loadList(key, n) {
  const [ids] = await pipe([['LRANGE', key, 0, n - 1]]);
  if (!ids || !ids.length) return [];
  const [vals] = await pipe([['MGET', ...ids.map((i) => 'com:' + i)]]);
  const out = [];
  (vals || []).forEach((v) => { try { if (v) out.push(JSON.parse(v)); } catch (e) {} });
  return out;
}

async function adminCheck(req, ip) {
  if (!ADMIN) return 'off';
  const fk = 'com:fail:' + hip(ip);
  const [f] = await pipe([['GET', fk]]);
  if (Number(f) >= 8) return 'locked';
  if (!same(req.headers['x-admin-code'] || '', ADMIN)) {
    await pipe([['INCR', fk], ['EXPIRE', fk, 900]]);
    return 'bad';
  }
  return 'ok';
}

module.exports = async (req, res) => {
  if (!URL_ || !TOKEN) return send(res, 503, { error: 'storage_not_configured' });
  try {
    const ip = ipOf(req);
    const ck = 'com:cd:' + hip(ip);

    if (req.method === 'GET') {
      const comments = await loadList('com:ok', SHOW);
      const [ttl] = await pipe([['TTL', ck]]);
      return send(res, 200, {
        comments: comments.map((c) => ({ id: c.id, name: c.name, text: c.text, ts: c.ts })),
        retryAfter: Math.max(Number(ttl) || 0, 0),
        auto: AUTO,
      });
    }

    if (req.method !== 'POST') { res.setHeader('Allow', 'GET, POST'); return send(res, 405, { error: 'method_not_allowed' }); }

    const body = await readBody(req);

    // ---- администратор ----
    if (body && body.action) {
      const st = await adminCheck(req, ip);
      if (st === 'off') return send(res, 503, { error: 'admin_not_configured' });
      if (st === 'locked') return send(res, 429, { error: 'locked' });
      if (st === 'bad') return send(res, 401, { error: 'bad_code' });

      const id = String(body.id == null ? '' : body.id);
      if (body.action === 'list') {
        return send(res, 200, { pending: await loadList('com:wait', ADMIN_LIST), approved: await loadList('com:ok', ADMIN_LIST) });
      }
      if (!/^\d{1,12}$/.test(id)) return send(res, 400, { error: 'bad_id' });
      if (body.action === 'approve') {
        const [exists] = await pipe([['EXISTS', 'com:' + id]]);
        if (!exists) return send(res, 404, { error: 'not_found' });
        await pipe([['LREM', 'com:wait', 0, id], ['LREM', 'com:ok', 0, id], ['LPUSH', 'com:ok', id]]);
        return send(res, 200, { ok: true });
      }
      if (body.action === 'reject' || body.action === 'delete') {
        await pipe([['LREM', 'com:wait', 0, id], ['LREM', 'com:ok', 0, id], ['DEL', 'com:' + id]]);
        return send(res, 200, { ok: true });
      }
      return send(res, 400, { error: 'bad_action' });
    }

    // ---- посетитель оставляет комментарий ----
    const text = clean(body && body.text, MAX_TEXT);
    const name = clean(body && body.name, MAX_NAME) || 'аноним';
    if (text.length < 2) return send(res, 400, { error: 'too_short' });

    if (!AUTO) {
      const [waiting] = await pipe([['LLEN', 'com:wait']]);
      if (Number(waiting) >= MAX_WAIT) return send(res, 429, { error: 'busy' });
    }

    const [set] = await pipe([['SET', ck, '1', 'NX', 'EX', COOLDOWN]]);
    if (set !== 'OK') {
      const [ttl] = await pipe([['TTL', ck]]);
      return send(res, 429, { error: 'cooldown', retryAfter: Math.max(Number(ttl) || 0, 0) });
    }
    try {
      const [id] = await pipe([['INCR', 'com:id']]);
      const c = { id: String(id), name, text, ts: Date.now() };
      await pipe([['SET', 'com:' + id, JSON.stringify(c)], ['LPUSH', AUTO ? 'com:ok' : 'com:wait', String(id)]]);
    } catch (e) {
      await pipe([['DEL', ck]]).catch(() => {});
      throw e;
    }
    return send(res, 200, { ok: true, pending: !AUTO, retryAfter: COOLDOWN });
  } catch (e) {
    return send(res, 500, { error: 'server_error' });
  }
};
