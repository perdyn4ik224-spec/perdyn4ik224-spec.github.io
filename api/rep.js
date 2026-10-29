// Vercel serverless function: /api/rep
// Хранит лайки/дизлайки в Upstash Redis и пускает один голос раз в 6 часов с одного IP.
const crypto = require('crypto');

const COOLDOWN = 6 * 60 * 60; // секунды
const URL_ = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;

async function pipe(cmds) {
  const r = await fetch(URL_.replace(/\/$/, '') + '/pipeline', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmds),
  });
  if (!r.ok) throw new Error('redis http ' + r.status);
  const out = await r.json();
  return out.map((x) => {
    if (x.error) throw new Error(x.error);
    return x.result;
  });
}

function ipOf(req) {
  const h = req.headers || {};
  const raw =
    h['x-vercel-forwarded-for'] ||
    h['x-forwarded-for'] ||
    h['x-real-ip'] ||
    (req.socket && req.socket.remoteAddress) ||
    '';
  return String(raw).split(',')[0].trim();
}

function cooldownKey(ip) {
  const salt = process.env.REP_SALT || 'meepo';
  return 'rep:cd:' + crypto.createHash('sha256').update(ip + '|' + salt).digest('hex').slice(0, 32);
}

async function readBody(req) {
  if (req.body !== undefined && req.body !== null) {
    if (typeof req.body === 'string') {
      try { return JSON.parse(req.body); } catch (e) { return {}; }
    }
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

async function state(k) {
  const [likes, dislikes, mine, ttl] = await pipe([
    ['GET', 'rep:likes'],
    ['GET', 'rep:dislikes'],
    ['GET', k],
    ['TTL', k],
  ]);
  return {
    likes: Number(likes) || 0,
    dislikes: Number(dislikes) || 0,
    mine: mine || null,
    retryAfter: mine ? Math.max(Number(ttl) || 0, 0) : 0,
  };
}

module.exports = async (req, res) => {
  if (!URL_ || !TOKEN) return send(res, 503, { error: 'storage_not_configured' });
  try {
    const k = cooldownKey(ipOf(req));

    if (req.method === 'POST') {
      const body = await readBody(req);
      const v = body && body.v;
      if (v !== 'like' && v !== 'dislike') return send(res, 400, { error: 'bad_vote' });

      const [set] = await pipe([['SET', k, v, 'NX', 'EX', COOLDOWN]]);
      if (set !== 'OK') return send(res, 429, { error: 'cooldown', ...(await state(k)) });

      try {
        await pipe([['INCR', v === 'like' ? 'rep:likes' : 'rep:dislikes']]);
      } catch (e) {
        await pipe([['DEL', k]]).catch(() => {});
        throw e;
      }
      return send(res, 200, await state(k));
    }

    if (req.method === 'GET') return send(res, 200, await state(k));

    res.setHeader('Allow', 'GET, POST');
    return send(res, 405, { error: 'method_not_allowed' });
  } catch (e) {
    return send(res, 500, { error: 'server_error' });
  }
};
