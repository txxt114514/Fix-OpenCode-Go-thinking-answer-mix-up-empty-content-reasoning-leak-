/* =============================================================================
 * server.js -- 一个极小的反向代理，修 OpenCode Go 的「思考/回答错位」
 *
 * 做什么
 *   把 DSH（或任何 OpenAI 兼容客户端）指向本代理，本代理转发到 Go 网关，
 *   在转发过程中修正两类错位：
 *
 *   情况 A（空正文）: 整条流只有 reasoning_content、content 始终为空。
 *       上游把答案留在思考字段里没吐出来。→ 把 reasoning 提升为 content。
 *
 *   情况 B（混在一起）: content 已经开始，但 reasoning_content 还在继续吐。
 *       客户端会把两段都显示，用户看到不该看到的英文规划。→ 清掉后续 reasoning。
 *
 *   工具调用（永远不碰）: 任何含 tool_calls / function_call 的事件原样透传。
 *
 * 为什么做成独立代理而不是 DSH 插件
 *   DSH 的插件接口只暴露 tools/pre-execute 与 agent/pre-step，没有流式输出钩子，
 *   插件层改不了 LLM 的 SSE。代理层是唯一能改到的地方，而且对客户端零要求。
 *
 * 启动
 *   node server.js                    # 默认 127.0.0.1:8912 -> opencode.ai
 *   PORT=9000 node server.js
 *   然后用环境变量或宿主配置把 baseURL 指过来：
 *     http://127.0.0.1:8912/zen/go/v1
 *
 * 环境变量
 *   PORT                  监听端口（默认 8912）
 *   HOST                  监听地址（默认 127.0.0.1）
 *   UPSTREAM_HOST         上游主机（默认 opencode.ai）
 *   UPSTREAM_PORT         上游端口（默认 443）
 *   UPSTREAM_TLS          设 0 走明文 http（自测用）
 *   RS_MODE               blank | delete  —— 清 reasoning 的方式（默认 blank）
 *   RS_BACKFILL           1 = 开启情况 A 的补发（默认 1）
 *   RS_SPLIT              1 = 开启情况 B 的分离（默认 1）
 *   RS_DEBUG              1 = 打详细日志
 *   RS_LOG                日志文件路径（默认 ./proxy.log）
 * ========================================================================== */

'use strict';

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const { ReasonSplitter, contentToText, hasToolCalls } = require('./lib/reason-split.js');

const PORT = Number(process.env.PORT || 8912);
const HOST = process.env.HOST || '127.0.0.1';
const UPSTREAM_HOST = process.env.UPSTREAM_HOST || 'opencode.ai';
const UPSTREAM_PORT = Number(process.env.UPSTREAM_PORT || 443);
const UPSTREAM_TLS = process.env.UPSTREAM_TLS !== '0';
const DEBUG = process.env.RS_DEBUG === '1';
const LOG_FILE = process.env.RS_LOG || path.join(__dirname, 'proxy.log');

const CFG = {
  backfill: process.env.RS_BACKFILL !== '0',
  split: process.env.RS_SPLIT !== '0',
  mode: process.env.RS_MODE === 'delete' ? 'delete' : 'blank',
};

/* ---------------------------------------------------------------- 日志 */

let logStream = null;
try { logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' }); } catch (_) { logStream = null; }

function logLine(fields) {
  const stamp = new Date().toISOString();
  const parts = Object.keys(fields).map((k) => k + '=' + fields[k]);
  const line = stamp + ' | ' + parts.join(' | ');
  if (logStream) logStream.write(line + '\n');
  if (DEBUG) console.log(line);
}

/* ------------------------------------------------------- 请求体读取 */

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

/* ------------------------------------------------- 响应头处理 */

const HOP_BY_HOP = new Set([
  'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization',
  'te', 'trailer', 'transfer-encoding', 'upgrade', 'content-length',
  'content-encoding',
]);

function forwardHeaders(src) {
  const out = {};
  for (const [k, v] of Object.entries(src)) {
    if (HOP_BY_HOP.has(k.toLowerCase())) continue;
    out[k] = v;
  }
  return out;
}

/* ---------------------------------------------------- 情况 A：补发 */

/**
 * 整条流结束时 content 从未出现，但攒下了 reasoning。
 * 构造一条合成 delta 插在终止事件之前。
 */
function buildBackfillChunk(template, text, isAnthropic) {
  const base = template || {};
  if (isAnthropic) {
    return 'event: content_block_delta\n' +
      'data: ' + JSON.stringify({
        type: 'content_block_delta',
        index: 0,
        delta: { type: 'text_delta', text },
      }) + '\n';
  }
  return 'data: ' + JSON.stringify({
    id: base.id,
    object: base.object || 'chat.completion.chunk',
    created: base.created,
    model: base.model,
    choices: [{ index: 0, delta: { content: text }, finish_reason: null }],
  }) + '\n';
}

/* --------------------------------------------------- SSE 改写器 */

class StreamFixer {
  constructor(write, cfg) {
    this.write = write;                 // (string) => void
    this.cfg = cfg;
    this.buf = '';
    this.splitter = new ReasonSplitter({ enabled: cfg.split, mode: cfg.mode });

    this.contentSeen = 0;
    this.contentChars = 0;
    this.reasoningChars = 0;
    this.toolCalls = 0;
    this.template = null;
    this.isAnthropic = false;
    this.formatLocked = false;
    this.backfilled = false;
    this.done = false;
    this.afterFinish = false;
  }

  push(chunk) {
    if (this.done) return;
    this.buf += chunk;
    let i;
    while ((i = this.buf.indexOf('\n')) !== -1) {
      const line = this.buf.slice(0, i).replace(/\r$/, '');
      this.buf = this.buf.slice(i + 1);
      this.handleLine(line);
    }
  }

  handleLine(line) {
    if (this.done) return;

    // 空行 = 事件分隔，原样
    if (line === '') { this.write('\n'); return; }

    // 非 data 行：记录 Anthropic 的 message_stop，然后原样
    if (!line.startsWith('data:')) {
      if (/^event:\s*message_stop\s*$/.test(line)) this.beforeFinish();
      this.write(line + '\n');
      return;
    }

    const payload = line.slice(5).trim();

    if (payload === '[DONE]') {
      this.beforeFinish();
      this.write(line + '\n');
      this.done = true;
      return;
    }

    let obj;
    try { obj = JSON.parse(payload); }
    catch (_) { this.write(line + '\n'); return; }

    this.observe(obj);

    const finishing = Array.isArray(obj.choices) && obj.choices.some((c) => c && c.finish_reason);
    if (finishing) this.beforeFinish();

    // 情况 B：分离（原地修改 obj）
    if (this.cfg.split) {
      this.splitter.process(obj);
    }

    this.write('data: ' + JSON.stringify(obj) + '\n');
  }

  /** 统计 + 锁定格式 + 记模板 */
  observe(obj) {
    if (!obj || typeof obj !== 'object') return;

    if (!this.formatLocked) {
      if (Array.isArray(obj.choices)) { this.isAnthropic = false; this.formatLocked = true; }
      else if (typeof obj.type === 'string' &&
        /^(message_start|message_delta|message_stop|content_block_start|content_block_delta|content_block_stop|ping)$/.test(obj.type)) {
        this.isAnthropic = true; this.formatLocked = true;
      }
    }

    if (!this.template && (obj.id || obj.model)) {
      this.template = { id: obj.id, object: obj.object, created: obj.created, model: obj.model };
    }

    if (Array.isArray(obj.choices)) {
      for (const c of obj.choices) {
        const d = c && c.delta;
        if (d) {
          const text = contentToText(d.content);
          if (text) { this.contentSeen++; this.contentChars += text.length; }
          const r = typeof d.reasoning_content === 'string' ? d.reasoning_content
            : (typeof d.reasoning === 'string' ? d.reasoning : '');
          if (r) this.reasoningChars += r.length;
          if (hasToolCalls(d)) this.toolCalls++;
        }
      }
    }

    if (obj.type === 'content_block_delta' && obj.delta) {
      if (obj.delta.type === 'text_delta' && typeof obj.delta.text === 'string' && obj.delta.text) {
        this.contentSeen++; this.contentChars += obj.delta.text.length;
      }
      if (obj.delta.type === 'thinking_delta' && typeof obj.delta.thinking === 'string') {
        this.reasoningChars += obj.delta.thinking.length;
      }
    }
  }

  /** 在终止事件之前：情况 A 补发 */
  beforeFinish() {
    if (this.afterFinish) return;
    this.afterFinish = true;
    if (!this.cfg.backfill) return;
    if (this.toolCalls > 0) return;              // 工具调用流，别插手
    const text = this.splitter.takeBackfillText();
    if (!text) return;
    this.write(buildBackfillChunk(this.template, text, this.isAnthropic) + '\n');
    this.backfilled = true;
  }

  stats() {
    return {
      content_seen: this.contentSeen,
      content_chars: this.contentChars,
      reasoning_chars: this.reasoningChars,
      tool_calls: this.toolCalls,
      backfilled: this.backfilled,
      split_cleared: this.splitter.clearedReasoningEvents,
      phase: this.splitter.phase,
    };
  }
}

/* ------------------------------------------------------------ 主服务 */

const server = http.createServer(async (req, res) => {
  const started = Date.now();

  // 只代理 API 路径，其余直接 404，避免变成开放代理
  if (!req.url || !req.url.startsWith('/zen/go/')) {
    res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
    res.end('dsh-go-reasoning-splitter: only /zen/go/* is proxied\n');
    return;
  }

  let body;
  try { body = await readBody(req); }
  catch (e) {
    res.writeHead(400); res.end('body read failed'); return;
  }

  const headers = forwardHeaders(req.headers);
  headers['content-length'] = Buffer.byteLength(body);
  headers.host = UPSTREAM_HOST;

  const mod = UPSTREAM_TLS ? https : http;
  const upstreamReq = mod.request({
    host: UPSTREAM_HOST,
    port: UPSTREAM_PORT,
    path: req.url,
    method: req.method,
    headers,
  }, (up) => {
    const ctype = String(up.headers['content-type'] || '');
    const isSse = ctype.includes('text/event-stream');

    const outHeaders = forwardHeaders(up.headers);
    // 我们要逐行看 SSE，所以不能让上游压缩
    delete outHeaders['content-encoding'];

    if (!isSse) {
      // 非流式：原样回传，不动一个字节
      res.writeHead(up.statusCode, outHeaders);
      up.pipe(res);
      logLine({ path: req.url, mode: 'passthrough', status: up.statusCode, ms: Date.now() - started });
      return;
    }

    res.writeHead(up.statusCode, outHeaders);
    const fixer = new StreamFixer((s) => { if (!res.writableEnded) res.write(s); }, CFG);
    up.setEncoding('utf8');
    up.on('data', (c) => fixer.push(c));
    up.on('end', () => {
      const st = fixer.stats();
      logLine(Object.assign({ path: req.url, mode: 'sse', status: up.statusCode, ms: Date.now() - started }, st));
      if (!res.writableEnded) res.end();
    });
    up.on('error', () => { if (!res.writableEnded) res.end(); });
  });

  upstreamReq.on('error', (e) => {
    logLine({ path: req.url, mode: 'error', error: e.message });
    if (!res.headersSent) { res.writeHead(502, { 'content-type': 'text/plain' }); }
    if (!res.writableEnded) res.end('upstream error: ' + e.message);
  });

  upstreamReq.end(body);
});

server.listen(PORT, HOST, () => {
  logLine({ mode: 'start', listen: HOST + ':' + PORT, upstream: UPSTREAM_HOST + ':' + UPSTREAM_PORT, tls: UPSTREAM_TLS, backfill: CFG.backfill, split: CFG.split, strip: CFG.mode });
  console.log('dsh-go-reasoning-splitter listening on http://' + HOST + ':' + PORT);
  console.log('  point your client at: http://' + HOST + ':' + PORT + '/zen/go/v1');
  console.log('  upstream: ' + (UPSTREAM_TLS ? 'https' : 'http') + '://' + UPSTREAM_HOST + ':' + UPSTREAM_PORT);
  console.log('  backfill(A)=' + CFG.backfill + '  split(B)=' + CFG.split + '  strip=' + CFG.mode);
});
