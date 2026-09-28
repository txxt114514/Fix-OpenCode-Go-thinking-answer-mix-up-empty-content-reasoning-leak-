/* =============================================================================
 * 端到端测试：起一个【假上游】，分别模拟情况 A / B，
 * 经过本代理后检查客户端收到的东西是否符合预期。
 *
 * 不联网，纯本地。
 * ========================================================================== */
'use strict';
const http = require('http');
const { spawn } = require('child_process');
const path = require('path');

let pass = 0, fail = 0;
function ok(name, cond, extra) {
  if (cond) { pass++; console.log('  PASS  ' + name); }
  else { fail++; console.log('  FAIL  ' + name + (extra !== undefined ? '   (' + extra + ')' : '')); }
}

/* ------------------------------------------------ 假上游：两种剧本 */

const SCENARIOS = {
  // 情况 A：只有 reasoning，content 永远为空（这就是"要么不输出"）
  A: (res) => {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    const send = (d) => res.write('data: ' + JSON.stringify({
      id: 'chatcmpl-A', object: 'chat.completion.chunk', created: 1, model: 'fake',
      choices: [{ index: 0, delta: d }],
    }) + '\n\n');
    send({ role: 'assistant', content: '', reasoning_content: '' });
    send({ content: '', reasoning_content: '用户问了一个问题。' });
    send({ content: '', reasoning_content: '我需要先用中文回答。' });
    send({ content: '', reasoning_content: '答案是四十二。' });
    res.write('data: ' + JSON.stringify({
      id: 'chatcmpl-A', object: 'chat.completion.chunk', created: 1, model: 'fake',
      choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
    }) + '\n\n');
    res.write('data: [DONE]\n\n');
    res.end();
  },

  // 情况 B：先 reasoning，切到 content 后 reasoning 还在继续（"全输出"）
  B: (res) => {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    const send = (d) => res.write('data: ' + JSON.stringify({
      id: 'chatcmpl-B', object: 'chat.completion.chunk', created: 2, model: 'fake',
      choices: [{ index: 0, delta: d }],
    }) + '\n\n');
    send({ role: 'assistant', content: '', reasoning_content: '' });
    send({ content: '', reasoning_content: 'The user asks something. Let me plan.' });
    send({ content: '', reasoning_content: 'So my job is to triage.' });
    send({ content: '值得做。', reasoning_content: 'still thinking out loud' });
    send({ content: '共四道。', reasoning_content: 'more leaked planning' });
    res.write('data: ' + JSON.stringify({
      id: 'chatcmpl-B', object: 'chat.completion.chunk', created: 2, model: 'fake',
      choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
    }) + '\n\n');
    res.write('data: [DONE]\n\n');
    res.end();
  },

  // 工具调用：必须原样透传，一个字都不许动
  T: (res) => {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write('data: ' + JSON.stringify({
      id: 'chatcmpl-T', object: 'chat.completion.chunk', created: 3, model: 'fake',
      choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: 'call_1', type: 'function', function: { name: 'read', arguments: '{"p":' } }] } }],
    }) + '\n\n');
    res.write('data: ' + JSON.stringify({
      id: 'chatcmpl-T', object: 'chat.completion.chunk', created: 3, model: 'fake',
      choices: [{ index: 0, delta: { tool_calls: [{ index: 0, function: { arguments: '"x"}' } }] } }],
    }) + '\n\n');
    res.write('data: [DONE]\n\n');
    res.end();
  },

  // 正常流：有 reasoning 也有 content，且切换后 reasoning 就停了（不该被改）
  N: (res) => {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    const send = (d) => res.write('data: ' + JSON.stringify({
      id: 'chatcmpl-N', object: 'chat.completion.chunk', created: 4, model: 'fake',
      choices: [{ index: 0, delta: d }],
    }) + '\n\n');
    send({ role: 'assistant', content: '', reasoning_content: '' });
    send({ content: '', reasoning_content: '思考中。' });
    send({ content: '极限', reasoning_content: '' });
    send({ content: '的定义。', reasoning_content: '' });
    res.write('data: [DONE]\n\n');
    res.end();
  },
};

/* ------------------------------------------------------------- 工具 */

function fetchSse(port, scenario) {
  return new Promise((resolve, reject) => {
    const req = http.request({
      host: '127.0.0.1', port, path: '/zen/go/v1/chat/completions', method: 'POST',
      headers: { 'content-type': 'application/json', 'x-scenario': scenario },
    }, (res) => {
      let buf = '';
      res.on('data', (c) => { buf += c.toString('utf8'); });
      res.on('end', () => resolve(buf));
    });
    req.on('error', reject);
    req.end('{"model":"fake","stream":true}');
  });
}

function parseSse(raw) {
  const out = [];
  for (const line of raw.split(/\r?\n/)) {
    if (!line.startsWith('data:')) continue;
    const p = line.slice(5).trim();
    if (p === '[DONE]') { out.push({ done: true }); continue; }
    try { out.push(JSON.parse(p)); } catch (_) { /* ignore */ }
  }
  return out;
}

function join(events, field) {
  let s = '';
  for (const e of events) {
    if (!e.choices) continue;
    for (const c of e.choices) {
      const d = c.delta;
      if (d && typeof d[field] === 'string') s += d[field];
    }
  }
  return s;
}

function waitPort(port, tries) {
  return new Promise((resolve) => {
    let n = 0;
    const tick = () => {
      n++;
      const req = http.request({ host: '127.0.0.1', port, path: '/zen/go/v1/ping', method: 'GET' }, () => resolve(true));
      req.on('error', () => { if (n < tries) setTimeout(tick, 120); else resolve(false); });
      req.end();
    };
    tick();
  });
}

/* -------------------------------------------------------------- 跑 */

(async () => {
  // 假上游
  const upstream = http.createServer((req, res) => {
    const s = req.headers['x-scenario'] || 'N';
    (SCENARIOS[s] || SCENARIOS.N)(res);
  });
  await new Promise((r) => upstream.listen(0, '127.0.0.1', r));
  const upstreamPort = upstream.address().port;
  console.log('fake upstream on 127.0.0.1:' + upstreamPort);

  // 本代理（指向假上游）
  const proxyPort = 18912;
  const child = spawn(process.execPath, [path.join(__dirname, '..', 'server.js')], {
    env: Object.assign({}, process.env, {
      PORT: String(proxyPort),
      UPSTREAM_HOST: '127.0.0.1',
      UPSTREAM_PORT: String(upstreamPort),
      UPSTREAM_TLS: '0',
      RS_BACKFILL: '1',
      RS_SPLIT: '1',
      RS_LOG: path.join(__dirname, 'e2e-proxy.log'),
    }),
    stdio: 'ignore',
  });

  const up = await waitPort(proxyPort, 40);
  ok('代理已启动并监听', up, 'port ' + proxyPort);
  if (!up) { child.kill(); upstream.close(); process.exit(1); }

  console.log('');
  console.log('=== 情况 A：content 全空 -> 应被补发 ===');
  {
    const raw = await fetchSse(proxyPort, 'A');
    const ev = parseSse(raw);
    const content = join(ev, 'content');
    const reasoning = join(ev, 'reasoning_content');
    console.log('  content  = ' + JSON.stringify(content));
    console.log('  reasoning= ' + JSON.stringify(reasoning.slice(0, 60)));
    ok('A: 客户端拿到了正文', content.length > 0);
    ok('A: 正文等于原来的 reasoning', content === '用户问了一个问题。我需要先用中文回答。答案是四十二。', content);
    ok('A: [DONE] 仍在最末', ev.length > 0 && ev[ev.length - 1].done === true);
  }

  console.log('');
  console.log('=== 情况 B：切换后 reasoning 还在吐 -> 应被清掉 ===');
  {
    const raw = await fetchSse(proxyPort, 'B');
    const ev = parseSse(raw);
    const content = join(ev, 'content');
    // 找出 content 首次非空的位置
    let firstContentIdx = -1;
    ev.forEach((e, i) => {
      if (firstContentIdx !== -1 || !e.choices) return;
      for (const c of e.choices) {
        if (c.delta && typeof c.delta.content === 'string' && c.delta.content) firstContentIdx = i;
      }
    });
    let leakedAfter = 0;
    ev.forEach((e, i) => {
      if (i <= firstContentIdx || !e.choices) return;
      for (const c of e.choices) {
        const d = c.delta;
        if (d && typeof d.reasoning_content === 'string' && d.reasoning_content.length > 0) leakedAfter++;
      }
    });
    console.log('  content = ' + JSON.stringify(content));
    console.log('  切换后残留 reasoning 事件数 = ' + leakedAfter);
    ok('B: 正文完整保留', content === '值得做。共四道。', content);
    ok('B: 切换后 reasoning 被清空', leakedAfter === 0, 'leaked=' + leakedAfter);
    ok('B: [DONE] 仍在最末', ev.length > 0 && ev[ev.length - 1].done === true);
  }

  console.log('');
  console.log('=== 工具调用：必须逐字节原样 ===');
  {
    const raw = await fetchSse(proxyPort, 'T');
    const ev = parseSse(raw);
    const tcs = [];
    for (const e of ev) {
      if (!e.choices) continue;
      for (const c of e.choices) {
        if (c.delta && Array.isArray(c.delta.tool_calls)) tcs.push(...c.delta.tool_calls);
      }
    }
    const args = tcs.map((t) => (t.function && t.function.arguments) || '').join('');
    console.log('  拼出的工具参数 = ' + JSON.stringify(args));
    ok('T: 工具调用片段原样', args === '{"p":"x"}', args);
    ok('T: 没有插入任何合成 content', join(ev, 'content') === '');
  }

  console.log('');
  console.log('=== 正常流：不该被改动 ===');
  {
    const raw = await fetchSse(proxyPort, 'N');
    const ev = parseSse(raw);
    ok('N: 正文不变', join(ev, 'content') === '极限的定义。', join(ev, 'content'));
    ok('N: 思考段保留（切换前）', join(ev, 'reasoning_content') === '思考中。', join(ev, 'reasoning_content'));
  }

  child.kill();
  upstream.close();

  console.log('');
  console.log('=== RESULT ===');
  console.log('  PASS ' + pass + '   FAIL ' + fail);
  setTimeout(() => process.exit(fail === 0 ? 0 : 1), 200);
})();
