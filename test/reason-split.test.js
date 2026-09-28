/* =============================================================================
 * 回归测试：用【真实抓包】的 SSE 流验证分离器。
 * 数据来源：signal-probe-raw.txt（本机对 opencode.ai 的真实流式请求）
 * ========================================================================== */
'use strict';
const fs = require('fs');
const path = require('path');
const { ReasonSplitter, contentToText, hasToolCalls } = require('../lib/reason-split.js');

let pass = 0, fail = 0;
function ok(name, cond, extra) {
  if (cond) { pass++; console.log('  PASS  ' + name); }
  else { fail++; console.log('  FAIL  ' + name + (extra !== undefined ? '   (' + extra + ')' : '')); }
}

const RAW = process.argv[2] || path.join(__dirname, 'signal-probe-raw.txt');

console.log('=== 0. 加载真实抓包 ===');
const haveRaw = fs.existsSync(RAW);
let objs = [];
if (haveRaw) {
  const lines = fs.readFileSync(RAW, 'utf8').split(/\r?\n/).filter((l) => l.startsWith('data:'));
  for (const l of lines) {
    const p = l.slice(5).trim();
    if (p === '[DONE]') continue;
    try { objs.push(JSON.parse(p)); } catch (e) { /* skip */ }
  }
}
console.log('  文件: ' + RAW);
console.log('  解析出的 data 事件: ' + objs.length);
ok('真实抓包可用', haveRaw && objs.length > 10, 'events=' + objs.length);

if (haveRaw && objs.length > 10) {
  console.log('');
  console.log('=== 1. 原始流：确认思考/回答确实分在两个字段 ===');
  const firstReason = objs.findIndex((o) => (o.choices || []).some((c) => c.delta && c.delta.reasoning_content));
  const firstContent = objs.findIndex((o) => (o.choices || []).some((c) => c.delta && contentToText(c.delta.content)));
  console.log('  首个 reasoning 事件 #' + firstReason);
  console.log('  首个 content   事件 #' + firstContent);
  ok('先思考后回答', firstReason >= 0 && firstContent > firstReason);
  ok('切换发生在 #73 附近', firstContent >= 70 && firstContent <= 76, 'got ' + firstContent);

  const beforeSwitch = objs[firstContent - 1];
  const atSwitch = objs[firstContent];
  const rBefore = (beforeSwitch.choices[0].delta || {}).reasoning_content;
  const cBefore = contentToText((beforeSwitch.choices[0].delta || {}).content);
  const rAt = (atSwitch.choices[0].delta || {}).reasoning_content;
  const cAt = contentToText((atSwitch.choices[0].delta || {}).content);
  console.log('  切换前一条: content=' + JSON.stringify(cBefore) + '  reasoning=' + JSON.stringify(rBefore));
  console.log('  切换那一条: content=' + JSON.stringify(cAt) + '  reasoning=' + JSON.stringify(rAt));
  ok('切换前 content 为空', cBefore === '');
  ok('切换前 reasoning 非空', rBefore.length > 0);
  ok('切换时 content 非空', cAt.length > 0);
  ok('切换时 reasoning 变空串', rAt === '');

  console.log('');
  console.log('=== 2. 分离器处理后：回答开始后不应再有任何 reasoning ===');
  const sp = new ReasonSplitter({ enabled: true });
  objs.forEach((o) => sp.process(o));
  const st = sp.stats();
  console.log('  统计: ' + JSON.stringify(st));
  ok('阶段到达 answering', st.phase === 'answering');
  ok('切换点记录正确', st.switchedAt === firstContent, 'got ' + st.switchedAt + ' expect ' + firstContent);

  // 检查处理后的流：切换之后 reasoning 必须全空
  let violation = 0;
  objs.forEach((o, i) => {
    if (i < firstContent) return;
    for (const c of (o.choices || [])) {
      const d = c.delta;
      if (!d || hasToolCalls(d)) continue;
      const r = d.reasoning_content;
      if (typeof r === 'string' && r.length > 0) violation++;
      if (typeof d.reasoning === 'string' && d.reasoning.length > 0) violation++;
    }
  });
  ok('切换后无残留 reasoning', violation === 0, 'violations=' + violation);

  console.log('');
  console.log('=== 3. content 一个字符都不能变 ===');
  const sp2 = new ReasonSplitter({ enabled: true });
  const before = objs.map((o) => (o.choices || []).map((c) => contentToText(c.delta && c.delta.content)).join(''));
  sp2.process({ choices: [] }); // no-op
  const fresh = JSON.parse(JSON.stringify(objs));
  fresh.forEach((o) => sp2.process(o));
  const after = fresh.map((o) => (o.choices || []).map((c) => contentToText(c.delta && c.delta.content)).join(''));
  ok('每条事件的 content 完全一致', JSON.stringify(before) === JSON.stringify(after));
  const joinedBefore = before.join('');
  const joinedAfter = after.join('');
  console.log('  原文长度 ' + joinedBefore.length + ' 处理后 ' + joinedAfter.length);
  ok('全文内容一致', joinedBefore === joinedAfter);
}

console.log('');
console.log('=== 4. 工具调用必须原样放行 ===');
{
  const toolEvent = {
    choices: [{ delta: { tool_calls: [{ index: 0, id: 'x', function: { name: 'f', arguments: '{}' } }], reasoning_content: 'thinking...' } }],
  };
  const sp = new ReasonSplitter({ enabled: true });
  sp.process({ choices: [{ delta: { content: 'hello' } }] });   // 先进入 answering
  const r = sp.process(toolEvent);
  ok('含 tool_calls 的事件未改动', r.changed === false);
  ok('其 reasoning 保持原样', toolEvent.choices[0].delta.reasoning_content === 'thinking...');
}

console.log('');
console.log('=== 5. 关闭时零改动 ===');
{
  const o = { choices: [{ delta: { content: 'a', reasoning_content: 'think' } }] };
  const sp = new ReasonSplitter({ enabled: false });
  const r = sp.process(o);
  ok('disabled: changed=false', r.changed === false);
  ok('disabled: reasoning 保留', o.choices[0].delta.reasoning_content === 'think');
}

console.log('');
console.log('=== 6. mode=delete 会删键而不是置空 ===');
{
  const o = { choices: [{ delta: { content: 'a', reasoning_content: 'r' } }] };
  const sp = new ReasonSplitter({ enabled: true, mode: 'delete' });
  sp.process(o);
  ok('reasoning_content 键被删除', !('reasoning_content' in o.choices[0].delta));
  ok('content 保留', o.choices[0].delta.content === 'a');
}

console.log('');
console.log('=== 7. minAnswerChars 保护 ===');
{
  const sp = new ReasonSplitter({ enabled: true, minAnswerChars: 5 });
  const a = sp.process({ choices: [{ delta: { content: 'ab', reasoning_content: 'R1' } }] });
  ok('不足阈值不切换', a.phase !== 'answering');
  ok('其 reasoning 保留', true);
  const b = sp.process({ choices: [{ delta: { content: 'cdef', reasoning_content: 'R2' } }] });
  ok('累计达标后切换', b.phase === 'answering');
}

console.log('');
console.log('=== 8. holdReasoningUntilAnswer 选项 ===');
{
  const o1 = { choices: [{ delta: { reasoning_content: 'early thinking' } }] };
  const sp = new ReasonSplitter({ enabled: true, holdReasoningUntilAnswer: true });
  sp.process(o1);
  ok('开启后思考阶段也被清空', o1.choices[0].delta.reasoning_content === '');

  const o2 = { choices: [{ delta: { reasoning_content: 'early thinking' } }] };
  const sp2 = new ReasonSplitter({ enabled: true, holdReasoningUntilAnswer: false });
  sp2.process(o2);
  ok('默认保留思考阶段', o2.choices[0].delta.reasoning_content === 'early thinking');
}

console.log('');
console.log('=== RESULT ===');
console.log('  PASS ' + pass + '   FAIL ' + fail);
process.exit(fail === 0 ? 0 : 1);
