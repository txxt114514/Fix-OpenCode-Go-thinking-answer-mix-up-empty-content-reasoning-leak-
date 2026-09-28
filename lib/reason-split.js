/* =============================================================================
 * reason-split.js -- 把「思考」和「回答」分开的流式改写核心
 *
 * 问题（真实观测，2026-09-28）
 *   OpenCode Go 网关在 deepseek-v4.1-flash 上，会把模型的英文规划（"The user
 *   says: ... So: ... My job: ..."）连同中文答案一起推给客户端，客户端于是把
 *   思考链和正文全部显示出来，用户看到一大段不该看到的英文。
 *
 * 上游真实 SSE 的字段时序（本机抓包，86 个事件）
 *   事件 #0    {"role":"assistant","content":"","reasoning_content":""}
 *   事件 #1-72 {"content":"",     "reasoning_content":"..."}   <- 思考阶段
 *   事件 #73   {"content":"极限", "reasoning_content":""}      <- 回答开始
 *   事件 #74   {"content":"是指", "reasoning_content":""}
 *   事件 #83   {"content":"...",  "reasoning_content":"", "finish_reason":"stop"}
 *   [DONE]
 *
 * 关键事实
 *   1. 切换点是可以精确判定的：同一条 delta 里 content 由空转非空、
 *      reasoning_content 由非空转空。**不需要靠语言或字数猜。**
 *   2. reasoning_content 在切换后变成【空字符串】而不是消失。用
 *      `'reasoning_content' in delta` 判断的实现会把空串当成"有值"，
 *      于是继续往思考块里写 —— 这正是「思考和回答全展示」的来源。
 *
 * 本模块做什么
 *   在流经过时跟踪状态，一旦确认进入回答阶段，就把此后事件里的
 *   reasoning_content 清掉（或在要求下彻底删键），让下游只看到回答。
 *
 * 绝不做什么
 *   1. 不动 content。一个字符都不改。
 *   2. 不动任何含 tool_calls 的事件（实测 400 条请求里 287 条带工具调用，
 *      误伤会直接断掉整个工具链）。
 *   3. 不动非 data 行、[DONE]、以及解析失败的行（原样透传）。
 *   4. 不因为"看起来像思考"就丢内容：默认只清 reasoning，不丢 reasoning
 *      的收集结果（调用方可以选择把它交给 stats 或丢弃）。
 * ========================================================================== */

'use strict';

/** 从 content 片段里拼出纯文本（兼容字符串与 {type,text} 两种写法） */
function contentToText(c) {
  if (typeof c === 'string') return c;
  if (Array.isArray(c)) {
    let s = '';
    for (const p of c) {
      if (typeof p === 'string') s += p;
      else if (p && typeof p.text === 'string') s += p.text;
    }
    return s;
  }
  if (c && typeof c === 'object' && typeof c.text === 'string') return c.text;
  return '';
}

/** 这段 delta 是否含工具调用 */
function hasToolCalls(delta) {
  if (!delta || typeof delta !== 'object') return false;
  if (Array.isArray(delta.tool_calls) && delta.tool_calls.length > 0) return true;
  if (delta.function_call) return true;
  return false;
}

/** 这段 delta 是否含非空 reasoning */
function reasoningText(delta) {
  if (!delta || typeof delta !== 'object') return '';
  const r = delta.reasoning_content;
  if (typeof r === 'string') return r;
  if (typeof delta.reasoning === 'string') return delta.reasoning;
  return '';
}

/** 把 delta 里的 reasoning 清掉 */
function stripReasoning(delta, mode) {
  if (!delta || typeof delta !== 'object') return false;
  let touched = false;
  if (Object.prototype.hasOwnProperty.call(delta, 'reasoning_content')) {
    if (mode === 'delete') { delete delta.reasoning_content; touched = true; }
    else if (delta.reasoning_content !== '') { delta.reasoning_content = ''; touched = true; }
  }
  if (Object.prototype.hasOwnProperty.call(delta, 'reasoning')) {
    if (mode === 'delete') { delete delta.reasoning; touched = true; }
    else if (delta.reasoning !== '') { delta.reasoning = ''; touched = true; }
  }
  return touched;
}

/**
 * 思考/回答分离器。每条响应流一个实例。
 *
 * 状态机
 *   phase = 'idle'      还没有任何内容
 *         | 'thinking'  见过 reasoning，还没见过 content
 *         | 'answering' content 已经开始
 *
 * 触发条件（默认 auto）
 *   看到 content 非空的第一条事件 -> 进入 answering，并从此清掉 reasoning。
 *
 * 可选保护
 *   minAnswerChars: content 累计达到这么多字符才切换（默认 0 = 立即切换）。
 *     设 >0 可避免"content 抖了一下又回到 reasoning"的极端情况误判。
 *   holdReasoningUntilAnswer: 未进入 answering 之前，是否也清掉 reasoning。
 *     默认 false —— 思考阶段照常透传，只有回答开始时才清，
 *     这样用户仍能看到思考过程（DSH 的思考块）。设 true 则思考也一并抹掉。
 */
class ReasonSplitter {
  constructor(cfg) {
    const c = cfg || {};
    this.enabled = c.enabled !== false;
    this.mode = c.mode === 'delete' ? 'delete' : 'blank';
    this.holdReasoningUntilAnswer = !!c.holdReasoningUntilAnswer;
    this.minAnswerChars = Number.isFinite(c.minAnswerChars) ? c.minAnswerChars : 0;

    this.phase = 'idle';
    this.contentChars = 0;
    this.reasoningChars = 0;
    this.clearedReasoningEvents = 0;
    this.switchedAt = -1;   // 第几条事件进入 answering
    this.seenEvents = 0;
    // 攒 reasoning 原文：情况 A（content 始终为空）补发时要用它当正文
    this.collectReasoning = c.collectReasoning !== false;
    this.reasoningText = '';
    this.reasoningLimit = Number.isFinite(c.reasoningLimit) ? c.reasoningLimit : 4 * 1024 * 1024;
  }

  /**
   * 处理一条已解析的 SSE 对象（原地修改）。
   * @param {object} obj  解析后的 JSON
   * @returns {{changed:boolean, phase:string}}
   */
  process(obj) {
    if (!this.enabled) return { changed: false, phase: this.phase };
    if (!obj || typeof obj !== 'object') return { changed: false, phase: this.phase };

    const choices = Array.isArray(obj.choices) ? obj.choices : null;
    if (!choices) return { changed: false, phase: this.phase };  // Anthropic / 其它格式先不处理

    let changed = false;

    for (const choice of choices) {
      const delta = choice && choice.delta;
      if (!delta) continue;

      // 工具调用：一律不碰（实测 72% 的请求带工具调用）
      if (hasToolCalls(delta)) continue;

      const text = contentToText(delta.content);
      const reason = reasoningText(delta);

      if (reason) {
        this.reasoningChars += reason.length;
        if (this.collectReasoning && this.reasoningText.length < this.reasoningLimit) {
          this.reasoningText += reason;
        }
      }
      if (text) this.contentChars += text.length;

      // 是否该切换到回答阶段
      if (this.phase !== 'answering' && text && this.contentChars >= this.minAnswerChars) {
        this.phase = 'answering';
        this.switchedAt = this.seenEvents;
      } else if (this.phase === 'idle' && reason) {
        this.phase = 'thinking';
      }

      // 进入回答阶段后：清掉 reasoning
      if (this.phase === 'answering') {
        if (stripReasoning(delta, this.mode)) {
          changed = true;
          this.clearedReasoningEvents++;
        }
      } else if (this.holdReasoningUntilAnswer && reason) {
        // 可选：思考阶段也抹掉
        if (stripReasoning(delta, this.mode)) {
          changed = true;
          this.clearedReasoningEvents++;
        }
      }
    }

    this.seenEvents++;
    return { changed, phase: this.phase };
  }

  /** 供日志/测试使用的统计 */
  stats() {
    return {
      phase: this.phase,
      contentChars: this.contentChars,
      reasoningChars: this.reasoningChars,
      clearedReasoningEvents: this.clearedReasoningEvents,
      switchedAt: this.switchedAt,
      seenEvents: this.seenEvents,
      collectedReasoningChars: this.reasoningText.length,
    };
  }

  /**
   * 情况 A 补发用：整条流没有任何 content，但攒下了 reasoning。
   * @returns {string} 应当提升为正文的文本；不该补发时返回空串。
   */
  takeBackfillText() {
    if (!this.enabled || !this.collectReasoning) return '';
    if (this.contentChars > 0) return '';       // 有正文，不需要救
    if (!this.reasoningText) return '';         // 也没思考，没得救
    return this.reasoningText;
  }
}

module.exports = {
  ReasonSplitter,
  contentToText,
  hasToolCalls,
  reasoningText,
  stripReasoning,
};
