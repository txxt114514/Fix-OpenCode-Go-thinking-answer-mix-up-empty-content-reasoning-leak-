# dsh-go-reasoning-splitter

A ~350-line, zero-dependency reverse proxy that fixes OpenCode Go's
**thinking / answer mix-up** on DeepSeek models.

> **TL;DR** — Point your OpenAI-compatible client at this proxy instead of
> `opencode.ai` directly. When the upstream sends only `reasoning_content`
> (empty answer), the proxy backfills `content`. When the upstream keeps
> sending `reasoning_content` after the answer has started, the proxy clears
> it so your client stops rendering internal planning as user-visible text.

---

## The problem

Two failure modes were observed on `opencode.ai/zen/go/v1` with
`deepseek-v4.1-flash` and `deepseek-v4-flash` (September 2026):

### A. Empty answer — "either nothing, or everything"

The upstream streams a substantial `reasoning_content` and then never emits
any `content`. The answer the model actually produced stays inside the
thinking field. Clients that only render `content` show an empty reply;
clients that render both dump ~2,500 characters of internal planning at the
user.

Measured with a two-line prompt (`帮我规划一下…用中文回答`):

```
events=570   reasoning=2485 chars   content=0 chars
shape:  #0:--   #1:-R          <- never reaches a content phase
```

### B. Mixed — planning leaks after the answer starts

The upstream starts emitting `content`, but `reasoning_content` keeps
flowing alongside it. A client that renders both fields shows the user raw
English planning, e.g.:

```
The user says: "我什么都没看我现在做是不是都值得做" — "I haven't looked at anything…"
So: they haven't seen the answers… My job: give an honest triage for 习题 1-6.
```

In a 400-request sample from a real proxy log, **8.3 %** of streaming
responses had both fields populated, with ratios like
`content=13 / reasoning=4865`.

### The exact switch point is observable

From a captured stream (85 SSE events):

```
#72  {"content":"",     "reasoning_content":"。"}      <- thinking
#73  {"content":"极限", "reasoning_content":""}        <- answer starts
#74  {"content":"是指", "reasoning_content":""}
```

The switch is a field flip inside one `delta`. No guessing, no heuristics.

The catch: after the switch the upstream **keeps the `reasoning_content` key
present with an empty string**. Code that tests `'reasoning_content' in delta`
(or `delta.reasoning_content ?? ''`) treats that as "still present" and keeps
appending to the thinking block — which is exactly how "thinking and answer
both shown" happens.

---

## What this proxy does

| Situation | Detection | Action |
|---|---|---|
| **A. empty `content`** | stream ends, `content` was never non-empty, `reasoning_content` was | backfill: emit a synthetic `content` delta carrying the collected reasoning, right before the terminator |
| **B. mixed** | `content` became non-empty | clear `reasoning_content` on every subsequent event (`""`, or delete the key with `RS_MODE=delete`) |
| **tool calls** | any event carrying `tool_calls` / `function_call` | **never touched, byte-for-byte passthrough** |
| **normal stream** | `content` present, `reasoning` stops on its own | untouched |

Hard invariants, each enforced by a test:

- `content` is never modified. Not one character.
- Events with tool calls are never modified (in the same 400-request sample,
  **287 of 400** requests carried tool calls — getting this wrong breaks every
  agent loop).
- `[DONE]` and `finish_reason` stay last.
- Non-SSE responses are piped through unmodified.
- Disabled mode is a true no-op.

---

## Install & run

Zero dependencies. Node 18+.

```bash
git clone https://github.com/txxt114514/Fix-OpenCode-Go-thinking-answer-mix-up-empty-content-reasoning-leak-.git
cd dsh-go-reasoning-splitter
node server.js
```

It listens on `http://127.0.0.1:8912` and forwards to `opencode.ai:443`.

Point your client at:

```
http://127.0.0.1:8912/zen/go/v1
```

For example, in an OpenAI-compatible config:

```yaml
baseURL: http://127.0.0.1:8912/zen/go/v1
apiKey: <your opencode-go key>
```

Only `/zen/go/*` is proxied; everything else returns 404 so the process
cannot be abused as an open relay.

### Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8912` | listen port |
| `HOST` | `127.0.0.1` | listen address |
| `UPSTREAM_HOST` | `opencode.ai` | upstream host |
| `UPSTREAM_PORT` | `443` | upstream port |
| `UPSTREAM_TLS` | `1` | set `0` for plaintext upstream (tests) |
| `RS_BACKFILL` | `1` | enable situation A backfill |
| `RS_SPLIT` | `1` | enable situation B separation |
| `RS_MODE` | `blank` | `blank` = set `reasoning_content:""`; `delete` = remove the key |
| `RS_DEBUG` | `0` | also print log lines to stdout |
| `RS_LOG` | `./proxy.log` | log file path |

Every proxied response writes one line to the log:

```
2026-09-28T11:19:17.123Z | path=/zen/go/v1/chat/completions | mode=sse | status=200 |
ms=4210 | content_seen=38 | content_chars=38 | reasoning_chars=305 |
tool_calls=0 | backfilled=false | split_cleared=0 | phase=answering
```

---

## Tests

```bash
npm test
```

- `test/reason-split.test.js` — 23 assertions against a **real captured
  stream** (`signal-probe-raw.txt`), plus unit coverage of the state machine.
- `test/server.e2e.test.js` — 11 assertions. Spins up a fake upstream
  replaying scenarios A / B / tool-calls / normal, runs the real proxy
  against it, and asserts on what the client receives.

Both suites are self-contained and offline.

```
PASS 23   FAIL 0     (unit)
PASS 11   FAIL 0     (e2e)
```

---

## Why a proxy and not a client plugin

Some harnesses (notably DeepSeek Harness) expose plugin hooks for tool calls
and user messages, but **no hook for LLM streaming output**. A plugin cannot
rewrite the SSE stream. A proxy sits at the only layer that sees and can
modify every event, and it works with any client that speaks
OpenAI-compatible chat completions.

## Limitations

- Only OpenAI-style `choices[].delta` streams are rewritten. Anthropic-style
  events are passed through untouched (backfill can emit an Anthropic-shaped
  `content_block_delta` when the stream is detected as Anthropic, but
  situation B is not implemented for that format yet).
- Non-streaming responses are not inspected at all.
- `reasoning_content` collection is capped at 4 MB per response to bound
  memory; beyond that the backfill text is truncated.
- No auth handling: the proxy forwards your `Authorization` header verbatim
  and stores nothing.

## License

MIT
