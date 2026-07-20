# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`go-session-mongo` is a small library providing a MongoDB-backed `session.ManagerStore`/`session.Store`
implementation for `github.com/go-session/session/v3`. It is imported by taxime-hq services, so it follows the
same observability conventions as the rest of the fleet.

# Observability: context propagation, spans, and logging (mandatory)

This service uses `github.com/taxime-hq/kit` — `tracing` (OTEL + dynamic span registry) and `logging` (slog wrappers with trace correlation). Every new or edited function MUST follow these rules; they keep fleet-wide debug tooling working. Full background: kit/tracing/README.md and kit/logging/README.md.

## Context propagation

1. Every function/method that does I/O (DB, gRPC, HTTP, Kafka) or calls anything that does takes `ctx context.Context` as its **first** parameter. Update internal interfaces, implementations, fakes, and all callers together. When changing a signature, find call sites with the Go LSP (gopls references) — never with text search; grep misses interface satisfactions and function values.
2. Never use `context.TODO()`. Bare `context.Background()` only in `main`/`init`/tests.
3. Use `tracing.NewBackgroundContext(ctx)` (keeps the trace, drops deadline/cancellation) at boundaries:
   - MongoDB / Redis / SQL calls
   - `producer.Publish(...)` (Kafka)
   - outgoing gRPC/HTTP calls to other taxime services (HTTP also gets an explicit `context.WithTimeout`, default 5s)
   - goroutines — pass it as a parameter, never capture the outer `ctx`:
     `go func(ctx context.Context) { ... }(tracing.NewBackgroundContext(ctx))`
4. HTTP handler closures: `ctx := r.Context()` as the first line. Kafka `Handle(ctx, payload)` uses the provided ctx. Never use HTTP client convenience methods (`client.Get`/`Post`) — build the request with `http.NewRequestWithContext` and use `client.Do(req)`.
5. Do NOT add ctx to methods that satisfy interfaces defined outside this module (stdlib, sarama, etc.). Internal interfaces DO get ctx.

## Spans

6. Every function with ctx (parameter, or derived via `r.Context()`/`msg.Context()`) starts with:
   ```go
   defer tracing.MaybeStartSpan(&ctx).End()
   ```
   No name argument — the name is auto-detected. This includes gRPC handler methods (they root the trace), Kafka `Handle` methods, HTTP handler closures (right after `ctx := r.Context()`), private helpers, thin delegating wrappers, and inline callbacks/closures that receive a ctx.
7. Exceptions — no span and no debug logs (add a short comment saying it is intentional): interface declarations, fakes/mocks/test code, `main`/`init`, health-check handlers, audit-trail middleware.

## Logging — the gold standard

8. Use the `logging.*Context` wrappers from `kit/logging` (`ErrorContext`, `WarnContext`, `InfoContext`, `DebugContext`); in main/init use `logging.Error`/`logging.Info` (no ctx). Never logrus, never `fmt.Print*`, never raw `slog.*`. Never add `"func"` manually — it is injected automatically.
9. Log-call style: static message string (no variable data in the message), all data as key-value attributes, multi-line (one attribute per line).
10. Every function with a span gets the full pattern:
    - **Input debug log** immediately after the span line: the actual data the function works with. Decoded structs are logged whole via `logging.DeferJSON(dto)`; path/query params individually. HTTP handlers always include `"method"` and `"path"`. Kafka handlers log the payload **after** unmarshaling. Skip the input log only when there are no parameters besides ctx.
    - **Output debug log** before every success return (including implicit 200 OK): the full result via `logging.DeferJSON(result)`. Add `logging.ShrinkArrays(1, 1)` whenever the value is or contains a slice. HTTP: include `"status_code"` (+ payload / response headers). Kafka: include `"consumed"`. Functions returning only `error`: log `"success", true`.
    - **Error log before every error return** (including sentinel errors and `status.Error(...)` returns):
      ```go
      logging.ErrorContext(ctx, "unable to <do thing>",
          "<relevant_id>", id,
          "error", err,
      )
      ```
      HTTP handlers include `"status_code"`; Kafka handlers include `"consumed"`. The error log doubles as the output log on error paths. No silent error returns — ever.
11. Never log just counts/lengths as debug data (`"count", len(items)` is useless) — log the data itself with `DeferJSON` + `ShrinkArrays`. Pass `err` directly (`"error", err`), never `err.Error()`.
12. Levels: Error = real failures only. Info = essential business events only (created/processed/completed). Debug = payloads and intermediate state — it is off by default and Redis-gated, so PII is acceptable at Debug; never at Info/Error.
13. `logging.DeferJSON`/`logging.DeferFormat` for structs and expensive serializations; cheap scalars logged directly. Sensitive fields: `log:"mask"` / `log:"hide"` / `log:"exclude"` struct tags, or `logging.Mask/Hide/Exclude` path options.

## Verification before committing

- `go build ./...` and the repo's test target (`make test` or `go test ./...`) must pass.
- Re-check every function you added or touched against rules 6 and 10 — span present, input/output debug logs present, every error return logged.
