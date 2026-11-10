# TODO / deferred ideas

## Browser / crawling

Context: sync run 159 (2026-11-10) lost 4 website sources (gloo-edge, gloo-gateway-v1,
helm, istio) because the `qdrant/browserless` pod was OOMKilled (4Gi limit) while
rendering huge pages, and each source's 5s retry fired while browserless was still
restarting. Fixed in code: the real connection error is reported, remote
connects retry for ~2 min, and pages over 4x `max_size` (uncompressed, via a HEAD
with `Accept-Encoding: identity`) are skipped before rendering, with their links
taken from the raw HTML. Still open:

- **Size probe gaps.** Servers that ignore `identity` and answer compressed, or send
  no `Content-Length` (chunked), are not pre-skipped and still get rendered. Catching
  those would need a streaming GET that aborts past the threshold.
- **Make the 4x pre-skip factor configurable** per source if a site needs it.
- **Browserless deployment (infra, not this repo).** Raise the memory limit above 4Gi
  or cap `/dev/shm` (currently a 4Gi memory-backed emptyDir that counts against the
  same limit). `CONCURRENT=4` is more than doc2vec uses (one session at a time).
- **Exclude `_print` pages** in the kubernetes source config (`exclude_paths`/pattern):
  they duplicate content already indexed page by page.
