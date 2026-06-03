# Native format framing + TCP TLS trust

03 June 2026. Two fixes, both validated against a real multi-node CH
26.2 cluster (not single-node docker -- see the testing note at the
end).

## 1. HTTP `FORMAT Native` was mis-framed (issue #15)

`insert_native` over HTTP emitted a leading `BlockInfo` and a per-column
custom-serialisation flag byte (the revision-54454 layout). ClickHouse
reads HTTP `FORMAT Native` input at **protocol revision 0**, which
expects neither. The extra bytes shifted the reader's cursor and it read
our `bucket_num = -1` bytes as a length varint -> `Code 33: Cannot read
all data ... Bytes expected: 16383`.

- Fix: the HTTP path now encodes at revision 0 -- `counts +
  per-column(name, type, data)`, no BlockInfo, no flag.
- TCP is unchanged: it negotiates a revision (>= 54454 on modern CH), so
  the connection actor writes BlockInfo and the encoder writes the flag.
  That's correct for TCP and stays.
- Confirmed live: old framing rejected with the exact error; new framing
  accepted; row read back correct.

## 2. `tcp_tls` only trusted public CAs (issue #16)

The TCP TLS path trusted webpki-roots only, so it couldn't verify a
server behind a private/internal CA. The HTTP path had the same gap (no
runtime custom-CA option, native roots only via a non-default compile
feature).

Rather than patch TCP alone, the trust mechanism is now **shared by both
transports**:

- One `TlsTrust` -> one `rustls::ClientConfig`, used by the HTTP
  connector and the TCP connector.
- Default: OS native roots + webpki (so internal-CA clusters work out of
  the box where the CA is OS-installed).
- Builder surface (Go's `Options.TLS` is the model): `with_tls_config`
  for full control; `with_tls_native_roots` / `with_tls_webpki_roots` /
  `with_tls_roots_exclusive` toggles; `try_with_tls_root_ca(path)` /
  `try_with_tls_intermediate_certs(path)` for explicit PEM bundles
  (cat-appended files iterate; `AppendCertsFromPEM` semantics).
- Back-compat: additive only. Existing HTTP users on the default path
  are untouched -- the custom path is opt-in.
- Confirmed live: connected to the internal-CA cluster over TCP+TLS on
  the secure native port, both via native-roots default and via an
  explicit exclusive CA.

## 3. Type coverage, checked live

Round-tripped the full type matrix against the cluster (encode for
INSERT, decode for SELECT): integers incl. Int/UInt128/256, Decimal
32/64/128/256, Date32, DateTime64 with precision + timezone,
LowCardinality (incl. Nullable + NULL), Nullable, Array (incl. nested +
empty), Map, FixedString, Tuple, UUID, IPv4/6, Enum. All correct.

Known gaps (own follow-up): `Variant` and `Dynamic` decode to an
"unsupported" placeholder (stream stays aligned -- safe). `JSON` decode
currently errors on the wire version and needs proper support; it's
mission-critical for us and gets a dedicated piece of work, including
auto sub-column creation.

## Testing note

These were validated against the real multi-node cluster
(ReplicatedMergeTree, `ON CLUSTER`, reads tolerant of replica routing),
not a single-node docker instance. We've been bitten by tests that pass
on single-node and fail distributed; the cluster is the source of truth.
