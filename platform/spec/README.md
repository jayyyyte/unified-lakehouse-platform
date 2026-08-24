# platform/spec — the domain contract

Schema and validation for `domains/<domain>/retrieval.yaml`, the single file that teaches the
generic core about a new domain.

A spec declares an **entity**, its **channels** (engine, query template, the dataset whose
watermark governs the channel, and a freshness SLA), and a **fusion** policy with its parameters.

This package is the boundary of the abstraction. Everything a domain is allowed to say lives in
this schema; anything it cannot express here would have to be added as a platform capability
first, which is exactly the review step that keeps `platform/` domain-free.
