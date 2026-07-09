Custom CA certificates
======================

Drop PEM-encoded CA certificate files into this directory if your build environment sits behind a TLS-inspecting proxy or otherwise needs to trust a private certificate authority. Any file extension may be used (`.crt`, `.pem`, `.cer`, etc.) — the contents are what matter.

When the Rust builder Docker images are built, anything in this directory is copied **into the builder container** at `/usr/local/share/ca-certificates/`, and `update-ca-certificates` is run **inside that container** to register the new trust roots. The container's `apt` sources are also rewritten to HTTPS at that point. None of this affects your host system. If this directory is empty (apart from this README), the builder images are built without any custom CA trust changes.

This README file is ignored by the builder scripts and excluded from the Docker build context — it exists only so the directory is tracked in Git.

See [docs/development/custom-environment.md](../docs/development/custom-environment.md) for the full list of hooks and environment variables that support building Sleeper in restricted network environments.
