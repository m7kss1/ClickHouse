# Regenerate `rust_vendor` Locally

If you work on the Vortex branch and the build fails because `contrib/rust_vendor`
is outdated, regenerate it locally from the current Cargo lockfiles.

`./rust/vendor.sh` uses ClickHouse's pinned Rust toolchain and rewrites
`contrib/rust_vendor` from the current Rust dependency lockfiles. Do not manually
edit files inside `contrib/rust_vendor`; update dependencies or lockfiles first,
then rerun the script

```bash
git submodule update --init contrib/rust_vendor
./rust/vendor.sh
```
