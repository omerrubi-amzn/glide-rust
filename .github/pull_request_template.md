<!--
Thanks for contributing to Valkey GLIDE!

Please make sure you are aware of our contributing guidelines
(see CONTRIBUTING.md).
-->

### Summary

<!-- Add a summary describing the changes -->

### Issue link

This Pull Request is linked to issue: [<Issue Title>](<Issue URL>)
Closes <Issue #>

### Features / Behaviour Changes

<!-- Outline the feature support or behaviour changes included in this PR -->

### Implementation

<!-- Describe the implementation details and call out areas for reviewer attention -->

### Limitations

<!-- Describe any features or use cases that are not implemented or only partially supported -->

### Testing

<!-- Describe what tests were added/run and any relevant results -->

### Checklist

Before submitting the PR make sure the following are checked:

-   [ ] This Pull Request is related to one issue.
-   [ ] Commit message has a detailed description of what changed and why.
-   [ ] Tests are added or updated (mock test in `src/command_mock/` and/or live test in `tests/it_*.rs`).
-   [ ] `cargo fmt --all` and `cargo clippy --all-features --all-targets -- -D warnings` pass.
-   [ ] `cargo test` and `cargo deny check` pass.
-   [ ] Docs (`DESIGN.md` / `DEVELOPER.md` / rustdoc) updated where relevant.
