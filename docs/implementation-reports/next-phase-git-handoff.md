# Next-phase Git handoff

The user requires the next authorized implementation phase to commit and push
all of its changes. This overrides any "no commit or push" instruction carried
over from an earlier phase prompt; it does not authorize production deployment,
model promotion, live data changes, or expansion of the implementation scope.

At the start, inspect the worktree, current branch, upstream, and remote. Preserve
unrelated user edits and existing frozen artifacts. The current repository is
`/home/wlodzimierrr/ufc-data`, branch `main`, with remote `origin` at
`https://github.com/wlodzimierrr/ufc-predictor.git`; verify these again rather than
assuming they are unchanged.

Before the final handoff:

1. Run the relevant safe tests and integrity checks. Do not run the known
   mutating live integration test or repair unrelated failures merely to commit.
2. Finish the phase report, recording the actual completion or blocker status.
   A blocked implementation may still have valid code, tests, and clearly marked
   incomplete evidence to commit; committing it does not resolve its blocker.
3. Review all pending changes for scope, secrets, and large/generated files.
   Include the phase's intended code, tests, documentation, reports, and frozen
   evidence. Keep credentials, dependency directories, and ignored runtime
   artifacts out of Git; do not force-add ignored files without specific authority.
4. Split the changes into logical commits. Preserve checksum-locked files
   byte-for-byte; `.gitattributes` disables text conversion for their directories.
5. Verify the committed artifacts and final worktree, then push the commits to
   the current branch's verified upstream. Use a normal fast-forward push, never
   a force push. If the remote has diverged or authentication fails, report the
   exact blocker without rewriting history or claiming success.
6. In the final response, list commit hashes and subjects, test results, remaining
   worktree changes, and the remote/ref and outcome of the push. Record hashes
   and push status in the final response rather than creating a self-referential
   report-update commit loop.

For the next implementation prompt, append:

> Follow `docs/implementation-reports/next-phase-git-handoff.md`. At the end,
> commit all phase changes in logical commits and push them to the verified
> upstream. This supersedes earlier prohibitions on committing or pushing, but
> leaves all model, data, deployment, and preservation scope restrictions intact.
