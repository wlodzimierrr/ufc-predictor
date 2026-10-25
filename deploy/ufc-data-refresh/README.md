# UFC data refresh workload

T9.2.2 and T9.2.3 source manifests, matching the homelab base/envs layout. The
dev/prod overlays render independently into `ufc-data-refresh-dev` and
`ufc-data-refresh-prod`. Both are suspended. No cluster apply or Argo registration
is performed by this repository's publishing workflow.

## Install in the homelab source repository

Copy this directory to `homelab/workloads/apps/ufc-data-refresh` after reviewing
the source diff. Existing homelab files do not need modification to review/render
the new workload. Register the chosen environment through the homelab GitOps
review process only after configuring it. Do not register both environments
against the same writable production database.

```bash
kustomize build apps/ufc-data-refresh/envs/dev
kustomize build apps/ufc-data-refresh/envs/prod
```

Replace the overlay's `images[].newTag` with a published full `sha-<commit>` tag.
The base contains the image name once; environment overlays select the version.
Image publishing does not choose or deploy that version for the cluster.

The dev source now pins the validated immutable digest for source revision
`95a01a44c299350d8c8ac4d8ec570805fde3a053`. Anonymous manifest access was verified,
so dev removes the unused registry-pull-secret reference. Prod remains a template.
The active dashboard reports `APP_ENV=dev`; this draft follows that environment
and uses its own refresh namespace. No workload has been applied.

`gitops/project.yaml` and `gitops/application-dev.yaml` are separate registration
templates. The project permits only the new dev namespace; the Application uses
manual sync. They are deliberately absent from the workload kustomizations and
existing homelab bootstrap. The dashboard's existing AppProject cannot be reused
because it only permits the dashboard namespace.

## Runtime configuration

1. Provision/seed `ufc-data-refresh-inputs` with reviewed `data/` and `models/`
   directories from the [refresh contract](../../docs/automation-refresh-contract.md).
   The job mounts these read-only and writes private disposable copies. Use the
   cluster's appropriate storage class; the templates use its default. Ensure
   the pod's UID/GID 10001 can read the inputs and write the audit PVC. The
   `ReadWriteOnce` input claim requires the seeding pod to finish before the job.
   Build an allowlisted candidate with `python3 scripts/automation/build_input_bundle.py
   --output /absolute/new/candidate-directory` in ufc-data first. Its manifest pins
   hashes/bytes and marks inputs `not_reviewed`; copying is not source approval.
2. Apply required warehouse migrations using the separate operator migration
   procedure. In particular the run-audit table must exist before any refresh.
   The job deliberately performs no DDL or migration.
3. Make a private populated copy of `runtime-secret.env.example`. Store a
   PostgreSQL URI with credentials in it, with restricted permissions. Do not
   put that file in Git or shell history. The URI must target the same warehouse
   as the dashboard; do not copy a dashboard read-only role for this writer.
4. From the homelab workloads root, run
   `bash apps/ufc-data-refresh/encrypt-runtime-secret.sh dev /private/runtime.env`
   (or prod). It uses that repository's `.sops.yaml` recipient and writes only
   encrypted `runtime-secret.enc.yaml`. Enable the commented `generators` entry
   in the environment kustomization; use the existing homelab KSOPS render helper
   to validate decryption. Do not add the encrypted Secret to `resources`.
5. Configure `ghcr-read-token` through the existing homelab SOPS/KSOPS process if
   GHCR is private. It must be a `kubernetes.io/dockerconfigjson` pull secret in
   the job namespace. If the image is public, remove `imagePullSecrets` through
   an environment patch after verifying anonymous pulls.
6. Verify DNS and Postgres connectivity, the approved input bundle, the image
   build and a successful one-off run before changing `spec.suspend` to false.
   That one-off run scores predictions and writes the warehouse; it belongs to
   the operator's reviewed activation, not offline implementation tests.

The runtime Secret is intentionally absent until real credentials are encrypted;
there are no dummy credentials or encrypted placeholders posing as working
configuration. Plain overlay rendering is available before secret setup. A
rendered suspended workload is not evidence that secrets, storage or database
access are ready.

## Network access and acquisition

Base policies deny all ingress/egress, then allow kube-system DNS and TCP/5432 to
the `homelab-database` pods. Check the database ingress policy also admits this
client. Adapt the policy if the chosen database is elsewhere. No internet egress
is required by `make update_dashboard_data`.

`optional/networkpolicy-https.yaml` is excluded from both overlays. A later
qualified scraper can opt into public IPv4 TCP/443; Kubernetes NetworkPolicy does
not filter hostnames. UFCStats currently uses HTTP URLs, so this HTTPS policy
alone is insufficient for that scraper. Enable the actual required source
protocols only in the separate readiness review. Cookie/session credentials
remain absent while scrapers are disabled; no browser is installed speculatively.

The CronJob's `Forbid` policy only prevents overlap with its own scheduled Jobs.
Before launching a manual Job, check that no other refresh or seeding Job is
running. Avoid applying both environment overlays to the same writer database.
The pipeline commits its loaders separately; no automatic retry follows failure.

## Rotation

Edit the private input file, rerun the encryption helper for the chosen
environment, review the encrypted diff, and deliver it through homelab GitOps.
Existing pods retain their original environment; run the next reviewed Job with
the new Secret. Remove the private plaintext copy using the operator's normal
secret handling. Rotate registry pull credentials in the same SOPS workflow.

Refer to the [automation runbook](../../docs/runbook.md) in the ufc-data repository
for inspection, retry and fallback commands. Those links are relative to this
repository; after copying to homelab, use the ufc-data docs directly.

Manifest semantics were checked against the official
[CronJob](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/)
and [NetworkPolicy](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
documentation. Cluster-side validation remains an activation task.
