"""Render infrastructure offline and check operational boundaries."""

from pathlib import Path
import shutil
import subprocess

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "deploy" / "ufc-data-refresh"


@pytest.mark.parametrize("environment", ["dev", "prod"])
def test_rendered_schedule_and_runtime_are_isolated_and_suspended(environment):
    if not shutil.which("kustomize"):
        pytest.skip("kustomize unavailable")
    rendered = subprocess.run(
        ["kustomize", "build", str(DEPLOY / "envs" / environment)],
        check=True, capture_output=True, text=True,
    ).stdout
    documents = list(yaml.safe_load_all(rendered))
    namespace = f"ufc-data-refresh-{environment}"
    assert [d["metadata"]["name"] for d in documents if d["kind"] == "Namespace"] == [namespace]
    assert all(d["metadata"]["namespace"] == namespace for d in documents if d["kind"] != "Namespace")
    cron = next(d for d in documents if d["kind"] == "CronJob")["spec"]
    assert cron["schedule"] == "0 12 * * 0"
    assert cron["timeZone"] == "Europe/London"
    assert cron["suspend"] is True
    assert cron["concurrencyPolicy"] == "Forbid"
    assert cron["jobTemplate"]["spec"]["backoffLimit"] == 0
    pod = cron["jobTemplate"]["spec"]["template"]["spec"]
    assert pod["restartPolicy"] == "Never"
    assert pod["automountServiceAccountToken"] is False
    assert pod["securityContext"]["runAsNonRoot"] is True
    container = pod["containers"][0]
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    mounts = {m["mountPath"]: m for m in container["volumeMounts"]}
    assert mounts["/runtime-inputs"]["readOnly"] is True
    volumes = {v["name"]: v for v in pod["volumes"]}
    for writable in ("/app/data", "/app/models", "/tmp"):
        assert "emptyDir" in volumes[mounts[writable]["name"]]
    assert "persistentVolumeClaim" in volumes[mounts["/audit"]["name"]]
    assert {"secretRef": {"name": "ufc-data-refresh-runtime"}} in container["envFrom"]
    assert not any(d["kind"] == "Secret" for d in documents)
    policies = [d for d in documents if d["kind"] == "NetworkPolicy"]
    assert len(policies) == 3
    assert not any(port["port"] in (80, 443) for d in policies for rule in d["spec"].get("egress", []) for port in rule.get("ports", []))
    database = next(d for d in policies if d["metadata"]["name"].endswith("-database"))
    peer = database["spec"]["egress"][0]["to"][0]
    assert peer["namespaceSelector"]["matchLabels"]["kubernetes.io/metadata.name"] == "homelab-database"
    assert peer["podSelector"]["matchLabels"]["app.kubernetes.io/name"] == "homelab-database"


def test_secret_bootstrap_rejects_placeholder_without_publishing_plaintext(tmp_path):
    (tmp_path / ".sops.yaml").write_text("creation_rules: []\n")
    target_dir = tmp_path / "apps/ufc-data-refresh/envs/dev"
    target_dir.mkdir(parents=True)
    secret_input = tmp_path / "private.env"
    secret_input.write_text("DATABASE_URL=postgresql://REPLACE_USER:REPLACE_PASSWORD@db/ufc\n")
    result = subprocess.run(
        ["bash", str(DEPLOY / "encrypt-runtime-secret.sh"), "dev", str(secret_input)],
        cwd=tmp_path, capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert not (target_dir / "runtime-secret.enc.yaml").exists()
    assert "REPLACE_PASSWORD" not in result.stdout + result.stderr
