from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import shutil
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


API_BASE = "https://api.github.com"
API_ACCEPT = "application/vnd.github+json"
API_VERSION = "2022-11-28"


class GitHubReleaseError(RuntimeError):
    pass


def _headers(token: str, *, accept: str = API_ACCEPT, extra: dict[str, str] | None = None) -> dict[str, str]:
    headers = {
        "Accept": accept,
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": API_VERSION,
        "User-Agent": "cb-orchestrator-release-client",
    }
    if extra:
        headers.update(extra)
    return headers


def _request(
    method: str,
    url: str,
    token: str,
    *,
    accept: str = API_ACCEPT,
    json_body: dict[str, Any] | None = None,
    data: bytes | None = None,
    extra_headers: dict[str, str] | None = None,
) -> tuple[int, bytes]:
    if json_body is not None and data is not None:
        raise ValueError("json_body and data are mutually exclusive")
    payload = data
    headers = _headers(token, accept=accept, extra=extra_headers)
    if json_body is not None:
        payload = json.dumps(json_body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=payload, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        body = exc.read()
        raise GitHubReleaseError(f"{method} {url} failed with {exc.code}: {body.decode('utf-8', errors='replace')}") from exc


def request_json(
    method: str,
    url: str,
    token: str,
    *,
    json_body: dict[str, Any] | None = None,
    data: bytes | None = None,
    accept: str = API_ACCEPT,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    _, body = _request(
        method,
        url,
        token,
        accept=accept,
        json_body=json_body,
        data=data,
        extra_headers=extra_headers,
    )
    if not body:
        return {}
    return json.loads(body.decode("utf-8"))


def get_release_by_tag(repo: str, tag: str, token: str) -> dict[str, Any] | None:
    url = f"{API_BASE}/repos/{repo}/releases/tags/{urllib.parse.quote(tag, safe='')}"
    request = urllib.request.Request(url, headers=_headers(token), method="GET")
    try:
        with urllib.request.urlopen(request) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        body = exc.read()
        raise GitHubReleaseError(f"GET {url} failed with {exc.code}: {body.decode('utf-8', errors='replace')}") from exc


def find_asset(release: dict[str, Any], asset_name: str) -> dict[str, Any] | None:
    for asset in release.get("assets", []):
        if asset.get("name") == asset_name:
            return asset
    return None


def download_asset(repo: str, asset_id: int, token: str, output_path: Path) -> Path:
    url = f"{API_BASE}/repos/{repo}/releases/assets/{asset_id}"
    request = urllib.request.Request(
        url,
        headers=_headers(token, accept="application/octet-stream"),
        method="GET",
    )
    try:
        with urllib.request.urlopen(request) as response:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(response.read())
            return output_path
    except urllib.error.HTTPError as exc:
        body = exc.read()
        raise GitHubReleaseError(f"GET {url} failed with {exc.code}: {body.decode('utf-8', errors='replace')}") from exc


def parse_release_body(body: str | None) -> dict[str, str]:
    payload: dict[str, str] = {}
    for raw_line in (body or "").splitlines():
        line = raw_line.strip()
        if not line.startswith("- "):
            continue
        content = line[2:]
        key, sep, value = content.partition(":")
        if not sep:
            continue
        payload[key.strip()] = value.strip()
    return payload


def inspect_latest_release(repo: str, tag: str, asset_name: str, token: str) -> dict[str, Any]:
    release = get_release_by_tag(repo, tag, token)
    if release is None:
        return {
            "release_poll_status": "missing_release",
            "release_repo": repo,
            "release_tag": tag,
            "release_asset_name": asset_name,
        }

    archive_asset = find_asset(release, asset_name)
    sha_asset = find_asset(release, f"{asset_name}.sha256")
    metadata = parse_release_body(str(release.get("body") or ""))
    target_trade_date = metadata.get("target_trade_date") or metadata.get("latest_complete_trade_date") or ""
    latest_complete_trade_date = metadata.get("latest_complete_trade_date") or target_trade_date
    fingerprint = metadata.get("content_fingerprint", "")
    sha256 = metadata.get("sha256", "")

    payload = {
        "release_repo": repo,
        "release_tag": tag,
        "release_id": release.get("id"),
        "release_name": release.get("name", ""),
        "release_asset_name": asset_name,
        "release_download_url": archive_asset.get("browser_download_url", "") if archive_asset else "",
        "release_sha256_url": sha_asset.get("browser_download_url", "") if sha_asset else "",
        "release_latest_complete_trade_date": latest_complete_trade_date,
        "release_target_trade_date": target_trade_date,
        "release_content_fingerprint": fingerprint,
        "release_sha256": sha256,
        "release_uploaded_at": metadata.get("uploaded_at", ""),
        "release_size_bytes": archive_asset.get("size") if archive_asset else None,
    }

    if not latest_complete_trade_date or not fingerprint:
        payload["release_poll_status"] = "missing_release_metadata"
        return payload
    if archive_asset is None or sha_asset is None:
        payload["release_poll_status"] = "missing_release_assets"
        return payload

    payload["release_poll_status"] = "ready"
    return payload


def release_state_path(state_root: Path) -> Path:
    return state_root / "release" / "latest.json"


def load_release_state(state_root: Path) -> dict[str, Any]:
    path = release_state_path(state_root)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_release_state(state_root: Path, payload: dict[str, Any]) -> Path:
    path = release_state_path(state_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def release_is_new(release_info: Mapping[str, Any], installed_state: Mapping[str, Any], install_dir: Path) -> bool:
    fingerprint = str(release_info.get("release_content_fingerprint") or "")
    trade_date = str(release_info.get("release_latest_complete_trade_date") or "")
    if not fingerprint or not trade_date:
        return False
    if not install_dir.exists():
        return True
    if str(installed_state.get("release_content_fingerprint") or "") != fingerprint:
        return True
    if str(installed_state.get("release_latest_complete_trade_date") or "") != trade_date:
        return True
    return False


def build_synthetic_upstream_state(release_info: Mapping[str, Any]) -> dict[str, Any]:
    target_trade_date = str(
        release_info.get("release_target_trade_date")
        or release_info.get("release_latest_complete_trade_date")
        or ""
    )
    latest_complete_trade_date = str(release_info.get("release_latest_complete_trade_date") or target_trade_date)
    return {
        "exit_class": "success",
        "cb_status": "success",
        "index_status": "success",
        "event_status": "success",
        "dump_status": "success",
        "qlib_status": "success",
        "target_trade_date": target_trade_date,
        "latest_complete_trade_date": latest_complete_trade_date,
    }


def parse_sha256_file(path: Path, expected_name: str) -> str:
    line = path.read_text(encoding="utf-8").strip()
    digest, _, filename = line.partition("  ")
    if not digest or filename != expected_name:
        raise GitHubReleaseError(f"invalid sha256 file format in {path}")
    return digest


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require_binary(binary: str) -> None:
    if shutil.which(binary):
        return
    raise GitHubReleaseError(f"required binary not found in PATH: {binary}")


def safe_extract(archive_path: Path, output_dir: Path) -> None:
    require_binary("tar")
    require_binary("zstd")
    listing = subprocess.run(
        ["tar", "--zstd", "-tf", str(archive_path)],
        check=True,
        capture_output=True,
        text=True,
    )
    base = output_dir.resolve()
    for member in listing.stdout.splitlines():
        member_path = (output_dir / member).resolve()
        if not member_path.is_relative_to(base):
            raise GitHubReleaseError(f"unsafe tar member detected: {member}")
    subprocess.run(
        ["tar", "--zstd", "-xf", str(archive_path), "-C", str(output_dir)],
        check=True,
    )


def atomic_replace(source_dir: Path, target_dir: Path) -> Path | None:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_path = target_dir.parent / f".{target_dir.name}.bak.{timestamp}"
    if target_dir.exists():
        os.replace(target_dir, backup_path)
    try:
        os.replace(source_dir, target_dir)
    except Exception:
        if backup_path.exists() and not target_dir.exists():
            os.replace(backup_path, target_dir)
        raise
    return backup_path if backup_path.exists() else None


def install_release(
    *,
    repo: str,
    tag: str,
    asset_name: str,
    token: str,
    target_dir: Path,
    work_dir: Path,
) -> dict[str, Any]:
    release_info = inspect_latest_release(repo, tag, asset_name, token)
    if release_info.get("release_poll_status") != "ready":
        return {**release_info, "release_install_status": "not_ready"}

    work_dir.mkdir(parents=True, exist_ok=True)
    target_dir.parent.mkdir(parents=True, exist_ok=True)

    release = get_release_by_tag(repo, tag, token)
    if release is None:
        raise GitHubReleaseError(f"release tag not found: {repo}:{tag}")

    archive_asset = find_asset(release, asset_name)
    sha_asset = find_asset(release, f"{asset_name}.sha256")
    if archive_asset is None or sha_asset is None:
        raise GitHubReleaseError(f"required assets not found in release {tag}")

    with tempfile.TemporaryDirectory(dir=str(work_dir), prefix=".cb-install-") as tmp_root:
        tmp_dir = Path(tmp_root)
        archive_path = download_asset(repo, int(archive_asset["id"]), token, tmp_dir / asset_name)
        sha_path = download_asset(repo, int(sha_asset["id"]), token, tmp_dir / f"{asset_name}.sha256")

        expected_sha = parse_sha256_file(sha_path, asset_name)
        actual_sha = compute_sha256(archive_path)
        if actual_sha != expected_sha:
            raise GitHubReleaseError(f"sha256 mismatch: expected {expected_sha}, got {actual_sha}")

        extract_root = tmp_dir / "extract"
        extract_root.mkdir(parents=True, exist_ok=True)
        safe_extract(archive_path, extract_root)
        staged_dir = extract_root / "qlib_data"
        if not staged_dir.is_dir():
            raise GitHubReleaseError("archive does not contain top-level qlib_data directory")

        backup_path = atomic_replace(staged_dir, target_dir)
        if backup_path and backup_path.exists():
            shutil.rmtree(backup_path)

    return {
        **release_info,
        "release_install_status": "success",
        "release_installed_at": datetime.now(timezone.utc).isoformat(),
        "release_install_dir": str(target_dir),
        "release_already_consumed": False,
    }
