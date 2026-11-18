"""Turn an approved file change into a GitHub pull request, or a patch file when GitHub isn't configured."""
import base64
from pathlib import Path

import httpx

from .settings import Settings

API = "https://api.github.com"


def publish_change(settings: Settings, branch: str, path_in_repo: str, new_content: str, diff: str,
                   title: str, body: str) -> str:
    """Open (or reuse) a PR for the change. Returns the PR URL, or `patch:<file>` in dry-run mode."""
    if not settings.opens_real_pull_requests:
        settings.outbox_dir.mkdir(parents=True, exist_ok=True)
        patch_file = settings.outbox_dir / f"{branch.replace('/', '-')}.patch"
        patch_file.write_text(f"# {title}\n\n{body}\n\n{diff}")
        return f"patch:{patch_file}"
    return _open_pull_request(settings, branch, path_in_repo, new_content, title, body)


def _open_pull_request(settings: Settings, branch: str, path_in_repo: str, new_content: str,
                       title: str, body: str) -> str:
    repo, base = settings.github_repo, settings.github_base_branch
    headers = {"Authorization": f"Bearer {settings.github_token}", "Accept": "application/vnd.github+json",
               "X-GitHub-Api-Version": "2022-11-28"}
    with httpx.Client(base_url=API, headers=headers, timeout=30) as github:
        existing = github.get(f"/repos/{repo}/pulls", params={"head": f"{repo.split('/')[0]}:{branch}", "state": "open"})
        existing.raise_for_status()
        if existing.json():
            return existing.json()[0]["html_url"]

        base_sha = github.get(f"/repos/{repo}/git/ref/heads/{base}").raise_for_status().json()["object"]["sha"]
        created = github.post(f"/repos/{repo}/git/refs", json={"ref": f"refs/heads/{branch}", "sha": base_sha})
        if created.status_code not in (201, 422):  # 422: branch already exists from an earlier attempt
            created.raise_for_status()

        full_path = str(Path(settings.github_config_root) / path_in_repo)
        current = github.get(f"/repos/{repo}/contents/{full_path}", params={"ref": branch})
        payload = {"message": title, "branch": branch,
                   "content": base64.b64encode(new_content.encode()).decode()}
        if current.status_code == 200:
            payload["sha"] = current.json()["sha"]
        github.put(f"/repos/{repo}/contents/{full_path}", json=payload).raise_for_status()

        pull = github.post(f"/repos/{repo}/pulls", json={"title": title, "head": branch, "base": base, "body": body})
        pull.raise_for_status()
        return pull.json()["html_url"]
