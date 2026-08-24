#!/usr/bin/env python3
"""Unit tests for image_staleness_check.py — CHG-101 stale-image guard.

Pure helpers get synthetic inputs; the git-backed functions run against a
real throwaway repository with pinned commit dates (GIT_AUTHOR_DATE /
GIT_COMMITTER_DATE) so source epochs are deterministic. Docker is never
touched: image_created_epoch is monkeypatched.

Run: python3 -m unittest discover -s code/01_platform/04_scripts/tests -v
"""
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import image_staleness_check as isc  # noqa: E402

EPOCH_A = 1787500000  # fixed "source last commit" epoch
EPOCH_B = EPOCH_A + 100


def make_repo(path: Path, filename: str = "src/file.txt", epoch: int = EPOCH_A):
    """Init a throwaway git repo with one commit at `epoch`."""
    repo = path / "repo"
    repo.mkdir()
    env = dict(os.environ,
               GIT_AUTHOR_DATE=f"@{epoch} +0000",
               GIT_COMMITTER_DATE=f"@{epoch} +0000",
               GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t",
               GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t")
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo,
                   env=env, check=True, capture_output=True)
    src = repo / filename
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("v1\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=repo, check=True, env=env,
                   capture_output=True)
    subprocess.run(["git", "commit", "-q", "-m", "one"], cwd=repo, check=True,
                   env=env, capture_output=True)
    return repo


class BuildServicesTest(unittest.TestCase):
    def test_build_services_parses_contexts(self):
        compose = {"services": {
            "a": {"build": {"context": "..", "dockerfile": "A/Dockerfile"},
                  "profiles": ["x"]},
            "b": {"image": "apache/fluss:0.9.1-incubating"},  # pull-only
        }}
        services = isc.build_services(compose)
        self.assertEqual(list(services), ["a"])
        self.assertEqual(services["a"]["context"], "..")
        self.assertEqual(services["a"]["dockerfile"], "A/Dockerfile")
        self.assertEqual(services["a"]["profile"], ["x"])

    def test_load_compose_rejects_missing_services(self):
        with self.assertRaises(ValueError):
            isc.load_compose(Path("/dev/null"))


class VerdictTest(unittest.TestCase):
    def test_fresh_when_image_newer(self):
        status, _ = isc.verdict(EPOCH_B, EPOCH_A, dirty=False)
        self.assertEqual(status, "FRESH")

    def test_stale_when_image_older(self):
        status, _ = isc.verdict(EPOCH_A, EPOCH_B, dirty=False)
        self.assertEqual(status, "STALE")

    def test_missing_image(self):
        status, detail = isc.verdict(None, EPOCH_A, dirty=False)
        self.assertEqual(status, "MISSING")
        self.assertIn("not built", detail)

    def test_no_history_sources(self):
        status, _ = isc.verdict(EPOCH_B, None, dirty=False)
        self.assertEqual(status, "NO-HISTORY")

    def test_dirty_wins_over_stale(self):
        status, _ = isc.verdict(EPOCH_A, EPOCH_B, dirty=True)
        self.assertEqual(status, "DIRTY-WARN")


class GitEpochTest(unittest.TestCase):
    def test_source_epoch_and_dirty(self):
        with tempfile.TemporaryDirectory() as td:
            repo = make_repo(Path(td))
            self.assertEqual(isc.source_epoch(
                repo, ["src/file.txt"]), EPOCH_A)
            self.assertFalse(isc.worktree_dirty(repo, ["src/file.txt"]))
            (repo / "src/file.txt").write_text("v2\n", encoding="utf-8")
            self.assertTrue(isc.worktree_dirty(repo, ["src/file.txt"]))

    def test_source_epoch_untracked_returns_none(self):
        with tempfile.TemporaryDirectory() as td:
            repo = make_repo(Path(td))
            self.assertIsNone(isc.source_epoch(repo, ["src/never.txt"]))

    def test_source_epoch_multiple_paths_uses_latest(self):
        with tempfile.TemporaryDirectory() as td:
            repo = make_repo(Path(td))
            env = dict(os.environ,
                       GIT_AUTHOR_DATE=f"@{EPOCH_B} +0000",
                       GIT_COMMITTER_DATE=f"@{EPOCH_B} +0000",
                       GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t",
                       GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t")
            other = repo / "other"
            other.mkdir()
            (other / "b.txt").write_text("b\n", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=repo, check=True,
                           env=env, capture_output=True)
            subprocess.run(["git", "commit", "-q", "-m", "two"], cwd=repo,
                           check=True, env=env, capture_output=True)
            self.assertEqual(isc.source_epoch(
                repo, ["src/file.txt", "other/b.txt"]), EPOCH_B)


class CheckServiceTest(unittest.TestCase):
    def test_check_service_fresh(self):
        with tempfile.TemporaryDirectory() as td:
            repo = make_repo(Path(td),
                             filename="code/02_services/04_executor/src/lib.rs")
            with mock.patch.object(isc, "image_created_epoch",
                                   return_value=EPOCH_A + 50):
                result = isc.check_service("nautilus", "img", repo)
            self.assertEqual(result["status"], "FRESH")
            self.assertEqual(result["image"], "img")

    def test_check_service_stale(self):
        with tempfile.TemporaryDirectory() as td:
            repo = make_repo(Path(td),
                             filename="code/02_services/04_executor/src/lib.rs")
            with mock.patch.object(isc, "image_created_epoch",
                                   return_value=EPOCH_A - 50):
                result = isc.check_service("nautilus", "img", repo)
            self.assertEqual(result["status"], "STALE")

    def test_check_service_fallback_to_compose(self):
        """Unlisted service uses the build context + dockerfile from compose."""
        with tempfile.TemporaryDirectory() as td:
            repo = make_repo(Path(td), filename="ctx/x.txt")
            compose_services = {"custom": {"context": "ctx",
                                           "dockerfile": "ctx/Dockerfile"}}
            with mock.patch.object(isc, "image_created_epoch",
                                   return_value=EPOCH_A + 50):
                result = isc.check_service("custom", "img", repo,
                                           compose_services)
            self.assertEqual(result["status"], "FRESH")

    def test_sources_overlay_is_consistent(self):
        # Every overlay path must exist in the real repo (catch typos early).
        repo_root = Path(__file__).resolve().parents[4]
        for service, paths in isc.SERVICE_SOURCES.items():
            for path in paths:
                self.assertTrue((repo_root / path).exists(),
                                f"{service}: {path} missing")
            self.assertGreaterEqual(len(paths), 1, service)


class MainTest(unittest.TestCase):
    def test_unknown_service_fails_usage(self):
        with tempfile.TemporaryDirectory() as td:
            repo = make_repo(Path(td))
            compose = td + "/compose.yml"
            Path(compose).write_text(
                textwrap.dedent("""\
                services:
                  ing: {build: {context: .}}
                """), encoding="utf-8")
            rc = isc.main(["--git-root", str(repo), "--compose", compose,
                           "--service", "nope"])
            self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()
