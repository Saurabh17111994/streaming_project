#!/usr/bin/env python3
"""Unit tests for evidence_ownership_check.py — the non-root ownership gate.

Ownership can't be set without root, so the check's stat_fn is injected with a
fake returning crafted (uid, gid, mode): one stat for the evidence root dir and
one per record basename. The corpus files still exist so the walk enumerates
them. Stdlib unittest, no third-party deps.

Run: python3 -m unittest discover -s code/01_platform/04_scripts/tests -v
"""
import os
import sys
import tempfile
import types
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import evidence_ownership_check  # noqa: E402

UID = 10001
GID = 10001
HOST_UID = 1000
HOST_GID = 1000
DIR_2775 = 0o2775


def fake_stat(corpus_dir, by_basename, dir_stat):
    """stat_fn: dir_stat (uid, gid, mode) for the corpus root, else per file."""
    def fn(path):
        if path == corpus_dir:
            return types.SimpleNamespace(
                st_uid=dir_stat[0], st_gid=dir_stat[1], st_mode=dir_stat[2])
        uid, gid, mode = by_basename[os.path.basename(path)]
        return types.SimpleNamespace(st_uid=uid, st_gid=gid, st_mode=mode)
    return fn


class CheckEvidenceDirTest(unittest.TestCase):
    def _corpus(self, tmp, records):
        """Create one apply.json per record name under its own subdir."""
        for name in records:
            sub = os.path.join(tmp, "r_" + name)
            os.makedirs(sub, exist_ok=True)
            with open(os.path.join(sub, "apply.json"), "w") as fh:
                fh.write("{}")
        return tmp

    def test_container_corpus_with_contract_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._corpus(tmp, ["a", "b"])
            problems, (checked, cw, host) = evidence_ownership_check.check_evidence_dir(
                tmp, UID, GID,
                fake_stat(tmp, {"apply.json": (UID, GID, 0o664)},
                          (UID, GID, DIR_2775)))
            self.assertEqual(problems, [])
            self.assertEqual((checked, cw, host), (2, 2, 0))

    def test_root_owned_dir_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._corpus(tmp, ["a"])
            problems, _ = evidence_ownership_check.check_evidence_dir(
                tmp, UID, GID,
                fake_stat(tmp, {"apply.json": (UID, GID, 0o664)}, (0, 0, DIR_2775)))
            self.assertEqual(len(problems), 1)
            self.assertIn("root-owned", problems[0])

    def test_dir_missing_setgid_or_group_write_fails(self):
        for mode, label in ((0o2754, "no group-write"), (0o0775, "no setgid")):
            with tempfile.TemporaryDirectory() as tmp:
                self._corpus(tmp, ["a"])
                problems, _ = evidence_ownership_check.check_evidence_dir(
                    tmp, UID, GID,
                    fake_stat(tmp, {"apply.json": (UID, GID, 0o664)},
                              (UID, GID, mode)))
                self.assertEqual(len(problems), 1, label)
                self.assertIn("missing setgid+group-write", problems[0], label)

    def test_container_written_not_group_writable_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._corpus(tmp, ["a"])
            problems, _ = evidence_ownership_check.check_evidence_dir(
                tmp, UID, GID,
                fake_stat(tmp, {"apply.json": (UID, GID, 0o644)}, (UID, GID, DIR_2775)))
            self.assertEqual(len(problems), 1)
            self.assertIn("NOT group-writable", problems[0])

    def test_container_written_wrong_group_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._corpus(tmp, ["a"])
            problems, _ = evidence_ownership_check.check_evidence_dir(
                tmp, UID, GID,
                fake_stat(tmp, {"apply.json": (UID, HOST_GID, 0o664)},
                          (UID, GID, DIR_2775)))
            self.assertEqual(len(problems), 1)
            self.assertIn(f"group {HOST_GID} != engine GID {GID}", problems[0])

    def test_root_owned_record_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._corpus(tmp, ["a"])
            problems, _ = evidence_ownership_check.check_evidence_dir(
                tmp, UID, GID,
                fake_stat(tmp, {"apply.json": (0, 0, 0o644)}, (UID, GID, DIR_2775)))
            self.assertEqual(len(problems), 1)
            self.assertIn("root-owned", problems[0])

    def test_host_owned_corpus_out_of_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._corpus(tmp, ["a"])
            problems, (checked, cw, host) = evidence_ownership_check.check_evidence_dir(
                tmp, UID, GID,
                fake_stat(tmp, {"apply.json": (HOST_UID, HOST_GID, 0o644)},
                          (HOST_UID, HOST_GID, 0o2775)))
            self.assertEqual(problems, [])
            self.assertEqual((checked, cw, host), (1, 0, 1))

    def test_missing_corpus_is_vacuous_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = os.path.join(tmp, "nope")
            problems, counts = evidence_ownership_check.check_evidence_dir(missing, UID)
            self.assertEqual(problems, [])
            self.assertEqual(counts, (0, 0, 0))

    def test_non_record_files_ignored(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "notes.txt"), "w") as fh:
                fh.write("not a record")
            os.makedirs(os.path.join(tmp, "r1"))
            with open(os.path.join(tmp, "r1", "other.json"), "w") as fh:
                fh.write("{}")
            problems, counts = evidence_ownership_check.check_evidence_dir(
                tmp, UID, GID,
                fake_stat(tmp, {"notes.txt": (0, 0, 0o644),
                                "other.json": (UID, GID, 0o644)},
                          (UID, GID, DIR_2775)))
            self.assertEqual(problems, [])
            self.assertEqual(counts, (0, 0, 0))


class ModuleDefaultsTest(unittest.TestCase):
    def test_defaults_point_at_repo_evidence_root(self):
        self.assertTrue(evidence_ownership_check.EVIDENCE_DIR.endswith(
            os.path.join("logs", "ddl-apply")))
        self.assertEqual(evidence_ownership_check.CONTAINER_UID, 10001)
        self.assertEqual(evidence_ownership_check.CONTAINER_GID, 10001)


if __name__ == "__main__":
    unittest.main()
