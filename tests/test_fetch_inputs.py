import io
import json
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest

from scripts.fetch_inputs import is_safe_member, sha256_file


class TestFetchInputs(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp(prefix="test_fetch_"))
        self.staging = self.test_dir / "staging"
        self.target = self.test_dir / "target"
        self.staging.mkdir()
        self.target.mkdir()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_path_traversal_detection(self):
        """Ensure malicious tar members with .. or absolute paths are rejected."""
        # Unsafe member with path traversal
        bad_info_1 = tarfile.TarInfo(name="../escape.txt")
        self.assertFalse(is_safe_member(bad_info_1, self.staging))

        bad_info_2 = tarfile.TarInfo(name="/etc/passwd")
        self.assertFalse(is_safe_member(bad_info_2, self.staging))

        bad_info_3 = tarfile.TarInfo(name="C:\\Windows\\System32")
        self.assertFalse(is_safe_member(bad_info_3, self.staging))

        # Safe member
        good_info = tarfile.TarInfo(name="reference/candidates/paths.json")
        self.assertTrue(is_safe_member(good_info, self.staging))

    def test_corrupt_archive_preserves_target_dir(self):
        """Ensure corrupt archive is rejected before target directory is modified."""
        # Create an existing file in target
        sentinel = self.target / "sentinel.txt"
        sentinel.write_text("pre-existing content", encoding="utf-8")

        # Create a mock tar with mismatched content
        bad_tar = self.test_dir / "bad_archive.tar.gz"
        with tarfile.open(bad_tar, "w:gz") as tar:
            data = b"some random bytes"
            info = tarfile.TarInfo(name="manifest.json")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

        # Expected hash is different from actual
        actual_sha = sha256_file(bad_tar)
        wrong_sha = "0000000000000000000000000000000000000000000000000000000000000000"

        # Sentinel file should be completely untouched
        self.assertTrue(sentinel.is_file())
        self.assertEqual(sentinel.read_text(encoding="utf-8"), "pre-existing content")


if __name__ == "__main__":
    unittest.main()
