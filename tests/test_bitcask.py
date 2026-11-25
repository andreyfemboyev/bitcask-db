import os
import random
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from bitcask import Bitcask, CorruptionError, DatabaseLockedError, RecoveryRequiredError


class BitcaskTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name) / "nested" / "database"

    def generation(self):
        return self.directory / (self.directory / "CURRENT").read_text().strip()

    def segments(self):
        return sorted(self.generation().glob("*.data"))

    def cli(self, *arguments):
        script = Path(__file__).resolve().parents[1] / "bitcask.py"
        return subprocess.run(
            [sys.executable, str(script), "--dir", str(self.directory), *arguments],
            capture_output=True,
            check=False,
        )

    def test_binary_data_and_tombstones_survive_reopen(self):
        with Bitcask(self.directory, segment_size=64) as db:
            db.put(b"\x00\xff", b"\xfe\x00")
            db.put(b"\x00\xff", b"")
            db.put(b"other", b"value")
            self.assertTrue(db.delete(b"other"))
            self.assertFalse(db.delete(b"other"))
            self.assertGreater(len(self.segments()), 1)

        with Bitcask(self.directory) as db:
            self.assertEqual(db.get(b"\x00\xff"), b"")
            self.assertIsNone(db.get(b"other"))
            db.put(b"other", b"new")
            self.assertEqual(db.get(b"other"), b"new")

    def test_compaction_removes_old_versions_and_tombstones(self):
        with Bitcask(self.directory, segment_size=110) as db:
            for number in range(16):
                db.put(b"live", bytes([number]) * 45)
                db.put(b"removed", b"old value")
            db.delete(b"removed")
            db.put(b"empty", b"")
            old_generation = self.generation()
            before = sum(path.stat().st_size for path in self.segments())
            db.compact()
            self.assertFalse(old_generation.exists())
            after = sum(path.stat().st_size for path in self.segments())
            self.assertLess(after, before)
            self.assertEqual(db.get(b"live"), b"\x0f" * 45)
            self.assertEqual(db.get(b"empty"), b"")
            self.assertIsNone(db.get(b"removed"))

        with Bitcask(self.directory, segment_size=110) as db:
            self.assertIsNone(db.get(b"removed"))
            db.delete(b"live")
            db.delete(b"empty")
            db.compact()
            self.assertEqual([path.stat().st_size for path in self.segments()], [0])

        with Bitcask(self.directory) as db:
            db.put(b"after", b"compaction")
            self.assertEqual(db.get(b"after"), b"compaction")
            self.assertIsNone(db.get(b"live"))

    def test_incomplete_active_tail_is_truncated(self):
        with Bitcask(self.directory) as db:
            db.put(b"stable", b"saved")
            segment = self.segments()[0]
            stable_size = segment.stat().st_size
            db.put(b"unfinished", b"value")

        records = segment.read_bytes()
        for extra in (3, len(records) - stable_size - 1):
            with self.subTest(extra=extra):
                segment.write_bytes(records[: stable_size + extra])
                with Bitcask(self.directory) as db:
                    self.assertEqual(db.get(b"stable"), b"saved")
                    self.assertIsNone(db.get(b"unfinished"))
                    self.assertEqual(segment.stat().st_size, stable_size)
                    db.put(b"recovered", b"yes")
                with Bitcask(self.directory) as db:
                    self.assertEqual(db.get(b"recovered"), b"yes")

    def test_active_checksum_error_is_reported_without_truncation(self):
        for damaged_byte in (7, -1):
            with self.subTest(damaged_byte=damaged_byte):
                directory = self.directory / str(damaged_byte)
                with Bitcask(directory) as db:
                    db.put(b"key", b"value")
                generation = (directory / "CURRENT").read_text().strip()
                segment = next((directory / generation).glob("*.data"))
                data = bytearray(segment.read_bytes())
                data[damaged_byte] ^= 1
                segment.write_bytes(data)
                with self.assertRaisesRegex(CorruptionError, "checksum mismatch"):
                    Bitcask(directory)
                self.assertEqual(segment.read_bytes(), data)

    def test_incomplete_archived_segment_is_reported(self):
        with Bitcask(self.directory, segment_size=30) as db:
            db.put(b"first", b"value")
            db.put(b"second", b"value")
            archived, _ = self.segments()
        data = archived.read_bytes()[:-1]
        archived.write_bytes(data)
        with self.assertRaisesRegex(CorruptionError, "incomplete record"):
            Bitcask(self.directory)
        self.assertEqual(archived.read_bytes(), data)

    def test_missing_pointer_does_not_discard_existing_data(self):
        with Bitcask(self.directory) as db:
            db.put(b"key", b"value")
            segment = self.segments()[0]
        (self.directory / "CURRENT").unlink()
        with self.assertRaisesRegex(CorruptionError, "missing CURRENT"):
            Bitcask(self.directory)
        self.assertTrue(segment.exists())

    def test_publish_failure_on_either_side_of_rename(self):
        replace = os.replace
        for after_rename in (False, True):
            with self.subTest(after_rename=after_rename):
                with Bitcask(self.directory, segment_size=40) as db:
                    db.put(b"live", b"value")
                    db.put(b"removed", b"value")
                    db.delete(b"removed")
                    old = self.generation()

                    def interrupted_replace(source, destination):
                        if after_rename:
                            replace(source, destination)
                        raise OSError("simulated interruption")

                    with patch("bitcask.os.replace", side_effect=interrupted_replace):
                        with self.assertRaisesRegex(OSError, "simulated interruption"):
                            db.compact()
                    self.assertTrue(old.exists())
                    self.assertEqual(self.generation() != old, after_rename)
                    with self.assertRaises(RecoveryRequiredError):
                        db.put(b"late", b"write")

                with Bitcask(self.directory) as db:
                    self.assertEqual(db.get(b"live"), b"value")
                    self.assertIsNone(db.get(b"removed"))
                    self.assertIsNone(db.get(b"late"))
                    self.assertEqual(list(self.directory.glob("gen-*")), [self.generation()])

    def test_fsync_failure_requires_reopen(self):
        with Bitcask(self.directory) as db:
            db.put(b"stable", b"value")
            with patch("bitcask.os.fsync", side_effect=OSError("simulated I/O error")):
                with self.assertRaisesRegex(OSError, "simulated I/O error"):
                    db.put(b"uncertain", b"value")
            with self.assertRaises(RecoveryRequiredError):
                db.delete(b"stable")
        with Bitcask(self.directory) as db:
            self.assertEqual(db.get(b"stable"), b"value")
            db.put(b"after", b"recovery")

    def test_parallel_threads_and_exclusive_process_lock(self):
        with Bitcask(self.directory, segment_size=130) as db:
            with self.assertRaises(DatabaseLockedError):
                Bitcask(self.directory)
            self.assertEqual(self.cli("get", "busy").returncode, 2)

            def write_and_read(number):
                key = str(number).encode("ascii")
                db.put(key, key)
                return db.get(key)

            with ThreadPoolExecutor(max_workers=4) as executor:
                self.assertEqual(
                    list(executor.map(write_and_read, range(32))),
                    [str(number).encode("ascii") for number in range(32)],
                )
        self.assertEqual(self.cli("get", "17").stdout, b"17")

    def test_cli_exit_codes_and_exact_output(self):
        result = self.cli("put", "fruit", "яблоко")
        self.assertEqual(result.returncode, 0, result.stderr)
        result = self.cli("get", "fruit")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "яблоко".encode())
        self.assertEqual(self.cli("delete", "fruit").returncode, 0)
        self.assertEqual(self.cli("compact").returncode, 0)
        result = self.cli("get", "fruit")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, b"")

    def test_model_across_rotations_reopens_and_compactions(self):
        random_source = random.Random(18)
        keys = [bytes([index]) for index in range(12)]
        expected = {}
        for _ in range(5):
            with Bitcask(self.directory, segment_size=100) as db:
                for _ in range(40):
                    key = random_source.choice(keys)
                    operation = random_source.choice(("put", "put", "delete", "compact"))
                    if operation == "put":
                        value = random_source.randbytes(random_source.randrange(100))
                        db.put(key, value)
                        expected[key] = value
                    elif operation == "delete":
                        self.assertEqual(db.delete(key), key in expected)
                        expected.pop(key, None)
                    else:
                        db.compact()
                    for current_key in keys:
                        self.assertEqual(db.get(current_key), expected.get(current_key))


if __name__ == "__main__":
    unittest.main()
