"""A persistent, single-process Bitcask-style store for binary keys and values."""

from __future__ import annotations

import argparse
import fcntl
import os
import re
import shutil
import struct
import sys
import threading
import uuid
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

DEFAULT_SEGMENT_SIZE = 16 * 1024 * 1024
MAX_KEY_SIZE = 1024 * 1024
MAX_VALUE_SIZE = 64 * 1024 * 1024

_MAGIC = b"BC01"
_PUT, _DELETE = 1, 2
_HEADER = struct.Struct(">4sBIII")
_CRC = struct.Struct(">I")
_HEADER_SIZE = _HEADER.size + _CRC.size
_GENERATION = re.compile(r"gen-[0-9a-f]{32}")
_SEGMENT = re.compile(r"[0-9]{20}\.data")


class BitcaskError(Exception):
    """Base class for storage errors."""


class DatabaseLockedError(BitcaskError):
    """Another open instance owns this database directory."""


class CorruptionError(BitcaskError):
    """A record or the database layout failed validation."""


class RecoveryRequiredError(BitcaskError):
    """The result of a disk operation is uncertain. Close and reopen the store."""


class _IncompleteRecord(CorruptionError):
    pass


@dataclass(frozen=True, slots=True)
class _Location:
    segment: int
    offset: int


def _sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _ensure_directory(path: Path) -> None:
    missing = []
    parent = path
    while not parent.exists():
        missing.append(parent)
        parent = parent.parent
    for directory in reversed(missing):
        directory.mkdir(mode=0o700, exist_ok=True)
        _sync_directory(directory.parent)


def _publish(root: Path, generation: str) -> None:
    pending = root / "CURRENT.tmp"
    with pending.open("wb") as file:
        file.write(generation.encode("ascii") + b"\n")
        file.flush()
        os.fsync(file.fileno())
    # Persist the new generation's directory entry before publishing its name.
    _sync_directory(root)
    os.replace(pending, root / "CURRENT")
    _sync_directory(root)


def _check_uninitialized(root: Path) -> None:
    # An interrupted first open can leave an empty generation behind.
    for directory in root.iterdir():
        if _GENERATION.fullmatch(directory.name) and directory.is_dir():
            for segment in directory.iterdir():
                if _SEGMENT.fullmatch(segment.name) and segment.stat().st_size:
                    raise CorruptionError("missing CURRENT file in a database with data")


def _payload_crc(key: bytes, value: bytes) -> int:
    return zlib.crc32(value, zlib.crc32(key))


def _read_record(file: BinaryIO) -> tuple[int, bytes, bytes]:
    position = f"{file.name}:{file.tell()}"
    header = file.read(_HEADER.size)
    checksum = file.read(_CRC.size)
    if len(header) != _HEADER.size or len(checksum) != _CRC.size:
        raise _IncompleteRecord(f"{position}: incomplete record header")

    if zlib.crc32(header) != _CRC.unpack(checksum)[0]:
        raise CorruptionError(f"{position}: header checksum mismatch")

    magic, operation, key_size, value_size, payload_crc = _HEADER.unpack(header)
    if (
        magic != _MAGIC
        or operation not in (_PUT, _DELETE)
        or not 0 < key_size <= MAX_KEY_SIZE
        or value_size > MAX_VALUE_SIZE
        or (operation == _DELETE and value_size != 0)
    ):
        raise CorruptionError(f"{position}: invalid record header")

    key = file.read(key_size)
    value = file.read(value_size)
    if len(key) != key_size or len(value) != value_size:
        raise _IncompleteRecord(f"{position}: incomplete record payload")
    if _payload_crc(key, value) != payload_crc:
        raise CorruptionError(f"{position}: payload checksum mismatch")
    return operation, key, value


class _Log:
    def __init__(
        self, directory: Path, segment_size: int, active_id: int, active: BinaryIO
    ) -> None:
        self.directory = directory
        self.segment_size = segment_size
        self.active_id = active_id
        self.active = active
        self.size = active.seek(0, os.SEEK_END)
        self.index: dict[bytes, _Location] = {}

    @staticmethod
    def _segment_path(directory: Path, segment_id: int) -> Path:
        return directory / f"{segment_id:020d}.data"

    @classmethod
    def _create_segment(cls, directory: Path, segment_id: int) -> BinaryIO:
        file = cls._segment_path(directory, segment_id).open("x+b", buffering=0)
        try:
            os.fsync(file.fileno())
            _sync_directory(directory)
        except BaseException:
            file.close()
            raise
        return file

    @classmethod
    def create(cls, root: Path, segment_size: int) -> _Log:
        directory = root / f"gen-{uuid.uuid4().hex}"
        directory.mkdir(mode=0o700)
        active = cls._create_segment(directory, 1)
        try:
            return cls(directory, segment_size, 1, active)
        except BaseException:
            active.close()
            raise

    @classmethod
    def open(cls, directory: Path, segment_size: int) -> _Log:
        if not directory.is_dir():
            raise CorruptionError(f"missing data directory: {directory}")
        paths = sorted(
            path for path in directory.iterdir() if _SEGMENT.fullmatch(path.name)
        )
        if not paths or any(
            path != cls._segment_path(directory, number)
            for number, path in enumerate(paths, 1)
        ):
            raise CorruptionError(f"missing segment in {directory}")

        active = paths[-1].open("r+b", buffering=0)
        try:
            log = cls(directory, segment_size, len(paths), active)
            for number, path in enumerate(paths[:-1], 1):
                with path.open("rb") as file:
                    log._replay(file, number)
            log.size = log._replay(active, log.active_id, repair_tail=True)
            return log
        except BaseException:
            active.close()
            raise

    def _replay(self, file: BinaryIO, segment: int, *, repair_tail: bool = False) -> int:
        end = file.seek(0, os.SEEK_END)
        file.seek(0)
        while file.tell() < end:
            offset = file.tell()
            try:
                operation, key, _ = _read_record(file)
            except _IncompleteRecord:
                if not repair_tail:
                    raise
                file.truncate(offset)
                os.fsync(file.fileno())
                return offset
            if operation == _DELETE:
                self.index.pop(key, None)
            else:
                self.index[key] = _Location(segment, offset)
        return end

    def append(self, operation: int, key: bytes, value: bytes, *, durable: bool) -> None:
        record_size = _HEADER_SIZE + len(key) + len(value)
        if self.size and self.size + record_size > self.segment_size:
            os.fsync(self.active.fileno())
            new_id = self.active_id + 1
            new_active = self._create_segment(self.directory, new_id)
            old_active = self.active
            self.active, self.active_id, self.size = new_active, new_id, 0
            old_active.close()

        header = _HEADER.pack(
            _MAGIC, operation, len(key), len(value), _payload_crc(key, value)
        )
        self.active.seek(self.size)
        for part in (header, _CRC.pack(zlib.crc32(header)), key, value):
            if part and self.active.write(part) != len(part):
                raise OSError(f"short write to {self.active.name}")
        if durable:
            os.fsync(self.active.fileno())

        if operation == _DELETE:
            del self.index[key]
        else:
            self.index[key] = _Location(self.active_id, self.size)
        self.size += record_size

    def read(self, key: bytes, location: _Location) -> bytes:
        if location.segment == self.active_id:
            self.active.seek(location.offset)
            operation, stored_key, value = _read_record(self.active)
        else:
            path = self._segment_path(self.directory, location.segment)
            with path.open("rb") as file:
                file.seek(location.offset)
                operation, stored_key, value = _read_record(file)

        if operation != _PUT or stored_key != key:
            raise CorruptionError(f"index mismatch at {location}")
        return value


class Bitcask:
    """Open a directory-backed key-value database. Use as a context manager."""

    def __init__(
        self, path: str | os.PathLike[str], *, segment_size: int = DEFAULT_SEGMENT_SIZE
    ) -> None:
        if type(segment_size) is not int or segment_size <= 0:
            raise ValueError("segment_size must be a positive integer")

        self._mutex = threading.Lock()
        self._closed = False
        self._needs_recovery = False
        self._root = Path(path).resolve()
        _ensure_directory(self._root)
        self._lock = (self._root / "LOCK").open("a+b")
        log = None
        try:
            try:
                fcntl.flock(self._lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise DatabaseLockedError(
                    f"database is already open: {self._root}"
                ) from error

            try:
                current = (self._root / "CURRENT").read_bytes()
            except FileNotFoundError:
                _check_uninitialized(self._root)
                log = _Log.create(self._root, segment_size)
                _publish(self._root, log.directory.name)
            else:
                match = re.fullmatch(rb"(gen-[0-9a-f]{32})\n", current)
                if match is None:
                    raise CorruptionError("invalid CURRENT file")
                generation = match[1].decode("ascii")
                log = _Log.open(self._root / generation, segment_size)
                # Make the observed pointer durable before retiring any old generations.
                _sync_directory(self._root)
            self._log = log
            self._discard_old_generations()
        except BaseException:
            try:
                if log is not None:
                    log.active.close()
            finally:
                self._lock.close()
            raise

    @staticmethod
    def _check_key(key: bytes) -> None:
        if not isinstance(key, bytes):
            raise TypeError("key must be bytes")
        if not 0 < len(key) <= MAX_KEY_SIZE:
            raise ValueError(f"key length must be between 1 and {MAX_KEY_SIZE} bytes")

    def _check_open(self) -> None:
        if self._closed:
            raise ValueError("database is closed")
        if self._needs_recovery:
            raise RecoveryRequiredError("close and reopen the database after a disk error")

    def _append(self, operation: int, key: bytes, value: bytes) -> None:
        try:
            self._log.append(operation, key, value, durable=True)
        except BaseException:
            self._needs_recovery = True
            raise

    def put(self, key: bytes, value: bytes) -> None:
        """Write or replace a value and sync the record to disk."""
        self._check_key(key)
        if not isinstance(value, bytes):
            raise TypeError("value must be bytes")
        if len(value) > MAX_VALUE_SIZE:
            raise ValueError(f"value length must be at most {MAX_VALUE_SIZE} bytes")
        with self._mutex:
            self._check_open()
            self._append(_PUT, key, value)

    def get(self, key: bytes) -> bytes | None:
        """Return the value, or None if there is no such key."""
        self._check_key(key)
        with self._mutex:
            self._check_open()
            location = self._log.index.get(key)
            if location is None:
                return None
            return self._log.read(key, location)

    def delete(self, key: bytes) -> bool:
        """Delete a key. Return whether the key previously existed."""
        self._check_key(key)
        with self._mutex:
            self._check_open()
            if key not in self._log.index:
                return False
            self._append(_DELETE, key, b"")
            return True

    def _discard_old_generations(self) -> None:
        for directory in self._root.iterdir():
            if directory != self._log.directory and _GENERATION.fullmatch(directory.name):
                # The pointer is already durable. Retry obsolete-file cleanup on next open.
                shutil.rmtree(directory, ignore_errors=True)

    def compact(self) -> None:
        """Copy live records into a fresh generation and atomically switch to it."""
        with self._mutex:
            self._check_open()
            old = self._log
            new = _Log.create(self._root, old.segment_size)
            try:
                for key, location in old.index.items():
                    new.append(_PUT, key, old.read(key, location), durable=False)
                os.fsync(new.active.fileno())
                _sync_directory(new.directory)
            except BaseException:
                new.active.close()
                shutil.rmtree(new.directory, ignore_errors=True)
                raise

            try:
                _publish(self._root, new.directory.name)
            except BaseException:
                # The rename might already be visible. Preserve both generations.
                self._needs_recovery = True
                new.active.close()
                raise
            self._log = new
            old.active.close()
            self._discard_old_generations()

    def close(self) -> None:
        with self._mutex:
            if self._closed:
                return
            self._closed = True
            try:
                self._log.active.close()
            finally:
                self._lock.close()

    def __enter__(self) -> Bitcask:
        with self._mutex:
            self._check_open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Bitcask-style key-value database")
    parser.add_argument("--dir", default="bitcask-data", help="database directory")
    parser.add_argument(
        "--segment-size", type=int, default=DEFAULT_SEGMENT_SIZE, metavar="BYTES"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    put = commands.add_parser("put", help="set or replace a value")
    put.add_argument("key")
    put.add_argument("value")
    for name in ("get", "delete"):
        commands.add_parser(name).add_argument("key")
    commands.add_parser("compact", help="reclaim space occupied by obsolete records")
    args = parser.parse_args()

    try:
        with Bitcask(args.dir, segment_size=args.segment_size) as db:
            if args.command == "compact":
                db.compact()
                return 0
            key = args.key.encode("utf-8")
            if args.command == "put":
                db.put(key, args.value.encode("utf-8"))
                return 0
            if args.command == "delete" and db.delete(key):
                return 0
            if args.command == "get":
                value = db.get(key)
                if value is not None:
                    sys.stdout.buffer.write(value)
                    return 0
            print("bitcask: key not found", file=sys.stderr)
            return 1
    except (OSError, ValueError, BitcaskError) as error:
        print(f"bitcask: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
