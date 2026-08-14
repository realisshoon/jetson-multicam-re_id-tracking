from __future__ import annotations

import hashlib
import os
import socket
import sqlite3
import tempfile
import struct
import zlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class CaptureCacheError(ValueError):
    pass


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


@dataclass(frozen=True)
class CaptureCacheSettings:
    enabled: bool
    storage_root: Path
    jetson_a_base_url: str
    allowed_host: str
    allowed_port: int
    allowed_url_prefix: str
    body_source_root: PurePosixPath
    face_source_root: PurePosixPath
    body_public_prefix: str
    face_public_prefix: str
    connect_timeout_seconds: float
    read_timeout_seconds: float
    max_file_bytes: int


@dataclass(frozen=True)
class CaptureSpec:
    capture_key: str
    request_id: str
    capture_type: str
    source_url: str
    quality_score: float | None
    captured_at: str
    index: int


def settings_from_document(
    document: dict[str, Any],
    project_root: Path,
) -> CaptureCacheSettings:
    raw_root = Path(str(document.get("storage_root", "data/captures")))
    storage_root = raw_root if raw_root.is_absolute() else project_root / raw_root
    prefix = "/" + str(document.get("allowed_url_prefix", "/captures/")).strip("/") + "/"
    return CaptureCacheSettings(
        enabled=bool(document.get("enabled", True)),
        storage_root=storage_root.resolve(),
        jetson_a_base_url=str(
            document.get("jetson_a_base_url", "http://10.10.20.56:8000")
        ).rstrip("/"),
        allowed_host=str(document.get("allowed_host", "10.10.20.56")).lower(),
        allowed_port=int(document.get("allowed_port", 8000)),
        allowed_url_prefix=prefix,
        body_source_root=PurePosixPath(
            str(document.get("body_source_root", "/home/aidl/work/pj/outputs/captures/A"))
        ),
        face_source_root=PurePosixPath(
            str(document.get("face_source_root", "/home/aidl/work/pj/outputs/captures/A_face"))
        ),
        body_public_prefix="/" + str(
            document.get("body_public_prefix", "/captures/body/")
        ).strip("/") + "/",
        face_public_prefix="/" + str(
            document.get("face_public_prefix", "/captures/face/")
        ).strip("/") + "/",
        connect_timeout_seconds=float(document.get("connect_timeout_seconds", 2.0)),
        read_timeout_seconds=float(document.get("read_timeout_seconds", 5.0)),
        max_file_bytes=int(document.get("max_file_bytes", 10 * 1024 * 1024)),
    )


def _quality(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, min(1.0, parsed))


def validate_source_url(raw_url: str, settings: CaptureCacheSettings) -> str:
    parsed = urlsplit(raw_url)
    expected_scheme = urlsplit(settings.jetson_a_base_url).scheme.lower()
    if parsed.scheme.lower() not in {"http", "https"}:
        raise CaptureCacheError("URL_SCHEME_NOT_ALLOWED")
    if parsed.scheme.lower() != expected_scheme:
        raise CaptureCacheError("URL_SCHEME_NOT_ALLOWED")
    if (parsed.hostname or "").lower() != settings.allowed_host:
        raise CaptureCacheError("URL_HOST_NOT_ALLOWED")
    try:
        port = parsed.port or (443 if parsed.scheme.lower() == "https" else 80)
    except ValueError as error:
        raise CaptureCacheError("URL_PORT_INVALID") from error
    if port != settings.allowed_port:
        raise CaptureCacheError("URL_PORT_NOT_ALLOWED")
    if parsed.username or parsed.password:
        raise CaptureCacheError("URL_CREDENTIALS_NOT_ALLOWED")
    if not parsed.path.startswith(settings.allowed_url_prefix):
        raise CaptureCacheError("URL_PATH_NOT_ALLOWED")
    if ".." in PurePosixPath(parsed.path).parts:
        raise CaptureCacheError("URL_PATH_NOT_ALLOWED")
    if parsed.query or parsed.fragment:
        raise CaptureCacheError("URL_QUERY_NOT_ALLOWED")
    return raw_url


def source_path_to_url(
    raw_path_or_url: Any,
    capture_type: str,
    settings: CaptureCacheSettings,
) -> str:
    if not isinstance(raw_path_or_url, str) or not raw_path_or_url.strip():
        raise CaptureCacheError("SOURCE_PATH_MISSING")
    raw = raw_path_or_url.strip()
    if urlsplit(raw).scheme:
        return validate_source_url(raw, settings)
    if raw.startswith(settings.allowed_url_prefix):
        public_path = PurePosixPath(raw)
        if ".." in public_path.parts:
            raise CaptureCacheError("URL_PATH_NOT_ALLOWED")
        return validate_source_url(
            settings.jetson_a_base_url + quote(public_path.as_posix(), safe="/%"),
            settings,
        )
    path = PurePosixPath(raw)
    if not path.is_absolute():
        raise CaptureCacheError("SOURCE_PATH_NOT_ABSOLUTE")
    normalized_type = capture_type.upper()
    root = settings.face_source_root if normalized_type == "FACE" else settings.body_source_root
    public_prefix = (
        settings.face_public_prefix if normalized_type == "FACE" else settings.body_public_prefix
    )
    try:
        relative = path.relative_to(root)
    except ValueError as error:
        raise CaptureCacheError("SOURCE_PATH_OUTSIDE_ALLOWED_ROOT") from error
    if not relative.parts or ".." in relative.parts:
        raise CaptureCacheError("SOURCE_PATH_OUTSIDE_ALLOWED_ROOT")
    url = settings.jetson_a_base_url + public_prefix + quote(relative.as_posix(), safe="/")
    return validate_source_url(url, settings)


def parse_capture_specs(
    payload: dict[str, Any],
    request_id: str | None,
    captured_at: str,
    settings: CaptureCacheSettings,
) -> tuple[list[CaptureSpec], list[dict[str, Any]]]:
    if not request_id:
        return [], [{"reason": "REQUEST_ID_REQUIRED"}]
    candidates: list[tuple[str, Any, Any, Any]] = []
    explicit = payload.get("captures")
    if isinstance(explicit, list):
        for item in explicit:
            if not isinstance(item, dict):
                continue
            capture_type = str(
                item.get("capture_type", item.get("type", "BODY"))
            ).upper()
            source = item.get(
                "source_url",
                item.get(
                    "url",
                    item.get(
                        "public_path",
                        item.get("source_path", item.get("capture_path")),
                    ),
                ),
            )
            candidates.append(
                (capture_type, source, item.get("quality_score", item.get("quality")), item.get("capture_key"))
            )

    for capture_type, paths_key, quality_key in (
        ("BODY", "body_capture_paths", "body_qualities"),
        ("FACE", "face_capture_paths", "face_qualities"),
    ):
        paths = payload.get(paths_key)
        qualities = payload.get(quality_key)
        if isinstance(paths, list):
            for index, path in enumerate(paths):
                quality = qualities[index] if isinstance(qualities, list) and index < len(qualities) else None
                candidates.append((capture_type, path, quality, None))

    legacy = payload.get("capture_path")
    body_paths = payload.get("body_capture_paths")
    if legacy and not (isinstance(body_paths, list) and legacy in body_paths):
        candidates.append(("BODY", legacy, payload.get("quality"), None))

    specs: list[CaptureSpec] = []
    errors: list[dict[str, Any]] = []
    type_counts = {"BODY": 0, "FACE": 0}
    seen_sources: set[tuple[str, str]] = set()
    for capture_type, source, quality, explicit_key in candidates:
        normalized_type = "FACE" if capture_type == "FACE" else "BODY"
        if not isinstance(source, str):
            errors.append({"capture_type": normalized_type, "reason": "SOURCE_PATH_MISSING"})
            continue
        identity = (normalized_type, source.strip())
        if identity in seen_sources:
            continue
        seen_sources.add(identity)
        type_counts[normalized_type] += 1
        index = type_counts[normalized_type]
        key = str(explicit_key).strip() if explicit_key else f"{request_id}:{normalized_type}:{index}"
        try:
            url = source_path_to_url(source, normalized_type, settings)
        except CaptureCacheError as error:
            errors.append(
                {
                    "capture_key": key,
                    "capture_type": normalized_type,
                    "source": source,
                    "reason": str(error),
                }
            )
            continue
        specs.append(
            CaptureSpec(
                capture_key=key,
                request_id=request_id,
                capture_type=normalized_type,
                source_url=url,
                quality_score=_quality(quality),
                captured_at=captured_at,
                index=index,
            )
        )
    return specs, errors


def insert_capture_rows(
    connection: sqlite3.Connection,
    specs: list[CaptureSpec],
    journey_id: str,
    person_uid: str | None,
    created_at: str,
) -> None:
    for spec in specs:
        connection.execute(
            """
            INSERT INTO captures (
                capture_key, request_id, journey_id, person_uid, camera_id,
                capture_type, source_url, quality_score, captured_at,
                cache_status, created_at
            ) VALUES (?, ?, ?, ?, 'A', ?, ?, ?, ?, 'PENDING', ?)
            ON CONFLICT(capture_key) DO NOTHING
            """,
            (
                spec.capture_key,
                spec.request_id,
                journey_id,
                person_uid,
                spec.capture_type,
                spec.source_url,
                spec.quality_score,
                spec.captured_at,
                created_at,
            ),
        )


def insert_failed_capture_rows(
    connection: sqlite3.Connection,
    errors: list[dict[str, Any]],
    request_id: str | None,
    journey_id: str,
    captured_at: str,
    created_at: str,
) -> None:
    if not request_id:
        return
    for error in errors:
        capture_key = error.get("capture_key")
        if not capture_key:
            continue
        connection.execute(
            """
            INSERT INTO captures (
                capture_key, request_id, journey_id, person_uid, camera_id,
                capture_type, source_url, captured_at, cache_status,
                cache_error, created_at
            ) VALUES (?, ?, ?, NULL, 'A', ?, ?, ?, 'FAILED', ?, ?)
            ON CONFLICT(capture_key) DO NOTHING
            """,
            (
                str(capture_key),
                request_id,
                journey_id,
                str(error.get("capture_type") or "BODY"),
                str(error.get("source") or ""),
                captured_at,
                str(error.get("reason") or "INVALID_CAPTURE_SOURCE")[:500],
                created_at,
            ),
        )


def _set_read_timeout(response: Any, timeout: float) -> None:
    candidates = [
        getattr(getattr(response, "fp", None), "raw", None),
        getattr(response, "fp", None),
    ]
    for candidate in candidates:
        sock = getattr(candidate, "_sock", None)
        if sock is not None:
            sock.settimeout(timeout)
            return


def _decode_and_identify(path: Path) -> tuple[str, str]:
    try:
        from PIL import Image
    except (ImportError, OSError) as error:
        # A strict PNG fallback keeps diagnostics/tests usable in minimal
        # environments. Production JPEG decoding requires Pillow.
        data = path.read_bytes()
        if data.startswith(b"\x89PNG\r\n\x1a\n"):
            try:
                offset = 8
                width = height = bit_depth = color_type = interlace = None
                compressed = bytearray()
                saw_iend = False
                while offset + 12 <= len(data):
                    length = struct.unpack(">I", data[offset : offset + 4])[0]
                    chunk_type = data[offset + 4 : offset + 8]
                    end = offset + 12 + length
                    if end > len(data):
                        raise ValueError("truncated chunk")
                    chunk = data[offset + 8 : offset + 8 + length]
                    expected_crc = struct.unpack(">I", data[offset + 8 + length : end])[0]
                    if zlib.crc32(chunk_type + chunk) & 0xFFFFFFFF != expected_crc:
                        raise ValueError("invalid crc")
                    if chunk_type == b"IHDR":
                        width, height, bit_depth, color_type, _, _, interlace = struct.unpack(
                            ">IIBBBBB", chunk
                        )
                    elif chunk_type == b"IDAT":
                        compressed.extend(chunk)
                    elif chunk_type == b"IEND":
                        saw_iend = True
                        break
                    offset = end
                channels = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}.get(color_type)
                if (
                    not saw_iend
                    or not width
                    or not height
                    or channels is None
                    or bit_depth not in {1, 2, 4, 8, 16}
                    or interlace != 0
                ):
                    raise ValueError("unsupported png")
                decoded = zlib.decompress(bytes(compressed))
                row_bytes = (width * channels * bit_depth + 7) // 8
                if len(decoded) != height * (row_bytes + 1):
                    raise ValueError("invalid decoded size")
                if any(decoded[row * (row_bytes + 1)] > 4 for row in range(height)):
                    raise ValueError("invalid png filter")
                return "image/png", ".png"
            except (ValueError, struct.error, zlib.error) as decode_error:
                raise CaptureCacheError("IMAGE_DECODE_FAILED") from decode_error
        raise CaptureCacheError("IMAGE_DECODER_UNAVAILABLE") from error
    try:
        with Image.open(path) as image:
            image.verify()
        with Image.open(path) as image:
            image.load()
            image_format = str(image.format or "").upper()
    except Exception as error:
        raise CaptureCacheError("IMAGE_DECODE_FAILED") from error
    if image_format == "JPEG":
        return "image/jpeg", ".jpg"
    if image_format == "PNG":
        return "image/png", ".png"
    raise CaptureCacheError("IMAGE_FORMAT_NOT_ALLOWED")


def cache_capture(
    connection_factory: Any,
    capture_key: str,
    settings: CaptureCacheSettings,
) -> dict[str, Any]:
    with connection_factory() as connection:
        row = connection.execute(
            "SELECT * FROM captures WHERE capture_key = ?", (capture_key,)
        ).fetchone()
        if row is None:
            raise CaptureCacheError("CAPTURE_NOT_FOUND")
        if row["cache_status"] == "CACHED" and row["stored_path"]:
            return dict(row)
        source_url = validate_source_url(str(row["source_url"]), settings)
        captured_at = str(row["captured_at"])

    try:
        day = datetime.fromisoformat(captured_at).date().isoformat()
    except ValueError:
        day = datetime.now().date().isoformat()
    target_dir = settings.storage_root / day
    target_dir.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        opener = build_opener(_RejectRedirects())
        request = Request(source_url, headers={"User-Agent": "CCTV-Main-Capture-Cache/1.0"})
        with opener.open(request, timeout=settings.connect_timeout_seconds) as response:
            final_url = validate_source_url(response.geturl(), settings)
            if final_url != source_url:
                raise CaptureCacheError("HTTP_REDIRECT_NOT_ALLOWED")
            length = response.headers.get("Content-Length")
            if length is not None and int(length) > settings.max_file_bytes:
                raise CaptureCacheError("IMAGE_TOO_LARGE")
            _set_read_timeout(response, settings.read_timeout_seconds)
            digest = hashlib.sha256()
            total = 0
            with tempfile.NamedTemporaryFile(
                mode="wb", prefix=".capture-", suffix=".tmp", dir=target_dir, delete=False
            ) as output:
                temp_path = Path(output.name)
                while True:
                    chunk = response.read(64 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > settings.max_file_bytes:
                        raise CaptureCacheError("IMAGE_TOO_LARGE")
                    digest.update(chunk)
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
        if total == 0:
            raise CaptureCacheError("EMPTY_RESPONSE")
        mime_type, suffix = _decode_and_identify(temp_path)
        sha256 = digest.hexdigest()
        final_path = target_dir / f"{sha256}{suffix}"
        if final_path.exists():
            temp_path.unlink()
        else:
            os.replace(str(temp_path), str(final_path))
        temp_path = None
        relative_path = final_path.relative_to(settings.storage_root).as_posix()
        with connection_factory() as connection:
            connection.execute(
                """
                UPDATE captures SET stored_path = ?, sha256 = ?, mime_type = ?,
                    cache_status = 'CACHED', cache_error = NULL
                WHERE capture_key = ?
                """,
                (relative_path, sha256, mime_type, capture_key),
            )
            connection.commit()
            result = connection.execute(
                "SELECT * FROM captures WHERE capture_key = ?", (capture_key,)
            ).fetchone()
            return dict(result)
    except (CaptureCacheError, HTTPError, URLError, socket.timeout, TimeoutError, ValueError) as error:
        reason = str(error) or error.__class__.__name__
        if isinstance(error, HTTPError):
            reason = f"HTTP_{error.code}"
        elif isinstance(error, (socket.timeout, TimeoutError)):
            reason = "DOWNLOAD_TIMEOUT"
        elif isinstance(error, URLError):
            reason = "DOWNLOAD_ERROR:" + str(error.reason)
        with connection_factory() as connection:
            connection.execute(
                "UPDATE captures SET cache_status = 'FAILED', cache_error = ? WHERE capture_key = ?",
                (reason[:500], capture_key),
            )
            connection.commit()
        return {"capture_key": capture_key, "cache_status": "FAILED", "cache_error": reason[:500]}
    finally:
        if temp_path is not None:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass


def choose_automatic_representative(
    connection: sqlite3.Connection,
    person_uid: str | None,
    request_id: str,
    updated_at: str,
) -> int | None:
    if not person_uid:
        return None
    person = connection.execute(
        "SELECT representative_capture_id FROM persons WHERE person_uid = ?",
        (person_uid,),
    ).fetchone()
    if person is None or person["representative_capture_id"] is not None:
        return None
    row = connection.execute(
        """
        SELECT capture_id FROM captures
        WHERE request_id = ? AND person_uid = ? AND cache_status = 'CACHED'
        ORDER BY CASE capture_type WHEN 'FACE' THEN 0 ELSE 1 END,
                 COALESCE(quality_score, -1) DESC, capture_id ASC
        LIMIT 1
        """,
        (request_id, person_uid),
    ).fetchone()
    if row is None:
        return None
    capture_id = int(row["capture_id"])
    connection.execute(
        """
        UPDATE persons SET representative_capture_id = ?,
            representative_source = 'AUTO', representative_updated_at = ?
        WHERE person_uid = ? AND representative_capture_id IS NULL
        """,
        (capture_id, updated_at, person_uid),
    )
    return capture_id
