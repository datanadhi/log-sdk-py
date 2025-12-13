"""Fallback server communication with compressed batch uploads."""

import gzip
import io
import json

import requests


def _encode_jsonl_gz(dicts: list[dict]) -> bytes:
    """Encode list of dicts as gzipped JSONL.

    Args:
        dicts: List of dictionaries to encode

    Returns:
        Gzipped JSONL bytes
    """
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="w") as gz:
        for obj in dicts:
            line = json.dumps(obj).encode("utf-8") + b"\n"
            gz.write(line)
    return buf.getvalue()


def send(
    session: requests.Session, server_host: str, payloads: list[dict], api_key: str
) -> dict:
    """Send batch of logs to fallback server.

    Args:
        session: Requests session
        server_host: Server URL
        payloads: List of log payloads
        api_key: API key for authentication

    Returns:
        Dict with keys: success, status_code, is_failure, is_unavailable
    """
    try:
        compressed_data = _encode_jsonl_gz(payloads)

        response = session.post(
            f"{server_host}/upload",
            data=compressed_data,
            headers={
                "Content-Type": "application/octet-stream",
                "DATANADHI_API_KEY": api_key,
            },
            timeout=30,  # Longer timeout for batch upload
        )

        return {
            "success": 200 <= response.status_code < 300,
            "status_code": response.status_code,
            "is_failure": 300 <= response.status_code <= 500,
            "is_unavailable": response.status_code > 500,
        }
    except requests.RequestException:
        # Connection error, DNS failure, timeout
        return {
            "success": False,
            "status_code": None,
            "is_failure": False,
            "is_unavailable": True,
        }
