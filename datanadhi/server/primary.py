"""Primary server communication."""

import requests


def is_healthy(session: requests.Session, server_host: str) -> bool:
    """Check if primary server responds with 2xx status."""
    try:
        response = session.get(f"{server_host}/", timeout=2)
        return 200 <= response.status_code < 300
    except requests.RequestException:
        return False


def send(
    session: requests.Session, server_host: str, payload: dict, api_key: str
) -> dict:
    """Send log to primary server. Returns status dict."""
    try:
        response = session.post(
            f"{server_host}/log",
            json=payload,
            headers={"DATANADHI_API_KEY": api_key},
            timeout=10,
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
