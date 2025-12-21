"""HTTP operations (read-only)."""

from typing import Optional

import requests

from .guards import GuardError, get_timeout_secs


def http_request(
    url: str,
    method: str = "GET",
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    data: Optional[str] = None,
    json_data: Optional[dict] = None,
    timeout: Optional[int] = None,
    allow_post: bool = False,
) -> dict:
    """
    Make HTTP request (read-only, GET only by default).
    
    Args:
        url: Request URL
        method: HTTP method (default: "GET", POST requires allow_post=True)
        headers: Optional headers dict
        params: Optional query parameters
        data: Optional request body (string, only for POST)
        json_data: Optional JSON body (dict, only for POST)
        timeout: Optional timeout in seconds (default: MCPKIT_TIMEOUT_SECS)
        allow_post: If True, allows POST requests (default: False for safety)
    
    Returns:
        dict with status_code, headers, and body
    """
    method_upper = method.upper()
    
    # Only allow GET by default, POST requires explicit opt-in
    if method_upper == "POST" and not allow_post:
        raise GuardError("POST requests are disabled by default for safety. Set allow_post=True to enable.")
    
    if method_upper not in ["GET", "POST"]:
        raise GuardError(f"Method must be GET or POST, got: {method}")
    
    if timeout is None:
        timeout = get_timeout_secs()
    
    try:
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
        else:  # POST
            if json_data:
                response = requests.post(url, headers=headers, params=params, json=json_data, timeout=timeout)
            elif data:
                response = requests.post(url, headers=headers, params=params, data=data, timeout=timeout)
            else:
                response = requests.post(url, headers=headers, params=params, timeout=timeout)
        
        response.raise_for_status()
        
        # Try to decode as JSON (dict or list), fallback to text
        try:
            body = response.json()
            content_type = "json"
        except ValueError:
            body = response.text
            content_type = "text"
        
        return {
            "url": response.url,
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "body": body,
            "content_type": content_type,
            "size": len(response.content),
        }
    except requests.RequestException as e:
        raise GuardError(f"HTTP request failed: {e}")

