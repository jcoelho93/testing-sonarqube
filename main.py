import requests

def fetch_user_details(user_id: str) -> dict:
    if not isinstance(user_id, str) or not user_id.strip():
        raise ValueError("user_id must be a non-empty string")

    url = f"https://api.example.com/users/{user_id.strip()}"
    response = requests.get(url, timeout=5.0)
    response.raise_for_status()  # Raise on HTTP error status

    # Parse JSON defensively
    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError("Invalid JSON received from user service") from exc

    if not isinstance(data, dict):
        raise RuntimeError("Unexpected JSON structure from user service")

    return data

