"""
YouTube Data API v3 module — OAuth 2.0 (offline access)
Handles: auth, video upload, live broadcast scheduling, broadcast management
"""
import os, json, time, logging, threading
from pathlib import Path

log = logging.getLogger(__name__)

_TOKEN_FILE = Path(__file__).parent / "youtube_token.json"
_SCOPE = "https://www.googleapis.com/auth/youtube"
_AUTH_BASE = "https://accounts.google.com/o/oauth2/v2/auth"
_TOKEN_URL = "https://oauth2.googleapis.com/token"

_CLIENT_ID     = ""
_CLIENT_SECRET = ""
_REDIRECT_URI  = "urn:ietf:wg:oauth:2.0:oob"

_token_lock = threading.Lock()
_token_data: dict = {}


# ── helpers ──────────────────────────────────────────────────────────────────

def _load_credentials():
    global _CLIENT_ID, _CLIENT_SECRET
    _CLIENT_ID     = os.environ.get("YT_CLIENT_ID", "")
    _CLIENT_SECRET = os.environ.get("YT_CLIENT_SECRET", "")


def _load_token():
    global _token_data
    with _token_lock:
        if _TOKEN_FILE.exists():
            try:
                _token_data = json.loads(_TOKEN_FILE.read_text())
            except Exception:
                _token_data = {}


def _save_token(data: dict):
    global _token_data
    with _token_lock:
        _token_data = data
        _TOKEN_FILE.write_text(json.dumps(data, indent=2))


def _refresh_access_token() -> str:
    """Use refresh_token to get a fresh access_token."""
    import requests as _req
    with _token_lock:
        rt = _token_data.get("refresh_token", "")
    if not rt:
        raise RuntimeError("لا يوجد refresh_token — قم بتسجيل الدخول أولاً: /ytauth")
    resp = _req.post(_TOKEN_URL, data={
        "client_id":     _CLIENT_ID,
        "client_secret": _CLIENT_SECRET,
        "refresh_token": rt,
        "grant_type":    "refresh_token",
    }, timeout=15)
    resp.raise_for_status()
    new_data = resp.json()
    with _token_lock:
        _token_data["access_token"] = new_data["access_token"]
        _token_data["expires_at"]   = time.time() + new_data.get("expires_in", 3600) - 60
        _TOKEN_FILE.write_text(json.dumps(_token_data, indent=2))
    return _token_data["access_token"]


def _get_access_token() -> str:
    """Return valid access_token, refreshing if needed."""
    with _token_lock:
        exp = _token_data.get("expires_at", 0)
        tok = _token_data.get("access_token", "")
    if tok and time.time() < exp:
        return tok
    return _refresh_access_token()


def _headers() -> dict:
    return {"Authorization": f"Bearer {_get_access_token()}",
            "Accept": "application/json"}


# ── public: auth ──────────────────────────────────────────────────────────────

def is_authenticated() -> bool:
    _load_token()
    with _token_lock:
        return bool(_token_data.get("refresh_token"))


def get_auth_url() -> str:
    """Generate OAuth consent URL for the user to open."""
    _load_credentials()
    if not _CLIENT_ID:
        raise RuntimeError("YT_CLIENT_ID غير موجود في متغيرات البيئة")
    from urllib.parse import urlencode
    params = {
        "client_id":     _CLIENT_ID,
        "redirect_uri":  _REDIRECT_URI,
        "response_type": "code",
        "scope":         _SCOPE,
        "access_type":   "offline",
        "prompt":        "consent",
    }
    return f"{_AUTH_BASE}?{urlencode(params)}"


def exchange_code(code: str) -> dict:
    """Exchange authorization code for tokens and save them."""
    import requests as _req
    _load_credentials()
    resp = _req.post(_TOKEN_URL, data={
        "client_id":     _CLIENT_ID,
        "client_secret": _CLIENT_SECRET,
        "code":          code.strip(),
        "redirect_uri":  _REDIRECT_URI,
        "grant_type":    "authorization_code",
    }, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if "refresh_token" not in data:
        raise RuntimeError(f"لم يُرجع الـ API refresh_token: {data}")
    data["expires_at"] = time.time() + data.get("expires_in", 3600) - 60
    _save_token(data)
    log.info("YouTube OAuth tokens saved.")
    return data


def get_channel_info() -> dict:
    """Return basic info about the authenticated YouTube channel."""
    import requests as _req
    r = _req.get(
        "https://www.googleapis.com/youtube/v3/channels",
        headers=_headers(),
        params={"part": "snippet,statistics", "mine": "true"},
        timeout=10,
    )
    r.raise_for_status()
    items = r.json().get("items", [])
    if not items:
        raise RuntimeError("لم يتم العثور على قناة يوتيوب مرتبطة بهذا الحساب")
    ch = items[0]
    sn = ch["snippet"]
    st = ch["statistics"]
    return {
        "id":          ch["id"],
        "title":       sn["title"],
        "description": sn.get("description", ""),
        "subscribers": st.get("subscriberCount", "مخفي"),
        "views":       st.get("viewCount", "0"),
        "videos":      st.get("videoCount", "0"),
    }


# ── public: video upload ──────────────────────────────────────────────────────

def upload_video(
    file_path: str,
    title: str = "فيديو جديد",
    description: str = "",
    privacy: str = "private",
    tags: list[str] | None = None,
    progress_cb=None,
) -> dict:
    """
    Upload a video to YouTube.
    progress_cb(bytes_sent, total) called periodically.
    Returns the created video resource dict.
    """
    import requests as _req

    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"الملف غير موجود: {file_path}")

    total_size = file_path.stat().st_size
    suffix = file_path.suffix.lower()
    mime_map = {".mp4": "video/mp4", ".avi": "video/avi",
                ".mkv": "video/x-matroska", ".mov": "video/quicktime",
                ".webm": "video/webm", ".flv": "video/x-flv"}
    content_type = mime_map.get(suffix, "video/mp4")

    metadata = {
        "snippet": {
            "title":       title,
            "description": description,
            "tags":        tags or [],
            "categoryId":  "22",
        },
        "status": {
            "privacyStatus":           privacy,
            "selfDeclaredMadeForKids": False,
        },
    }

    # Step 1 — initiate resumable upload session
    init_url = (
        "https://www.googleapis.com/upload/youtube/v3/videos"
        "?uploadType=resumable&part=snippet,status"
    )
    init_resp = _req.post(
        init_url,
        headers={**_headers(), "Content-Type": "application/json",
                 "X-Upload-Content-Type": content_type,
                 "X-Upload-Content-Length": str(total_size)},
        json=metadata,
        timeout=30,
    )
    init_resp.raise_for_status()
    upload_url = init_resp.headers["Location"]
    log.info(f"[YT Upload] Resumable session: {upload_url[:60]}…")

    # Step 2 — chunked upload
    CHUNK = 5 * 1024 * 1024  # 5 MB
    sent = 0
    with open(file_path, "rb") as fh:
        while sent < total_size:
            chunk = fh.read(CHUNK)
            end   = sent + len(chunk) - 1
            resp  = _req.put(
                upload_url,
                headers={
                    "Content-Length": str(len(chunk)),
                    "Content-Range":  f"bytes {sent}-{end}/{total_size}",
                    "Content-Type":   content_type,
                },
                data=chunk,
                timeout=120,
            )
            if resp.status_code in (200, 201):
                video = resp.json()
                log.info(f"[YT Upload] Done: {video['id']}")
                if progress_cb:
                    progress_cb(total_size, total_size)
                return video
            elif resp.status_code == 308:
                sent = end + 1
                if progress_cb:
                    progress_cb(sent, total_size)
            else:
                raise RuntimeError(f"Upload error {resp.status_code}: {resp.text[:200]}")

    raise RuntimeError("Upload ended without completion")


# ── public: live broadcasts ───────────────────────────────────────────────────

def schedule_broadcast(
    title: str,
    start_time_iso: str,
    description: str = "",
    privacy: str = "public",
    resolution: str = "1080p",
    frame_rate: str = "30fps",
) -> dict:
    """
    Create a scheduled YouTube live broadcast + bind a stream.
    start_time_iso: ISO 8601 e.g. "2025-04-10T18:00:00+03:00"
    Returns: {broadcast_id, stream_id, stream_key, rtmp_url, watch_url}
    """
    import requests as _req

    # 1 — Create liveBroadcast
    bc_resp = _req.post(
        "https://www.googleapis.com/youtube/v3/liveBroadcasts?part=id,snippet,status,contentDetails",
        headers={**_headers(), "Content-Type": "application/json"},
        json={
            "snippet": {
                "title":              title,
                "description":        description,
                "scheduledStartTime": start_time_iso,
            },
            "status": {
                "privacyStatus":           privacy,
                "selfDeclaredMadeForKids": False,
            },
            "contentDetails": {
                "enableAutoStart": True,
                "enableAutoStop":  True,
                "recordFromStart": True,
                "enableDvr":       True,
            },
        },
        timeout=15,
    )
    bc_resp.raise_for_status()
    broadcast = bc_resp.json()
    broadcast_id = broadcast["id"]
    log.info(f"[YT Schedule] Broadcast created: {broadcast_id}")

    # 2 — Create liveStream (encoder config)
    ls_resp = _req.post(
        "https://www.googleapis.com/youtube/v3/liveStreams?part=id,snippet,cdn,status",
        headers={**_headers(), "Content-Type": "application/json"},
        json={
            "snippet": {"title": f"Stream for {title}"},
            "cdn": {
                "frameRate":        frame_rate,
                "ingestionType":    "rtmp",
                "resolution":       resolution,
            },
        },
        timeout=15,
    )
    ls_resp.raise_for_status()
    stream = ls_resp.json()
    stream_id  = stream["id"]
    ingestion  = stream["cdn"]["ingestionInfo"]
    rtmp_url   = ingestion["ingestionAddress"]
    stream_key = ingestion["streamName"]
    log.info(f"[YT Schedule] Stream created: {stream_id}")

    # 3 — Bind broadcast ↔ stream
    bind_resp = _req.post(
        f"https://www.googleapis.com/youtube/v3/liveBroadcasts/bind"
        f"?id={broadcast_id}&part=id,contentDetails&streamId={stream_id}",
        headers=_headers(),
        timeout=15,
    )
    bind_resp.raise_for_status()
    log.info(f"[YT Schedule] Bound {broadcast_id} ↔ {stream_id}")

    return {
        "broadcast_id": broadcast_id,
        "stream_id":    stream_id,
        "stream_key":   stream_key,
        "rtmp_url":     rtmp_url,
        "rtmp_full":    f"{rtmp_url}/{stream_key}",
        "watch_url":    f"https://youtu.be/{broadcast_id}",
        "title":        title,
        "start_time":   start_time_iso,
    }


def list_broadcasts(status: str = "all") -> list[dict]:
    """List broadcasts. status: upcoming | live | completed | all"""
    import requests as _req
    params = {
        "part":        "id,snippet,status",
        "mine":        "true",
        "maxResults":  "10",
    }
    if status != "all":
        params["broadcastStatus"] = status
    r = _req.get(
        "https://www.googleapis.com/youtube/v3/liveBroadcasts",
        headers=_headers(), params=params, timeout=10,
    )
    r.raise_for_status()
    items = r.json().get("items", [])
    result = []
    for it in items:
        sn = it["snippet"]
        st = it["status"]
        result.append({
            "id":          it["id"],
            "title":       sn["title"],
            "start":       sn.get("scheduledStartTime", ""),
            "status":      st.get("lifeCycleStatus", ""),
            "privacy":     st.get("privacyStatus", ""),
            "watch_url":   f"https://youtu.be/{it['id']}",
        })
    return result


def delete_broadcast(broadcast_id: str) -> bool:
    import requests as _req
    r = _req.delete(
        "https://www.googleapis.com/youtube/v3/liveBroadcasts",
        headers=_headers(),
        params={"id": broadcast_id},
        timeout=10,
    )
    return r.status_code == 204


def transition_broadcast(broadcast_id: str, to_status: str) -> bool:
    """to_status: testing | live | complete"""
    import requests as _req
    r = _req.post(
        "https://www.googleapis.com/youtube/v3/liveBroadcasts/transition",
        headers=_headers(),
        params={"id": broadcast_id, "broadcastStatus": to_status, "part": "id,status"},
        timeout=15,
    )
    return r.status_code == 200


# ── public: videos management ─────────────────────────────────────────────────

def list_videos(max_results: int = 10) -> list[dict]:
    """List the latest uploads on the channel."""
    import requests as _req
    # get uploads playlist id
    ch_r = _req.get(
        "https://www.googleapis.com/youtube/v3/channels",
        headers=_headers(),
        params={"part": "contentDetails", "mine": "true"},
        timeout=10,
    )
    ch_r.raise_for_status()
    pl_id = ch_r.json()["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    pl_r = _req.get(
        "https://www.googleapis.com/youtube/v3/playlistItems",
        headers=_headers(),
        params={"part": "snippet", "playlistId": pl_id, "maxResults": max_results},
        timeout=10,
    )
    pl_r.raise_for_status()
    result = []
    for it in pl_r.json().get("items", []):
        sn = it["snippet"]
        vid = sn["resourceId"]["videoId"]
        result.append({
            "id":      vid,
            "title":   sn["title"],
            "url":     f"https://youtu.be/{vid}",
            "date":    sn.get("publishedAt", "")[:10],
        })
    return result


def delete_video(video_id: str) -> bool:
    import requests as _req
    r = _req.delete(
        "https://www.googleapis.com/youtube/v3/videos",
        headers=_headers(),
        params={"id": video_id},
        timeout=10,
    )
    return r.status_code == 204


# init on import
_load_credentials()
_load_token()
