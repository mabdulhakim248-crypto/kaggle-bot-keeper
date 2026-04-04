"""
Live radio streaming module — streams audio from a URL directly
into Telegram channels via RTMP using Pyrogram (raw API) + FFmpeg.
Uses a user account session (SESSION_STRING) since bots cannot manage
live streams directly.
"""
import asyncio
import logging
import os
import threading
import time

import json

log = logging.getLogger(__name__)

_STATE_FILE   = "streams_state.json"
_GITHUB_TOKEN = os.environ.get("GH_SYNC_TOKEN", "")
_GITHUB_REPO  = "mabdulhakim248-crypto/kaggle-bot-keeper"
_GITHUB_PATH  = "streams_state.json"


def _sync_state_to_github(state: dict):
    try:
        import base64, urllib.request
        content_b64 = base64.b64encode(json.dumps(state).encode()).decode()
        headers = {
            "Authorization": f"token {_GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json",
        }
        url = f"https://api.github.com/repos/{_GITHUB_REPO}/contents/{_GITHUB_PATH}"
        sha = None
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as resp:
                sha = json.loads(resp.read())["sha"]
        except Exception:
            pass
        body = {"message": "sync streams state", "content": content_b64}
        if sha:
            body["sha"] = sha
        req2 = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers, method="PUT")
        urllib.request.urlopen(req2, timeout=10)
        log.info(f"Stream state synced to GitHub: {len(state.get('tg',{}))} TG, {len(state.get('yt',{}))} YT")
    except Exception as e:
        log.warning(f"Failed to sync state to GitHub: {e}")


def _download_state_from_github():
    try:
        import base64, urllib.request
        url = f"https://api.github.com/repos/{_GITHUB_REPO}/contents/{_GITHUB_PATH}"
        headers = {
            "Authorization": f"token {_GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
        }
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            content = base64.b64decode(data["content"]).decode()
            state = json.loads(content)
            with open(_STATE_FILE, "w") as f:
                json.dump(state, f)
            log.info(f"Stream state downloaded from GitHub: {len(state.get('tg',{}))} TG, {len(state.get('yt',{}))} YT")
            return True
    except Exception as e:
        log.warning(f"Failed to download state from GitHub: {e}")
        return False


def _get_config():
    return (
        os.environ.get("API_ID", ""),
        os.environ.get("API_HASH", ""),
        os.environ.get("SESSION_STRING", ""),
    )

_app = None
_me = None
_loop: asyncio.AbstractEventLoop | None = None
_thread: threading.Thread | None = None
_ready = threading.Event()
_init_error: str | None = None
_active_streams: dict[int, dict] = {}
_yt_streams: dict[str, dict] = {}


def save_streams_state():
    state = {
        "tg": {
            str(cid): {
                "url": info.get("yt_url") or info.get("url",""),
                "title": info.get("title","بث مباشر"),
                "is_yt": info.get("is_yt", False),
                "yt_url": info.get("yt_url",""),
            }
            for cid, info in _active_streams.items() if info.get("url") or info.get("yt_url")
        },
        "yt": {
            tag: {"url": info.get("url",""), "title": info.get("title","بث مباشر")}
            for tag, info in _yt_streams.items() if info.get("url")
        }
    }
    try:
        with open(_STATE_FILE, "w") as f:
            json.dump(state, f)
        log.info(f"Stream state saved: {len(state['tg'])} TG, {len(state['yt'])} YT")
        threading.Thread(target=_sync_state_to_github, args=(state,), daemon=True).start()
    except Exception as e:
        log.warning(f"Failed to save stream state: {e}")


def load_streams_state() -> dict:
    if not os.path.exists(_STATE_FILE):
        _download_state_from_github()
    try:
        if os.path.exists(_STATE_FILE):
            with open(_STATE_FILE) as f:
                state = json.load(f)
            log.info(f"Stream state loaded: {len(state.get('tg',{}))} TG, {len(state.get('yt',{}))} YT")
            return state
    except Exception as e:
        log.warning(f"Failed to load stream state: {e}")
    return {"tg": {}, "yt": {}}


def _ensure_config():
    api_id, api_hash, session_string = _get_config()
    if not api_id or not api_hash:
        raise ValueError(
            "يجب تعيين API_ID و API_HASH\n"
            "احصل عليهم من https://my.telegram.org"
        )
    if not session_string:
        raise ValueError(
            "يجب تعيين SESSION_STRING\n"
            "شغّل generate_session.py لإنشاء session string من حسابك\n"
            "ثم أضفه كـ Secret باسم SESSION_STRING"
        )
    return api_id, api_hash, session_string


def _bg_thread():
    global _app, _loop, _init_error

    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)

    try:
        from pyrogram import Client

        api_id, api_hash, session_string = _get_config()

        _app = Client(
            "radio_user",
            api_id=int(api_id),
            api_hash=api_hash,
            session_string=session_string,
            in_memory=True,
        )

        async def _run():
            global _me
            await _app.start()

            _me = await _app.get_me()
            log.info(f"Session account: {_me.first_name} (id={_me.id})")

            try:
                count = 0
                async for dialog in _app.get_dialogs():
                    count += 1
                    if count >= 50:
                        break
                log.info(f"Loaded {count} dialogs into peer cache")
            except Exception as e:
                log.warning(f"Could not preload dialogs: {e}")

            _ready.set()
            while True:
                await asyncio.sleep(3600)

        _loop.run_until_complete(_run())
    except Exception as e:
        _init_error = str(e)
        log.error(f"Pyrogram init error: {e}", exc_info=True)
        _ready.set()


def _init_client():
    global _thread, _init_error

    if _thread is not None and _ready.is_set():
        if _init_error:
            raise RuntimeError(f"فشل تهيئة البث: {_init_error}")
        return

    if _thread is not None:
        _ready.wait(timeout=30)
        if _init_error:
            raise RuntimeError(f"فشل تهيئة البث: {_init_error}")
        return

    _ensure_config()
    _init_error = None

    _thread = threading.Thread(target=_bg_thread, daemon=True)
    _thread.start()

    if not _ready.wait(timeout=30):
        raise RuntimeError("فشل في تهيئة عميل البث (timeout)")

    if _init_error:
        raise RuntimeError(f"فشل تهيئة البث: {_init_error}")

    log.info("Radio client ready")


def _run_async(coro, timeout=90):
    if _loop is None:
        raise RuntimeError("Client not initialized")
    future = asyncio.run_coroutine_threadsafe(coro, _loop)
    return future.result(timeout=timeout)


def is_streaming(chat_id: int) -> bool:
    return chat_id in _active_streams


def get_stream_info(chat_id: int) -> dict | None:
    return _active_streams.get(chat_id)


def start_stream(chat_id: int, stream_url: str, title: str = "بث مباشر"):
    _init_client()

    if chat_id in _active_streams:
        stop_stream(chat_id)

    _active_streams[chat_id] = {
        "url": stream_url,
        "title": title,
        "started_at": time.time(),
        "is_yt": False,
    }

    try:
        _run_async(_async_play(chat_id, stream_url))
        log.info(f"Live stream started in chat {chat_id}: {stream_url}")
        save_streams_state()
    except Exception:
        _active_streams.pop(chat_id, None)
        log.error(f"start_stream failed for chat {chat_id}", exc_info=True)
        raise


async def _resolve_peer(chat_id: int):
    try:
        peer = await _app.resolve_peer(chat_id)
        log.info(f"Peer resolved for {chat_id}: {peer}")
        return
    except Exception as e:
        log.warning(f"resolve_peer failed for {chat_id}: {e}")

    if str(chat_id).startswith("-100"):
        real_id = int(str(chat_id)[4:])
        try:
            from pyrogram.raw.types import InputPeerChannel
            from pyrogram.raw.functions.channels import GetChannels, GetFullChannel
            from pyrogram.raw.types import InputChannel

            peer = InputPeerChannel(channel_id=real_id, access_hash=0)
            await _app.resolve_peer(chat_id)
        except Exception:
            pass

    try:
        chat = await _app.get_chat(chat_id)
        log.info(f"get_chat resolved for {chat_id}: {chat.title}")
        return
    except Exception as e:
        log.warning(f"get_chat failed for {chat_id}: {e}")

    try:
        async for dialog in _app.get_dialogs():
            if dialog.chat.id == chat_id:
                log.info(f"Found chat {chat_id} in dialogs: {dialog.chat.title}")
                return
    except Exception as e:
        log.warning(f"get_dialogs scan failed: {e}")


_BG_IMAGE = os.path.join(os.path.dirname(__file__), "radio_bg.jpg")


def _build_audio_ffmpeg(stream_url: str) -> str:
    ua = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    referer = "https://radio.garden/"

    return (
        f"ffmpeg "
        f"-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 "
        f"-reconnect_at_eof 1 "
        f"-user_agent \"{ua}\" "
        f"-headers \"Referer: {referer}\\r\\n\" "
        f"-i {stream_url} "
        f"-f s16le -ac 2 -ar 48000 -v quiet pipe:1"
    )


def _build_rtmp_ffmpeg_yt(stream_url: str, rtmp_target: str, image_path: str | None) -> list[str]:
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "warning"]

    cmd += [
        "-reconnect", "1", "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-i", stream_url,
    ]
    cmd += [
        "-c:v", "libx264", "-preset", "ultrafast",
        "-pix_fmt", "yuv420p", "-s", "640x360", "-r", "25",
        "-b:v", "500k", "-maxrate", "500k", "-bufsize", "1000k",
        "-c:a", "aac", "-ar", "48000", "-b:a", "128k", "-ac", "2",
    ]
    log.info("RTMP FFmpeg (YT source): video + audio")

    cmd += ["-f", "flv", rtmp_target]
    return cmd


def _build_rtmp_ffmpeg(stream_url: str, rtmp_target: str, image_path: str | None) -> list[str]:
    ua = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    referer = "https://radio.garden/"

    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "warning"]

    if image_path and os.path.isfile(image_path):
        cmd += ["-re", "-stream_loop", "-1", "-loop", "1", "-i", image_path]
        cmd += [
            "-reconnect", "1", "-reconnect_streamed", "1",
            "-reconnect_delay_max", "5", "-reconnect_at_eof", "1",
            "-user_agent", ua,
            "-headers", f"Referer: {referer}\r\n",
            "-i", stream_url,
        ]
        cmd += [
            "-map", "0:v", "-map", "1:a",
            "-c:v", "libx264", "-preset", "ultrafast",
            "-tune", "stillimage",
            "-pix_fmt", "yuv420p", "-s", "640x360", "-r", "25",
            "-b:v", "500k", "-maxrate", "500k", "-bufsize", "1000k",
            "-c:a", "aac", "-ar", "48000", "-b:a", "128k", "-ac", "2",
        ]
        log.info("RTMP FFmpeg: static image + audio stream")
    else:
        log.warning("radio_bg.jpg not found — black screen + audio")
        cmd += [
            "-reconnect", "1", "-reconnect_streamed", "1",
            "-reconnect_delay_max", "5", "-reconnect_at_eof", "1",
            "-user_agent", ua,
            "-headers", f"Referer: {referer}\r\n",
            "-i", stream_url,
        ]
        cmd += [
            "-f", "lavfi", "-i", "color=c=black:s=640x360:r=25",
            "-map", "1:v", "-map", "0:a",
            "-c:v", "libx264", "-preset", "ultrafast",
            "-pix_fmt", "yuv420p", "-b:v", "300k",
            "-c:a", "aac", "-ar", "48000", "-b:a", "128k", "-ac", "2",
        ]

    cmd += ["-f", "flv", rtmp_target]
    return cmd


async def _yt_watchdog(chat_id: int, yt_url: str, rtmp_target: str):
    """Background watchdog: monitors FFmpeg and restarts with a fresh URL when it exits."""
    import subprocess
    log.info(f"[YT watchdog] Started for chat {chat_id}")
    await asyncio.sleep(10)

    while True:
        if chat_id not in _active_streams:
            log.info(f"[YT watchdog] Stream {chat_id} was stopped — watchdog exiting")
            return

        info = _active_streams.get(chat_id, {})
        proc = info.get("proc")

        if proc is None or proc.poll() is None:
            await asyncio.sleep(30)
            continue

        ret = proc.poll()
        try:
            stderr_out = proc.stderr.read().decode(errors="replace")[-400:]
        except Exception:
            stderr_out = ""
        log.warning(f"[YT watchdog] FFmpeg exited (code {ret}) for {chat_id}: {stderr_out}")

        if chat_id not in _active_streams:
            log.info(f"[YT watchdog] Stream {chat_id} was stopped — watchdog exiting")
            return

        log.info(f"[YT watchdog] Refreshing YouTube URL for chat {chat_id}…")
        try:
            loop = asyncio.get_event_loop()
            yt_info = await loop.run_in_executor(None, lambda: extract_youtube_url(yt_url))
            fresh_url = yt_info["url"]
        except Exception as e:
            log.error(f"[YT watchdog] Failed to get fresh URL: {e} — retrying in 60s")
            await asyncio.sleep(60)
            continue

        cmd = _build_rtmp_ffmpeg_yt(fresh_url, rtmp_target, _BG_IMAGE)
        log.info(f"[YT watchdog] Restarting FFmpeg for chat {chat_id}")
        try:
            new_proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        except Exception as e:
            log.error(f"[YT watchdog] Failed to start FFmpeg: {e}")
            await asyncio.sleep(30)
            continue

        if chat_id in _active_streams:
            _active_streams[chat_id]["proc"] = new_proc

        await asyncio.sleep(5)
        ret2 = new_proc.poll()
        if ret2 is not None:
            stderr2 = new_proc.stderr.read().decode(errors="replace")[-300:]
            log.error(f"[YT watchdog] FFmpeg exited immediately (code {ret2}): {stderr2}")
            await asyncio.sleep(20)
        else:
            log.info(f"[YT watchdog] FFmpeg restarted OK (PID {new_proc.pid}) for {chat_id}")


async def _async_play_yt(chat_id: int, stream_url: str, yt_url: str = ""):
    import subprocess
    import random
    from pyrogram.raw.functions.channels import GetFullChannel
    from pyrogram.raw.functions.phone import CreateGroupCall, GetGroupCallStreamRtmpUrl

    await _resolve_peer(chat_id)
    channel_peer = await _app.resolve_peer(chat_id)

    log.info(f"[YT→TG] Checking RTMP group-call state for {chat_id}…")
    full = await _app.invoke(GetFullChannel(channel=channel_peer))
    active_call = getattr(full.full_chat, 'call', None)

    if active_call is None:
        log.info("[YT→TG] No active live stream — creating RTMP group call…")
        try:
            await _app.invoke(
                CreateGroupCall(
                    peer=channel_peer,
                    random_id=random.randint(1_000_000, 9_999_999),
                    rtmp_stream=True,
                )
            )
            full = await _app.invoke(GetFullChannel(channel=channel_peer))
            active_call = getattr(full.full_chat, 'call', None)
        except Exception as ce:
            raise RuntimeError(f"فشل إنشاء بث مباشر RTMP: {ce}") from ce

    rtmp_info = await _app.invoke(
        GetGroupCallStreamRtmpUrl(peer=channel_peer, revoke=False)
    )
    rtmp_url  = rtmp_info.url
    rtmp_key  = rtmp_info.key
    rtmp_target = f"{rtmp_url}/{rtmp_key}"
    log.info(f"[YT→TG] RTMP target: {rtmp_url}/<key>")

    cmd = _build_rtmp_ffmpeg_yt(stream_url, rtmp_target, _BG_IMAGE)
    log.info(f"[YT→TG] Launching FFmpeg: {' '.join(cmd[:6])} …")
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    log.info(f"[YT→TG] FFmpeg PID {proc.pid} started for chat {chat_id}")

    if chat_id in _active_streams:
        _active_streams[chat_id]["proc"] = proc
        _active_streams[chat_id]["type"] = "rtmp"
        _active_streams[chat_id]["yt_url"] = yt_url
    else:
        _active_streams[chat_id] = {
            "proc": proc, "type": "rtmp",
            "url": yt_url or stream_url, "title": "بث مباشر",
            "started_at": time.time(),
            "yt_url": yt_url,
        }

    await asyncio.sleep(5)
    ret = proc.poll()
    if ret is not None:
        stderr = proc.stderr.read().decode(errors="replace")
        raise RuntimeError(f"FFmpeg exited immediately (code {ret}): {stderr[-500:]}")
    log.info(f"[YT→TG] RTMP stream running (PID {proc.pid})")

    if yt_url:
        asyncio.ensure_future(_yt_watchdog(chat_id, yt_url, rtmp_target))
        log.info(f"[YT→TG] Watchdog scheduled for chat {chat_id}")


def start_stream_yt(chat_id: int, stream_url: str, title: str = "بث مباشر", yt_url: str = ""):
    _init_client()

    if chat_id in _active_streams:
        stop_stream(chat_id)

    _active_streams[chat_id] = {
        "url": yt_url or stream_url,
        "title": title,
        "started_at": time.time(),
        "is_yt": True,
        "yt_url": yt_url,
    }

    try:
        _run_async(_async_play_yt(chat_id, stream_url, yt_url=yt_url))
        log.info(f"[YT→TG] Live stream started in chat {chat_id}")
        save_streams_state()
    except Exception:
        _active_streams.pop(chat_id, None)
        log.error(f"[YT→TG] start_stream_yt failed for chat {chat_id}", exc_info=True)
        raise


async def _async_play(chat_id: int, stream_url: str):
    import subprocess
    import random
    from pyrogram.raw.functions.channels import GetFullChannel
    from pyrogram.raw.functions.phone import CreateGroupCall, GetGroupCallStreamRtmpUrl

    await _resolve_peer(chat_id)
    channel_peer = await _app.resolve_peer(chat_id)

    log.info(f"Checking RTMP group-call state for {chat_id}…")
    full = await _app.invoke(GetFullChannel(channel=channel_peer))
    active_call = getattr(full.full_chat, 'call', None)
    log.info(f"full_chat.call = {active_call}")

    if active_call is None:
        log.info("No active live stream found — creating RTMP group call…")
        try:
            await _app.invoke(
                CreateGroupCall(
                    peer=channel_peer,
                    random_id=random.randint(1_000_000, 9_999_999),
                    rtmp_stream=True,
                )
            )
            full = await _app.invoke(GetFullChannel(channel=channel_peer))
            active_call = getattr(full.full_chat, 'call', None)
            log.info(f"CreateGroupCall OK — full_chat.call = {active_call}")
        except Exception as ce:
            log.error(f"CreateGroupCall failed: {ce}")
            raise RuntimeError(
                f"فشل إنشاء بث مباشر RTMP على القناة: {ce}"
            ) from ce

    rtmp_info = await _app.invoke(
        GetGroupCallStreamRtmpUrl(peer=channel_peer, revoke=False)
    )
    rtmp_url  = rtmp_info.url
    rtmp_key  = rtmp_info.key
    rtmp_target = f"{rtmp_url}/{rtmp_key}"
    log.info(f"RTMP target: {rtmp_url}/<key>")

    cmd = _build_rtmp_ffmpeg(stream_url, rtmp_target, _BG_IMAGE)
    log.info(f"Launching RTMP FFmpeg: {' '.join(cmd[:6])} … flv {rtmp_url}/<key>")
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    log.info(f"FFmpeg PID {proc.pid} started for chat {chat_id}")

    if chat_id in _active_streams:
        _active_streams[chat_id]["proc"] = proc
        _active_streams[chat_id]["type"] = "rtmp"
    else:
        _active_streams[chat_id] = {
            "proc": proc,
            "type": "rtmp",
            "url": stream_url,
            "title": "بث مباشر",
            "started_at": time.time(),
        }

    await asyncio.sleep(5)
    ret = proc.poll()
    if ret is not None:
        stderr = proc.stderr.read().decode(errors="replace")
        raise RuntimeError(
            f"FFmpeg exited immediately (code {ret}): {stderr[-500:]}"
        )
    log.info(f"RTMP stream running (PID {proc.pid}) → {rtmp_url}/<key>")


def _kill_ffmpeg(chat_id: int):
    info = _active_streams.get(chat_id, {})
    proc = info.get("proc")
    if proc and proc.poll() is None:
        try:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
            log.info(f"FFmpeg PID {proc.pid} stopped for chat {chat_id}")
        except Exception as e:
            log.warning(f"Error stopping FFmpeg for {chat_id}: {e}")


def stop_stream(chat_id: int) -> bool:
    if chat_id not in _active_streams:
        return False

    _kill_ffmpeg(chat_id)
    _active_streams.pop(chat_id, None)
    log.info(f"Live stream stopped in chat {chat_id}")
    save_streams_state()
    return True


def pause_stream(chat_id: int) -> bool:
    if chat_id not in _active_streams:
        return False
    info = _active_streams.get(chat_id, {})
    _kill_ffmpeg(chat_id)
    info["paused"] = True
    log.info(f"RTMP stream paused (FFmpeg killed) for {chat_id}")
    return True


def resume_stream(chat_id: int) -> bool:
    if chat_id not in _active_streams:
        return False
    info = _active_streams.get(chat_id, {})
    if info.get("paused"):
        url = info.get("url", "")
        title = info.get("title", "بث مباشر")
        if url:
            _active_streams.pop(chat_id, None)
            start_stream(chat_id, url, title)
            return True
    return False


def change_stream(chat_id: int, new_url: str, title: str = "بث مباشر"):
    if chat_id not in _active_streams:
        start_stream(chat_id, new_url, title)
        return

    stop_stream(chat_id)
    start_stream(chat_id, new_url, title)
    log.info(f"Stream changed in chat {chat_id}: {new_url}")


def extract_youtube_url(yt_url: str) -> dict:
    import subprocess, json
    title_cmd = [
        "yt-dlp", "--no-download", "--print", "%(title)s",
        "--print", "%(is_live)s",
        "-f", "best[height<=720][ext=mp4]/best[height<=720]/best",
        "--no-playlist", yt_url,
    ]
    url_cmd = [
        "yt-dlp", "-g",
        "-f", "best[height<=720][ext=mp4]/best[height<=720]/best",
        "--no-playlist", yt_url,
    ]
    try:
        t_result = subprocess.run(title_cmd, capture_output=True, text=True, timeout=30)
        u_result = subprocess.run(url_cmd, capture_output=True, text=True, timeout=30)

        if u_result.returncode != 0:
            raise RuntimeError(u_result.stderr.strip()[-300:])

        lines = t_result.stdout.strip().split("\n")
        title = lines[0] if lines else "بث مباشر"
        is_live = lines[1].lower() == "true" if len(lines) > 1 else False
        direct_url = u_result.stdout.strip().split("\n")[0]

        log.info(f"[yt-dlp] title={title}, is_live={is_live}, url_len={len(direct_url)}")
        return {
            "url": direct_url,
            "title": title,
            "is_live": is_live,
        }
    except subprocess.TimeoutExpired:
        raise RuntimeError("انتهت مهلة استخراج الرابط من يوتيوب")


def _build_yt_source_ffmpeg(source_url: str, rtmp_target: str, image_path: str | None) -> list[str]:
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "warning"]

    if image_path and os.path.isfile(image_path):
        cmd += ["-re", "-stream_loop", "-1", "-loop", "1", "-i", image_path]
        cmd += [
            "-reconnect", "1", "-reconnect_streamed", "1",
            "-reconnect_delay_max", "5",
            "-i", source_url,
        ]
        cmd += [
            "-map", "0:v", "-map", "1:a",
            "-c:v", "libx264", "-preset", "ultrafast",
            "-tune", "stillimage",
            "-pix_fmt", "yuv420p", "-s", "640x360", "-r", "25",
            "-b:v", "500k", "-maxrate", "500k", "-bufsize", "1000k",
            "-c:a", "aac", "-ar", "48000", "-b:a", "128k", "-ac", "2",
        ]
    else:
        cmd += [
            "-reconnect", "1", "-reconnect_streamed", "1",
            "-reconnect_delay_max", "5",
            "-i", source_url,
        ]
        cmd += [
            "-c:v", "libx264", "-preset", "ultrafast",
            "-pix_fmt", "yuv420p", "-s", "640x360", "-r", "25",
            "-b:v", "500k", "-maxrate", "500k", "-bufsize", "1000k",
            "-c:a", "aac", "-ar", "48000", "-b:a", "128k", "-ac", "2",
        ]

    cmd += ["-f", "flv", rtmp_target]
    return cmd


def start_youtube_stream(stream_url: str, yt_rtmp_url: str, yt_stream_key: str,
                         title: str = "بث مباشر", tag: str = "default"):
    import subprocess

    if tag in _yt_streams:
        stop_youtube_stream(tag)

    rtmp_target = f"{yt_rtmp_url}/{yt_stream_key}"
    image_path = _BG_IMAGE if os.path.isfile(_BG_IMAGE) else None
    cmd = _build_rtmp_ffmpeg(stream_url, rtmp_target, image_path)
    log.info(f"Launching YouTube RTMP FFmpeg [{tag}]: {' '.join(cmd[:6])} …")

    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    log.info(f"YouTube FFmpeg PID {proc.pid} started [{tag}]")

    save_streams_state()
    _yt_streams[tag] = {
        "proc": proc,
        "url": stream_url,
        "title": title,
        "rtmp_url": yt_rtmp_url,
        "started_at": time.time(),
    }

    time.sleep(5)
    ret = proc.poll()
    if ret is not None:
        stderr = proc.stderr.read().decode(errors="replace")
        _yt_streams.pop(tag, None)
        raise RuntimeError(f"FFmpeg exited immediately (code {ret}): {stderr[-500:]}")

    log.info(f"YouTube RTMP stream running [{tag}] (PID {proc.pid})")


def stop_youtube_stream(tag: str = "default") -> bool:
    info = _yt_streams.pop(tag, None)
    if not info:
        return False
    proc = info.get("proc")
    if proc and proc.poll() is None:
        try:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
            log.info(f"YouTube FFmpeg PID {proc.pid} stopped [{tag}]")
        except Exception as e:
            log.warning(f"Error stopping YouTube FFmpeg [{tag}]: {e}")
    save_streams_state()
    return True


def is_youtube_streaming(tag: str = "default") -> bool:
    info = _yt_streams.get(tag)
    if not info:
        return False
    proc = info.get("proc")
    if proc and proc.poll() is not None:
        _yt_streams.pop(tag, None)
        return False
    return True


def get_youtube_stream_info(tag: str = "default") -> dict | None:
    return _yt_streams.get(tag)
