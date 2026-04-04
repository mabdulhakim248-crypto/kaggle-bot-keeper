BOT_TOKEN      = "8717639109:AAHtPfAodWgXHpkPtC4zFujiosDm49AdxWs"
API_ID         = "38635106"
API_HASH       = "e159cf0eb690131778801b19bfa8fb08"
SESSION_STRING = "BAJNhmIAgknADgaoH1cYU7iA6P7EWRBcjmELcjIdJ6YbJN3OsSCmIiwGCU-4Qpb_hOhV30NsvlQaPnR3Ibf30SDgdMRM-VbDxSquXOWIoplHD5Fxb4BM-4DxSxMULXb1wwHlFpTC47pPTmKFpzSiQH2TYgpXXQ_CJ7oHlSHPSretD9G9P1hifmpwCfkUGVnk_fcPJ8-22AGrmP71dWEOQ8JmJt365qDzvl6HSZZfQWpcg_hDHkBVP77jzhtYVJEEC9W9z-PdIUK6AYYBC2K0pa6Fv1zwv2vRRulaL26vK2Op3PEZxGiAVYccG9WK-LgE1ww5BiYVvfWV1fdW_-9qmJUnRS5iqQAAAAGCJ_DhAA"

import os

os.environ.setdefault("API_ID",         API_ID)
os.environ.setdefault("API_HASH",       API_HASH)
os.environ.setdefault("SESSION_STRING", SESSION_STRING)
import logging
import threading
import time
import asyncio
from pathlib import Path

asyncio.set_event_loop(asyncio.new_event_loop())
from pyrogram import Client as PyroClient

def _load_dotenv():
    env_file = Path(__file__).parent / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and value:
            os.environ.setdefault(key, value)
        if key == "BOT_TOKEN" and value:
            global BOT_TOKEN
            BOT_TOKEN = value

_load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN") or BOT_TOKEN

import telebot
from telebot.types import (
    Message,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)

import radio

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)
logging.getLogger("TeleBot").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN فارغ — ضعه في السطر 1 من هذا الملف")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None, num_threads=8)

MAX_TG_MSG = 4000

_waiting_bg: set[str] = set()
_waiting_yt_title: set[str] = set()
_waiting_yt_desc: set[str] = set()


def _send_long_message(chat_id: int, text: str):
    if len(text) <= MAX_TG_MSG:
        bot.send_message(chat_id, text)
        return
    for i in range(0, len(text), MAX_TG_MSG):
        bot.send_message(chat_id, text[i: i + MAX_TG_MSG])


@bot.message_handler(commands=["start"])
def cmd_start(msg: Message):
    bot.reply_to(msg,
        "مرحباً! أنا بوت البث المباشر\n\n"
        "📻 البث على تليجرام:\n"
        "• /radio <رابط بث> – بدء بث مباشر\n"
        "• /quran – إذاعة القرآن الكريم\n"
        "• /live <رابط يوتيوب> – بث فيديو/لايف يوتيوب على تليجرام\n"
        "• /stop – إيقاف البث\n\n"
        "📺 البث على يوتيوب:\n"
        "• /yt – بث القرآن على يوتيوب\n"
        "• /yt <رابط يوتيوب> – بث فيديو/لايف على قناتك\n"
        "• /yt stop – إيقاف بث يوتيوب\n\n"
        "⚙️ /settings – الإعدادات\n"
        "/help – المساعدة"
    )


@bot.message_handler(commands=["help"])
def cmd_help(msg: Message):
    bot.reply_to(msg,
        "كيفية الاستخدام:\n\n"
        "📻 البث المباشر على تليجرام:\n"
        "• /radio <رابط البث> – بدء بث مباشر في المجموعة\n"
        "• /radio <رابط> <اسم> – بث مع اسم مخصص\n"
        "• /quran – إذاعة القرآن الكريم من القاهرة\n"
        "• /quran quran2 – إذاعة تراتيل\n"
        "• /quran quran3 – إذاعة مشاري العفاسي\n"
        "• /stop – إيقاف البث\n"
        "• /pause – إيقاف مؤقت\n"
        "• /resume – استئناف البث\n\n"
        "📺 بث من يوتيوب:\n"
        "• /live <رابط يوتيوب> – بث فيديو/لايف يوتيوب على تليجرام\n\n"
        "📺 البث على يوتيوب:\n"
        "• /yt – بث إذاعة القرآن على يوتيوب\n"
        "• /yt <رابط يوتيوب> – بث فيديو/لايف على قناتك\n"
        "• /yt stop – إيقاف بث يوتيوب\n"
        "• /yt status – حالة بث يوتيوب\n\n"
        "⚙️ الإعدادات:\n"
        "• /settings – تغيير الخلفية + التحكم في بث يوتيوب\n\n"
        "ملاحظة: بث تليجرام يعمل في المجموعات والقنوات فقط.\n"
        "بث يوتيوب يعمل من أي مكان."
    )


_session_pending: dict[str, dict] = {}


@bot.message_handler(commands=["session"])
def cmd_session(msg: Message):
    uid = str(msg.from_user.id)

    if uid in _session_pending and _session_pending[uid].get("waiting_code"):
        bot.reply_to(msg, "أنت بالفعل في عملية إنشاء Session. أرسل الكود أو /cancel للإلغاء.")
        return

    bot.reply_to(msg,
        "جاري إرسال كود التحقق إلى رقمك...\n"
        "انتظر لحظة..."
    )

    api_id = os.environ.get("API_ID", "")
    api_hash = os.environ.get("API_HASH", "")

    if not api_id or not api_hash:
        bot.send_message(msg.chat.id, "خطأ: API_ID و API_HASH غير مضبوطين.")
        return

    def _do_send():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def _run():
            app = PyroClient(
                f"sess_{uid}",
                api_id=int(api_id),
                api_hash=api_hash,
                in_memory=True,
            )
            await app.connect()
            sent = await app.send_code('+201558636638')
            return app, sent

        try:
            app, sent = loop.run_until_complete(_run())
            _session_pending[uid] = {
                "app": app,
                "loop": loop,
                "hash": sent.phone_code_hash,
                "waiting_code": True,
                "chat_id": msg.chat.id,
            }
            bot.send_message(msg.chat.id,
                "تم إرسال كود التحقق إلى رقمك على Telegram!\n\n"
                "أرسل الكود هنا الآن (5 أرقام):\n\n"
                "للإلغاء: /cancel"
            )
        except Exception as e:
            bot.send_message(msg.chat.id, f"خطأ في إرسال الكود: {e}")

    threading.Thread(target=_do_send, daemon=True).start()


@bot.message_handler(commands=["cancel"])
def cmd_cancel(msg: Message):
    uid = str(msg.from_user.id)
    info = _session_pending.pop(uid, None)
    if info:
        try:
            info["loop"].run_until_complete(info["app"].disconnect())
        except Exception:
            pass
        bot.reply_to(msg, "تم إلغاء عملية إنشاء Session.")
    else:
        bot.reply_to(msg, "لا توجد عملية جارية.")


# ─────────────────────────────────────────────────────────────────────────────
# Radio presets — verified working stream URLs (April 2026)
#
# Cairo Quran Radio (إذاعة القرآن الكريم من القاهرة):
#   Primary : stream.radiojar.com/8s5u5tpdtwzuv
#             → Radiojar CDN relay of Egypt's official Quran FM 98.2
#             → FFmpeg uses browser User-Agent + Referer (handled in radio.py)
#   Backup  : qurango.net/radio/tarateel
#             → Icecast stream, works without auth headers
# ─────────────────────────────────────────────────────────────────────────────
YT_RTMP_URL = "rtmp://a.rtmp.youtube.com/live2"
YT_STREAM_KEY = "3mzj-8v09-tuce-7xsd-4crt"

RADIO_PRESETS = {
    # ── إذاعة القرآن الكريم من القاهرة الرسمية — Radiojar CDN ──
    "quran": {
        "url": "https://stream.radiojar.com/8s5u5tpdtwzuv",
        "title": "📡 إذاعة القرآن الكريم من القاهرة",
        "backup_url": "https://qurango.net/radio/tarateel",
    },
    # ── نسخة احتياطية — Icecast مباشر (لا تحتاج headers) ──
    "quran2": {
        "url": "https://qurango.net/radio/tarateel",
        "title": "🕌 إذاعة القرآن الكريم – تراتيل",
    },
    # ── إذاعة القرآن الكريم MBC — HLS عالي الجودة ──
    "quran3": {
        "url": "https://quraanfm-radio.mbc.net/quraanfm-radio.m3u8",
        "title": "🌙 إذاعة القرآن الكريم – MBC",
    },
}


@bot.message_handler(commands=["quran"])
def cmd_quran(msg: Message):
    if msg.chat.type == "private":
        bot.reply_to(msg,
            "البث المباشر يعمل في المجموعات والقنوات فقط.\n"
            "أضف البوت لمجموعة أو قناة واستخدم الأمر هناك."
        )
        return

    parts = msg.text.strip().split(maxsplit=1)
    preset_key = parts[1].strip().lower() if len(parts) > 1 else "quran"

    if preset_key not in RADIO_PRESETS:
        bot.reply_to(msg,
            "الإذاعات المتاحة:\n\n"
            "• /quran – 📡 إذاعة القرآن الكريم من القاهرة (الرسمية)\n"
            "• /quran quran2 – 🕌 إذاعة تراتيل (Icecast مباشر)\n"
            "• /quran quran3 – 🌙 إذاعة القرآن MBC\n\n"
            "أو شغّل رابط مخصص:\n"
            "• /radio https://رابط-البث"
        )
        return

    preset = RADIO_PRESETS[preset_key]
    status_msg = bot.reply_to(msg, f"جاري بدء {preset['title']}... 📻")

    def _do_quran():
        try:
            radio.start_stream(msg.chat.id, preset["url"], preset["title"])
            bot.edit_message_text(
                f"🔴 البث المباشر شغال الآن!\n\n"
                f"📻 {preset['title']}\n\n"
                f"للإيقاف: /stop\n"
                f"إيقاف مؤقت: /pause\n"
                f"استئناف: /resume",
                chat_id=msg.chat.id,
                message_id=status_msg.message_id,
            )
        except ValueError as e:
            bot.edit_message_text(str(e), chat_id=msg.chat.id, message_id=status_msg.message_id)
        except Exception as e:
            log.error(f"Quran radio error: {e}", exc_info=True)
            backup = preset.get("backup_url")
            if backup:
                try:
                    radio.start_stream(msg.chat.id, backup, preset["title"] + " (احتياطي)")
                    bot.edit_message_text(
                        f"🔴 البث شغال (رابط احتياطي)!\n\n"
                        f"📻 {preset['title']}\n\n"
                        f"للإيقاف: /stop",
                        chat_id=msg.chat.id,
                        message_id=status_msg.message_id,
                    )
                    return
                except Exception as e2:
                    log.error(f"Backup stream also failed: {e2}", exc_info=True)
            bot.edit_message_text(
                f"❌ خطأ في بدء البث.\n\n"
                "تأكد أن:\n"
                "1. البوت أدمن في المجموعة/القناة\n"
                "2. عنده صلاحية إدارة المحادثات الصوتية\n"
                "3. المحادثة الصوتية (Voice Chat) مفتوحة\n\n"
                "جرب: /quran quran2",
                chat_id=msg.chat.id,
                message_id=status_msg.message_id,
            )

    threading.Thread(target=_do_quran, daemon=True).start()


@bot.message_handler(commands=["radio"])
def cmd_radio(msg: Message):
    if msg.chat.type == "private":
        bot.reply_to(msg,
            "البث المباشر يعمل في المجموعات والقنوات فقط.\n"
            "أضف البوت لمجموعة أو قناة واستخدم الأمر هناك."
        )
        return

    parts = msg.text.strip().split(maxsplit=2)
    if len(parts) < 2 or not parts[1].startswith("http"):
        bot.reply_to(msg,
            "أرسل رابط البث المباشر بعد الأمر:\n"
            "/radio https://stream.example.com/live\n\n"
            "يمكنك إضافة اسم للبث:\n"
            "/radio https://stream.example.com/live إذاعة القرآن\n\n"
            "أو استخدم الإذاعات الجاهزة:\n"
            "/quran – إذاعة القرآن الكريم من القاهرة"
        )
        return

    stream_url = parts[1]
    title = parts[2] if len(parts) > 2 else "بث مباشر"

    if _is_youtube_url(stream_url):
        status_msg = bot.reply_to(msg, "⏳ جاري استخراج الرابط من يوتيوب...")

        def _do_yt():
            try:
                yt_info = radio.extract_youtube_url(stream_url)
                source_url = yt_info["url"]
                yt_title = yt_info["title"] if title == "بث مباشر" else title
                if not source_url:
                    bot.edit_message_text("❌ تعذّر استخراج رابط البث.",
                        chat_id=msg.chat.id, message_id=status_msg.message_id)
                    return
                bot.edit_message_text(f"جاري بدء البث: {yt_title}... 📻",
                    chat_id=msg.chat.id, message_id=status_msg.message_id)
                if radio.is_streaming(msg.chat.id):
                    radio.stop_stream(msg.chat.id)
                radio.start_stream_yt(msg.chat.id, source_url, yt_title, yt_url=stream_url)
                bot.edit_message_text(
                    f"🔴 البث المباشر شغال!\n\n📺 {yt_title}\n\nللإيقاف: /stop",
                    chat_id=msg.chat.id, message_id=status_msg.message_id)
            except Exception as e:
                log.error(f"Radio YT error: {e}", exc_info=True)
                bot.edit_message_text(
                    f"خطأ في بدء البث: {e}\n\n"
                    "تأكد أن:\n1. البوت أدمن\n"
                    "2. عنده صلاحية إدارة المحادثات الصوتية\n"
                    "3. المحادثة الصوتية مفتوحة",
                    chat_id=msg.chat.id, message_id=status_msg.message_id)

        threading.Thread(target=_do_yt, daemon=True).start()
        return

    if radio.is_streaming(msg.chat.id):
        change_msg = bot.reply_to(msg, "جاري تغيير البث...")
        def _do_change():
            try:
                radio.change_stream(msg.chat.id, stream_url, title)
                bot.edit_message_text(f"🔴 تم تغيير البث إلى: {title}",
                    chat_id=msg.chat.id, message_id=change_msg.message_id)
            except Exception as e:
                bot.edit_message_text(f"خطأ في تغيير البث: {e}",
                    chat_id=msg.chat.id, message_id=change_msg.message_id)
        threading.Thread(target=_do_change, daemon=True).start()
        return

    status_msg = bot.reply_to(msg, "جاري بدء البث المباشر... 📻")

    def _do_radio():
        try:
            radio.start_stream(msg.chat.id, stream_url, title)
            bot.edit_message_text(
                f"🔴 البث المباشر شغال الآن!\n\n"
                f"📻 {title}\n"
                f"🔗 {stream_url}\n\n"
                f"للإيقاف: /stop\n"
                f"إيقاف مؤقت: /pause\n"
                f"استئناف: /resume",
                chat_id=msg.chat.id,
                message_id=status_msg.message_id,
            )
        except ValueError as e:
            bot.edit_message_text(str(e), chat_id=msg.chat.id, message_id=status_msg.message_id)
        except Exception as e:
            log.error(f"Radio start error: {e}", exc_info=True)
            bot.edit_message_text(
                f"خطأ في بدء البث: {e}\n\n"
                "تأكد أن:\n"
                "1. البوت أدمن في المجموعة/القناة\n"
                "2. عنده صلاحية إدارة المحادثات الصوتية\n"
                "3. المحادثة الصوتية (Voice Chat) مفتوحة",
                chat_id=msg.chat.id,
                message_id=status_msg.message_id,
            )

    threading.Thread(target=_do_radio, daemon=True).start()


@bot.message_handler(commands=["stop"])
def cmd_stop(msg: Message):
    if not radio.is_streaming(msg.chat.id):
        bot.reply_to(msg, "لا يوجد بث مباشر حالياً.")
        return

    try:
        radio.stop_stream(msg.chat.id)
        bot.reply_to(msg, "⏹ تم إيقاف البث المباشر.")
    except Exception as e:
        bot.reply_to(msg, f"خطأ في إيقاف البث: {e}")


@bot.message_handler(commands=["pause"])
def cmd_pause(msg: Message):
    if not radio.is_streaming(msg.chat.id):
        bot.reply_to(msg, "لا يوجد بث مباشر حالياً.")
        return

    if radio.pause_stream(msg.chat.id):
        bot.reply_to(msg, "⏸ تم الإيقاف المؤقت.\nللاستئناف: /resume")
    else:
        bot.reply_to(msg, "خطأ في الإيقاف المؤقت.")


@bot.message_handler(commands=["resume"])
def cmd_resume(msg: Message):
    if not radio.is_streaming(msg.chat.id):
        bot.reply_to(msg, "لا يوجد بث مباشر حالياً.")
        return

    if radio.resume_stream(msg.chat.id):
        bot.reply_to(msg, "▶️ تم استئناف البث!")
    else:
        bot.reply_to(msg, "خطأ في استئناف البث.")


@bot.message_handler(commands=["settings"])
def cmd_settings(msg: Message):
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🖼 تغيير خلفية البث", callback_data="set_bg"))
    if radio.is_youtube_streaming():
        markup.add(InlineKeyboardButton("✏️ تغيير اسم البث", callback_data="yt_title"))
        markup.add(InlineKeyboardButton("📝 تغيير وصف البث", callback_data="yt_desc"))
        markup.add(InlineKeyboardButton("⏹ إيقاف بث يوتيوب", callback_data="yt_stop"))
    bot.reply_to(msg, "⚙️ الإعدادات:", reply_markup=markup)


@bot.message_handler(content_types=["photo"])
def handle_photo(msg: Message):
    uid = str(msg.from_user.id)
    if uid not in _waiting_bg:
        bot.reply_to(msg, "استخدم /settings لتغيير خلفية البث.")
        return

    _waiting_bg.discard(uid)
    try:
        photo = msg.photo[-1]
        file_info = bot.get_file(photo.file_id)
        file_bytes = bot.download_file(file_info.file_path)

        import os as _os
        bg_path = _os.path.join(_os.path.dirname(__file__), "radio_bg.jpg")
        with open(bg_path, "wb") as f:
            f.write(file_bytes)

        bot.reply_to(msg, "✅ تم تغيير خلفية البث بنجاح!\nالخلفية الجديدة هتظهر في البث الجاي أو لما تعيد تشغيل البث الحالي.")
    except Exception as e:
        log.error(f"Background change error: {e}", exc_info=True)
        bot.reply_to(msg, f"❌ خطأ في تغيير الخلفية: {e}")


@bot.callback_query_handler(func=lambda call: call.data in ("set_bg", "yt_title", "yt_desc", "yt_stop"))
def handle_settings_callback(call: CallbackQuery):
    uid = str(call.from_user.id)
    chat_id = call.message.chat.id
    bot.answer_callback_query(call.id)

    if call.data == "set_bg":
        _waiting_bg.add(uid)
        bot.send_message(chat_id, "🖼 أرسل الصورة الجديدة للخلفية الآن:")

    elif call.data == "yt_title":
        _waiting_yt_title.add(uid)
        _waiting_yt_desc.discard(uid)
        bot.send_message(chat_id, "✏️ اكتب اسم البث الجديد:")

    elif call.data == "yt_desc":
        _waiting_yt_desc.add(uid)
        _waiting_yt_title.discard(uid)
        bot.send_message(chat_id, "📝 اكتب وصف البث الجديد:")

    elif call.data == "yt_stop":
        if radio.is_youtube_streaming():
            radio.stop_youtube_stream()
            bot.send_message(chat_id, "⏹ تم إيقاف بث يوتيوب.")
        else:
            bot.send_message(chat_id, "لا يوجد بث يوتيوب حالياً.")


@bot.message_handler(commands=["live"])
def cmd_live(msg: Message):
    if msg.chat.type == "private":
        bot.reply_to(msg,
            "البث المباشر يعمل في المجموعات والقنوات فقط.\n"
            "أضف البوت لمجموعة أو قناة واستخدم الأمر هناك."
        )
        return

    parts = msg.text.strip().split(maxsplit=1) if msg.text else []
    if len(parts) < 2 or not parts[1].startswith("http"):
        bot.reply_to(msg,
            "أرسل رابط يوتيوب بعد الأمر:\n\n"
            "/live https://www.youtube.com/watch?v=xxx\n"
            "/live https://www.youtube.com/live/xxx\n\n"
            "يدعم فيديوهات يوتيوب والبث المباشر."
        )
        return

    yt_url = parts[1].strip()
    status_msg = bot.reply_to(msg, "⏳ جاري استخراج الرابط من يوتيوب...")

    def _do_live():
        try:
            yt_info = radio.extract_youtube_url(yt_url)
            source_url = yt_info["url"]
            title = yt_info["title"]

            if not source_url:
                bot.edit_message_text(
                    "❌ تعذّر استخراج رابط البث من يوتيوب.",
                    chat_id=msg.chat.id, message_id=status_msg.message_id,
                )
                return

            bot.edit_message_text(
                f"جاري بدء البث: {title}... 📻",
                chat_id=msg.chat.id, message_id=status_msg.message_id,
            )

            if radio.is_streaming(msg.chat.id):
                radio.stop_stream(msg.chat.id)

            radio.start_stream_yt(msg.chat.id, source_url, title, yt_url=yt_url)
            bot.edit_message_text(
                f"🔴 البث المباشر شغال!\n\n"
                f"📺 {title}\n\n"
                f"للإيقاف: /stop\n"
                f"إيقاف مؤقت: /pause\n"
                f"استئناف: /resume",
                chat_id=msg.chat.id, message_id=status_msg.message_id,
            )
        except Exception as e:
            log.error(f"Live YT error: {e}", exc_info=True)
            bot.edit_message_text(
                f"❌ خطأ: {e}",
                chat_id=msg.chat.id, message_id=status_msg.message_id,
            )

    threading.Thread(target=_do_live, daemon=True).start()


@bot.message_handler(commands=["yt", "youtube"])
def cmd_youtube(msg: Message):
    parts = msg.text.strip().split(maxsplit=1) if msg.text else []
    sub = parts[1].strip() if len(parts) > 1 else ""
    sub_lower = sub.lower()

    if sub_lower == "stop":
        if not radio.is_youtube_streaming():
            bot.reply_to(msg, "لا يوجد بث يوتيوب حالياً.")
            return
        radio.stop_youtube_stream()
        bot.reply_to(msg, "⏹ تم إيقاف بث يوتيوب.")
        return

    if sub_lower == "status":
        if radio.is_youtube_streaming():
            info = radio.get_youtube_stream_info()
            elapsed = int(time.time() - info["started_at"]) if info else 0
            hours, remainder = divmod(elapsed, 3600)
            minutes, secs = divmod(remainder, 60)
            bot.reply_to(msg,
                f"🔴 بث يوتيوب شغال\n\n"
                f"📻 {info.get('title', 'بث مباشر')}\n"
                f"⏱ المدة: {hours}:{minutes:02d}:{secs:02d}"
            )
        else:
            bot.reply_to(msg, "لا يوجد بث يوتيوب حالياً.")
        return

    if radio.is_youtube_streaming():
        bot.reply_to(msg, "بث يوتيوب شغال بالفعل.\nللإيقاف: /yt stop\nللحالة: /yt status")
        return

    is_yt_link = sub.startswith("http") and ("youtube.com" in sub or "youtu.be" in sub)

    if is_yt_link:
        status_msg = bot.reply_to(msg, "⏳ جاري استخراج الرابط من يوتيوب...")

        def _do_yt_restream():
            try:
                yt_info = radio.extract_youtube_url(sub)
                source_url = yt_info["url"]
                title = yt_info["title"]

                if not source_url:
                    bot.edit_message_text(
                        "❌ تعذّر استخراج رابط البث من يوتيوب.",
                        chat_id=msg.chat.id, message_id=status_msg.message_id,
                    )
                    return

                bot.edit_message_text(
                    f"جاري بدء البث: {title}... 📺",
                    chat_id=msg.chat.id, message_id=status_msg.message_id,
                )

                radio.start_youtube_stream(source_url, YT_RTMP_URL, YT_STREAM_KEY, title)
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("✏️ تغيير اسم البث", callback_data="yt_title"))
                markup.add(InlineKeyboardButton("📝 تغيير وصف البث", callback_data="yt_desc"))
                markup.add(InlineKeyboardButton("⏹ إيقاف البث", callback_data="yt_stop"))
                bot.edit_message_text(
                    f"🔴 البث شغال على يوتيوب!\n\n"
                    f"📺 {title}\n\n"
                    f"⚙️ اضغط على الأزرار للتحكم:",
                    chat_id=msg.chat.id, message_id=status_msg.message_id,
                    reply_markup=markup,
                )
            except Exception as e:
                log.error(f"YT restream error: {e}", exc_info=True)
                bot.edit_message_text(
                    f"❌ خطأ: {e}",
                    chat_id=msg.chat.id, message_id=status_msg.message_id,
                )

        threading.Thread(target=_do_yt_restream, daemon=True).start()
        return

    preset = RADIO_PRESETS.get("quran", {})
    stream_url = preset.get("url", "https://stream.radiojar.com/8s5u5tpdtwzuv")
    title = "📡 إذاعة القرآن الكريم من القاهرة"

    status_msg = bot.reply_to(msg, "جاري بدء البث على يوتيوب... 📺")
    try:
        radio.start_youtube_stream(stream_url, YT_RTMP_URL, YT_STREAM_KEY, title)
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("✏️ تغيير اسم البث", callback_data="yt_title"))
        markup.add(InlineKeyboardButton("📝 تغيير وصف البث", callback_data="yt_desc"))
        markup.add(InlineKeyboardButton("🖼 تغيير الخلفية", callback_data="set_bg"))
        markup.add(InlineKeyboardButton("⏹ إيقاف البث", callback_data="yt_stop"))
        bot.edit_message_text(
            f"🔴 البث المباشر شغال على يوتيوب!\n\n"
            f"📻 {title}\n\n"
            f"⚙️ اضغط على الأزرار للتحكم:",
            chat_id=msg.chat.id,
            message_id=status_msg.message_id,
            reply_markup=markup,
        )
    except Exception as e:
        log.error(f"YouTube stream error: {e}", exc_info=True)
        backup_url = preset.get("backup_url")
        if backup_url:
            try:
                radio.start_youtube_stream(backup_url, YT_RTMP_URL, YT_STREAM_KEY, title + " (احتياطي)")
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("✏️ تغيير اسم البث", callback_data="yt_title"))
                markup.add(InlineKeyboardButton("📝 تغيير وصف البث", callback_data="yt_desc"))
                markup.add(InlineKeyboardButton("⏹ إيقاف البث", callback_data="yt_stop"))
                bot.edit_message_text(
                    f"🔴 البث شغال على يوتيوب (رابط احتياطي)!\n\n"
                    f"📻 {title}\n\n"
                    f"⚙️ اضغط على الأزرار للتحكم:",
                    chat_id=msg.chat.id,
                    message_id=status_msg.message_id,
                    reply_markup=markup,
                )
                return
            except Exception as e2:
                log.error(f"YouTube backup also failed: {e2}", exc_info=True)
        bot.edit_message_text(
            f"❌ خطأ في بدء البث على يوتيوب: {e}",
            chat_id=msg.chat.id,
            message_id=status_msg.message_id,
        )


@bot.message_handler(func=lambda m: True)
def handle_message(msg: Message):
    uid = str(msg.from_user.id)
    user_text = msg.text.strip() if msg.text else ""
    if not user_text:
        return

    if uid in _waiting_yt_title:
        _waiting_yt_title.discard(uid)
        info = radio.get_youtube_stream_info()
        if info:
            info["title"] = user_text
            bot.reply_to(msg,
                f"✅ تم تغيير اسم البث إلى: {user_text}\n\n"
                "ملاحظة: لتغيير العنوان على يوتيوب نفسه، غيّره من YouTube Studio."
            )
        else:
            bot.reply_to(msg, "لا يوجد بث يوتيوب حالياً.")
        return

    if uid in _waiting_yt_desc:
        _waiting_yt_desc.discard(uid)
        info = radio.get_youtube_stream_info()
        if info:
            info["description"] = user_text
            bot.reply_to(msg,
                f"✅ تم تغيير وصف البث.\n\n"
                "ملاحظة: لتغيير الوصف على يوتيوب نفسه، غيّره من YouTube Studio."
            )
        else:
            bot.reply_to(msg, "لا يوجد بث يوتيوب حالياً.")
        return

    if uid in _session_pending and _session_pending[uid].get("waiting_code"):
        code = user_text.strip().replace(" ", "").replace(".", "")
        if code.isdigit() and len(code) == 5:
            info = _session_pending[uid]
            bot.reply_to(msg, "جاري التحقق من الكود...")

            def _do_verify():
                loop = info["loop"]
                app_c = info["app"]

                async def _verify():
                    try:
                        await app_c.sign_in('+201558636638', info["hash"], code)
                    except Exception as e:
                        if "SESSION_PASSWORD_NEEDED" in str(e):
                            return "need_2fa"
                        raise
                    ss = await app_c.export_session_string()
                    await app_c.disconnect()
                    return ss

                try:
                    result = loop.run_until_complete(_verify())
                    if result == "need_2fa":
                        _session_pending[uid]["waiting_code"] = False
                        _session_pending[uid]["waiting_2fa"] = True
                        bot.send_message(info["chat_id"],
                            "حسابك محمي بتحقق بخطوتين.\n"
                            "أرسل كلمة المرور:"
                        )
                        return

                    _session_pending.pop(uid, None)
                    bot.send_message(info["chat_id"],
                        "تم بنجاح! هذا هو الـ Session String:\n\n"
                        f"`{result}`\n\n"
                        "انسخه وأرسله لي عشان أضيفه للبوت.",
                        parse_mode="Markdown",
                    )
                except Exception as e:
                    bot.send_message(info["chat_id"], f"خطأ: {e}")

            threading.Thread(target=_do_verify, daemon=True).start()
            return

    if uid in _session_pending and _session_pending[uid].get("waiting_2fa"):
        info = _session_pending[uid]
        password = user_text.strip()
        bot.reply_to(msg, "جاري التحقق...")

        def _do_2fa():
            loop = info["loop"]
            app_c = info["app"]

            async def _check():
                await app_c.check_password(password)
                ss = await app_c.export_session_string()
                await app_c.disconnect()
                return ss

            try:
                ss = loop.run_until_complete(_check())
                _session_pending.pop(uid, None)

                bot.send_message(info["chat_id"],
                    "تم بنجاح! هذا هو الـ Session String:\n\n"
                    f"`{ss}`\n\n"
                    "انسخه وأرسله لي عشان أضيفه للبوت.",
                    parse_mode="Markdown",
                )
            except Exception as e:
                bot.send_message(info["chat_id"], f"خطأ: {e}")

        threading.Thread(target=_do_2fa, daemon=True).start()
        return

    bot.reply_to(
        msg,
        "استخدم /help للمساعدة."
    )


def _is_youtube_url(url: str) -> bool:
    return any(d in url for d in ("youtube.com", "youtu.be"))


@bot.channel_post_handler(commands=["live"])
def channel_live(msg: Message):
    parts = msg.text.strip().split(maxsplit=1) if msg.text else []
    if len(parts) < 2 or not parts[1].startswith("http"):
        bot.send_message(msg.chat.id,
            "أرسل رابط يوتيوب بعد الأمر:\n\n"
            "/live https://www.youtube.com/watch?v=xxx\n"
            "/live https://www.youtube.com/live/xxx"
        )
        return

    yt_url = parts[1].strip()
    status_msg = bot.send_message(msg.chat.id, "⏳ جاري استخراج الرابط من يوتيوب...")

    def _do():
        try:
            yt_info = radio.extract_youtube_url(yt_url)
            source_url = yt_info["url"]
            title = yt_info["title"]
            if not source_url:
                bot.edit_message_text("❌ تعذّر استخراج رابط البث.",
                    chat_id=msg.chat.id, message_id=status_msg.message_id)
                return
            bot.edit_message_text(f"جاري بدء البث: {title}... 📻",
                chat_id=msg.chat.id, message_id=status_msg.message_id)
            if radio.is_streaming(msg.chat.id):
                radio.stop_stream(msg.chat.id)
            radio.start_stream_yt(msg.chat.id, source_url, title, yt_url=yt_url)
            bot.edit_message_text(
                f"🔴 البث المباشر شغال!\n\n📺 {title}\n\nللإيقاف: /stop",
                chat_id=msg.chat.id, message_id=status_msg.message_id)
        except Exception as e:
            log.error(f"Channel live error: {e}", exc_info=True)
            bot.edit_message_text(f"❌ خطأ: {e}",
                chat_id=msg.chat.id, message_id=status_msg.message_id)

    threading.Thread(target=_do, daemon=True).start()


@bot.channel_post_handler(commands=["radio"])
def channel_radio(msg: Message):
    parts = msg.text.strip().split(maxsplit=2) if msg.text else []
    if len(parts) < 2 or not parts[1].startswith("http"):
        bot.send_message(msg.chat.id,
            "أرسل رابط البث المباشر بعد الأمر:\n"
            "/radio https://stream.example.com/live\n\n"
            "لبث يوتيوب استخدم: /live <رابط يوتيوب>\n"
            "أو استخدم /quran لإذاعة القرآن الكريم"
        )
        return

    stream_url = parts[1]
    title = parts[2] if len(parts) > 2 else "بث مباشر"

    if _is_youtube_url(stream_url):
        status_msg = bot.send_message(msg.chat.id, "⏳ جاري استخراج الرابط من يوتيوب...")

        def _do_yt():
            try:
                yt_info = radio.extract_youtube_url(stream_url)
                source_url = yt_info["url"]
                yt_title = yt_info["title"] if title == "بث مباشر" else title
                if not source_url:
                    bot.edit_message_text("❌ تعذّر استخراج رابط البث.",
                        chat_id=msg.chat.id, message_id=status_msg.message_id)
                    return
                bot.edit_message_text(f"جاري بدء البث: {yt_title}... 📻",
                    chat_id=msg.chat.id, message_id=status_msg.message_id)
                if radio.is_streaming(msg.chat.id):
                    radio.stop_stream(msg.chat.id)
                radio.start_stream_yt(msg.chat.id, source_url, yt_title, yt_url=stream_url)
                bot.edit_message_text(
                    f"🔴 البث المباشر شغال!\n\n📺 {yt_title}\n\nللإيقاف: /stop",
                    chat_id=msg.chat.id, message_id=status_msg.message_id)
            except Exception as e:
                log.error(f"Channel radio YT error: {e}", exc_info=True)
                bot.edit_message_text(
                    f"خطأ في بدء البث: {e}\n\n"
                    "تأكد أن:\n1. البوت أدمن في القناة\n"
                    "2. عنده صلاحية إدارة المحادثات الصوتية\n"
                    "3. المحادثة الصوتية (Voice Chat) مفتوحة",
                    chat_id=msg.chat.id, message_id=status_msg.message_id)

        threading.Thread(target=_do_yt, daemon=True).start()
        return

    if radio.is_streaming(msg.chat.id):
        try:
            radio.change_stream(msg.chat.id, stream_url, title)
            bot.send_message(msg.chat.id, f"🔴 تم تغيير البث إلى: {title}")
        except Exception as e:
            bot.send_message(msg.chat.id, f"خطأ في تغيير البث: {e}")
        return

    status_msg = bot.send_message(msg.chat.id, "جاري بدء البث المباشر... 📻")
    try:
        radio.start_stream(msg.chat.id, stream_url, title)
        bot.edit_message_text(
            f"🔴 البث المباشر شغال الآن!\n\n"
            f"📻 {title}\n"
            f"🔗 {stream_url}\n\n"
            f"للإيقاف: /stop\n"
            f"إيقاف مؤقت: /pause\n"
            f"استئناف: /resume",
            chat_id=msg.chat.id,
            message_id=status_msg.message_id,
        )
    except ValueError as e:
        bot.edit_message_text(str(e), chat_id=msg.chat.id, message_id=status_msg.message_id)
    except Exception as e:
        log.error(f"Channel radio error: {e}", exc_info=True)
        bot.edit_message_text(
            f"خطأ في بدء البث: {e}\n\n"
            "تأكد أن:\n"
            "1. البوت أدمن في القناة\n"
            "2. عنده صلاحية إدارة المحادثات الصوتية\n"
            "3. المحادثة الصوتية (Voice Chat) مفتوحة",
            chat_id=msg.chat.id,
            message_id=status_msg.message_id,
        )


@bot.channel_post_handler(commands=["quran"])
def channel_quran(msg: Message):
    parts = msg.text.strip().split(maxsplit=1) if msg.text else []
    preset_key = parts[1].strip().lower() if len(parts) > 1 else "quran"

    if preset_key not in RADIO_PRESETS:
        bot.send_message(msg.chat.id,
            "الإذاعات المتاحة:\n\n"
            "• /quran – 📡 إذاعة القرآن الكريم من القاهرة\n"
            "• /quran quran2 – 🕌 إذاعة تراتيل\n"
            "• /quran quran3 – 🌙 إذاعة القرآن MBC"
        )
        return

    preset = RADIO_PRESETS[preset_key]
    status_msg = bot.send_message(msg.chat.id, f"جاري بدء {preset['title']}... 📻")

    def _do_ch_quran():
        try:
            radio.start_stream(msg.chat.id, preset["url"], preset["title"])
            bot.edit_message_text(
                f"🔴 البث المباشر شغال الآن!\n\n"
                f"📻 {preset['title']}\n\n"
                f"للإيقاف: /stop\n"
                f"إيقاف مؤقت: /pause\n"
                f"استئناف: /resume",
                chat_id=msg.chat.id,
                message_id=status_msg.message_id,
            )
        except ValueError as e:
            bot.edit_message_text(str(e), chat_id=msg.chat.id, message_id=status_msg.message_id)
        except Exception as e:
            log.error(f"Channel quran error: {e}", exc_info=True)
            backup = preset.get("backup_url")
            if backup:
                try:
                    radio.start_stream(msg.chat.id, backup, preset["title"] + " (احتياطي)")
                    bot.edit_message_text(
                        f"🔴 البث شغال (رابط احتياطي)!\n\n"
                        f"📻 {preset['title']}\n\n"
                        f"للإيقاف: /stop",
                        chat_id=msg.chat.id,
                        message_id=status_msg.message_id,
                    )
                    return
                except Exception as e2:
                    log.error(f"Backup stream also failed: {e2}", exc_info=True)
            bot.edit_message_text(
                f"❌ خطأ في بدء البث.\n\n"
                "تأكد أن:\n"
                "1. البوت أدمن في القناة\n"
                "2. عنده صلاحية إدارة المحادثات الصوتية\n"
                "3. المحادثة الصوتية (Voice Chat) مفتوحة\n\n"
                "جرب: /quran quran2",
                chat_id=msg.chat.id,
                message_id=status_msg.message_id,
            )

    threading.Thread(target=_do_ch_quran, daemon=True).start()


@bot.channel_post_handler(commands=["stop"])
def channel_stop(msg: Message):
    if not radio.is_streaming(msg.chat.id):
        bot.send_message(msg.chat.id, "لا يوجد بث مباشر حالياً.")
        return
    try:
        radio.stop_stream(msg.chat.id)
        bot.send_message(msg.chat.id, "⏹ تم إيقاف البث المباشر.")
    except Exception as e:
        bot.send_message(msg.chat.id, f"خطأ في إيقاف البث: {e}")


@bot.channel_post_handler(commands=["pause"])
def channel_pause(msg: Message):
    if not radio.is_streaming(msg.chat.id):
        bot.send_message(msg.chat.id, "لا يوجد بث مباشر حالياً.")
        return
    if radio.pause_stream(msg.chat.id):
        bot.send_message(msg.chat.id, "⏸ تم الإيقاف المؤقت.\nللاستئناف: /resume")
    else:
        bot.send_message(msg.chat.id, "خطأ في الإيقاف المؤقت.")


@bot.channel_post_handler(commands=["resume"])
def channel_resume(msg: Message):
    if not radio.is_streaming(msg.chat.id):
        bot.send_message(msg.chat.id, "لا يوجد بث مباشر حالياً.")
        return
    if radio.resume_stream(msg.chat.id):
        bot.send_message(msg.chat.id, "▶️ تم استئناف البث!")
    else:
        bot.send_message(msg.chat.id, "خطأ في استئناف البث.")


if __name__ == "__main__":
    log.info("Radio Bot starting...")

    import gc
    import signal
    import requests as _req

    gc.enable()

    def _restore_streams():
        time.sleep(15)
        state = radio.load_streams_state()

        yt_streams = state.get("yt", {})
        for tag, info in yt_streams.items():
            url = info.get("url", "")
            title = info.get("title", "بث مباشر")
            if not url:
                continue
            try:
                log.info(f"[Restore] Resuming YouTube stream [{tag}]: {title}")
                radio.start_youtube_stream(url, YT_RTMP_URL, YT_STREAM_KEY, title, tag)
                log.info(f"[Restore] YouTube stream [{tag}] restored")
            except Exception as e:
                log.error(f"[Restore] Failed to restore YT stream [{tag}]: {e}")

        tg_streams = state.get("tg", {})
        for cid_str, info in tg_streams.items():
            url = info.get("url", "")
            title = info.get("title", "بث مباشر")
            is_yt = info.get("is_yt", False)
            if not url:
                continue
            try:
                cid = int(cid_str)
                log.info(f"[Restore] Resuming TG stream in {cid}: {title}")
                if is_yt:
                    yt_original = info.get("yt_url") or url
                    log.info(f"[Restore] Extracting fresh YouTube URL for {cid}…")
                    try:
                        yt_info = radio.extract_youtube_url(yt_original)
                        fresh_url = yt_info["url"]
                    except Exception as ye:
                        log.error(f"[Restore] Failed to extract YouTube URL: {ye}")
                        fresh_url = url
                    radio.start_stream_yt(cid, fresh_url, title, yt_url=yt_original)
                else:
                    radio.start_stream(cid, url, title)
                log.info(f"[Restore] TG stream in {cid} restored")
            except Exception as e:
                log.error(f"[Restore] Failed to restore TG stream {cid_str}: {e}")

    threading.Thread(target=_restore_streams, daemon=True).start()
    log.info("Stream restore thread started")

    def _memory_cleaner():
        while True:
            time.sleep(180)
            gc.collect()
            log.info(f"GC cleanup done — garbage: {gc.get_count()}")

    threading.Thread(target=_memory_cleaner, daemon=True).start()
    log.info("Memory cleaner started (every 3 min)")

    # ── Clean shutdown on SIGTERM/SIGINT ──────────────────────────────────────
    def _shutdown_handler(signum, frame):
        log.info(f"Received signal {signum} — stopping bot cleanly...")
        for cid in list(radio._active_streams.keys()):
            try:
                radio.stop_stream(cid)
            except Exception:
                pass
        for tag in list(radio._yt_streams.keys()):
            try:
                radio.stop_youtube_stream(tag)
            except Exception:
                pass
        try:
            bot.stop_polling()
        except Exception:
            pass
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT,  _shutdown_handler)
    # ─────────────────────────────────────────────────────────────────────────

    def _force_close_other_sessions():
        """
        Kill any competing getUpdates session on Telegram's side.

        Strategy:
        1. Delete webhook (clears server-side webhook lock).
        2. Send getUpdates with timeout=2 — this creates a NEW long-poll slot on
           Telegram's server, which forces the OLD slot (from the previous process)
           to be closed immediately.  timeout=0 does NOT do this; only timeout>0 does.
        3. Send a final getUpdates(timeout=0, offset=-1) to mark all pending updates
           as seen so the next real poll starts cleanly.
        4. Wait long enough for Telegram's GC to expire the old connection (~5 s).
        """
        try:
            _req.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook",
                params={"drop_pending_updates": "true"},
                timeout=10,
            )
        except Exception:
            pass

        # Step 2: "steal" the long-poll slot with timeout=2 so Telegram kicks old conn
        try:
            _req.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                json={"timeout": 2, "limit": 1},
                timeout=8,
            )
        except Exception:
            pass

        # Step 3: clear pending updates
        try:
            _req.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                json={"offset": -1, "timeout": 0, "limit": 1},
                timeout=8,
            )
        except Exception:
            pass

        time.sleep(5)   # give Telegram time to fully expire the old connection

    _force_close_other_sessions()
    bot.remove_webhook()
    time.sleep(3)

    while True:
        try:
            log.info("Starting polling...")
            bot.polling(
                non_stop=False,
                timeout=20,
                long_polling_timeout=15,
                skip_pending=True,
                allowed_updates=["message", "callback_query", "channel_post"],
            )
        except Exception as e:
            err_str = str(e)
            log.error(f"Polling error: {e}")
            if "409" in err_str or "Conflict" in err_str:
                log.info("Conflict detected — forcing session close and retrying...")
                _force_close_other_sessions()
            else:
                time.sleep(5)
