# main.py
import asyncio
# ---- KEEP ALIVE SERVER FOR RENDER ----
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
import os

class PingHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Bot is alive')

def run_keep_alive_server():
    port = int(os.getenv("PORT", 10000))
    server = HTTPServer(("", port), PingHandler)
    print(f"[Render] Keep-alive HTTP server running on port {port}")
    server.serve_forever()

# запускаем сервер в отдельном потоке
threading.Thread(target=run_keep_alive_server, daemon=True).start()
# ---- END KEEP ALIVE ----
import os
import json
import urllib.request
import urllib.error
from urllib.parse import urlencode
import random
import string
import re
import logging
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    MenuButtonCommands,
    LabeledPrice,
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.storage.base import BaseStorage
from decimal import Decimal, ROUND_HALF_UP
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv
from database import Database
import html

def esc(value):
    """Экранирование текста от пользователя для сообщений с parse_mode='HTML'."""
    return html.escape(str(value), quote=False) if value is not None else ""

def prepare_markdown_v2(text):
    """Экранирует служебные символы MarkdownV2 (. ! - = и т.д.), оставляя * _ ` [ ] ( ) для форматирования."""
    return re.sub(r'(?<!\\)([~>#+\-=|{}.!])', r'\\\1', text)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
MODERATOR_GROUP_ID = os.getenv('MODERATOR_GROUP_ID')
CHANNEL_ID = os.getenv('CHANNEL_ID')

def _env_int(name, default):
    try:
        return int(os.getenv(name, str(default)).strip())
    except ValueError:
        return default

# Лобби создают игроки. Настройки (можно менять переменными окружения на Render):
MAX_OPEN_LOBBIES = _env_int('MAX_OPEN_LOBBIES', 3)                  # сколько лобби могут набирать игроков одновременно
LOBBY_EMPTY_TIMEOUT_MIN = _env_int('LOBBY_EMPTY_TIMEOUT_MIN', 15)   # через сколько минут пустое лобби удаляется
ADMINS_CAN_CREATE_LOBBY = os.getenv('ADMINS_CAN_CREATE_LOBBY', 'false').strip().lower() in ('true', '1', 'yes', 'on')
MENU_IMAGE_URL = os.getenv('MENU_IMAGE_URL')
PROFILE_IMAGE_URL = os.getenv('PROFILE_IMAGE_URL')
TOP_IMAGE_URL = os.getenv('TOP_IMAGE_URL')
SCREENSHOT_IMAGE_URL = os.getenv('SCREENSHOT_IMAGE_URL')
LOBBY_IMAGE_URL = os.getenv('LOBBY_IMAGE_URL', 'https://example.com/lobby.jpg')
PREMIUM_IMAGE_URL = os.getenv('PREMIUM_IMAGE_URL')
PREMIUM_LOG_CHAT_ID = os.getenv('PREMIUM_LOG_CHAT_ID') or MODERATOR_GROUP_ID
CRYPTOBOT_TOKEN = os.getenv("CRYPTOBOT_TOKEN", "").strip().strip('"').strip("'")
CRYPTOBOT_USE_TESTNET = os.getenv('CRYPTOBOT_USE_TESTNET', 'true').lower() in ('1', 'true', 'yes')

PREMIUM_30_DAYS = 30
PREMIUM_90_DAYS = 90
PREMIUM_PRICE_USD_30 = '1.01'
PREMIUM_PRICE_USD_90 = '2.02'
PREMIUM_STARS_30 = 29
PREMIUM_STARS_90 = 49
PROMO_PREMIUM_BONUS_RATING = 100  # бонус к рейтингу за Premium по промокоду (как при покупке)
FIRST_PURCHASE_DISCOUNT_PERCENT = max(0, min(90, _env_int('FIRST_PURCHASE_DISCOUNT_PERCENT', 20)))  # скидка на первую покупку, %

pending_cryptobot_invoices = {}

def set_pending_lobby_join(user_id, lobby_id):
    """Человек пришёл по ссылке из канала, но ещё не зарегистрирован: запоминаем лобби (в базе, переживает перезапуск)."""
    db.kv_set(f"pending_join:{user_id}", lobby_id)

def pop_pending_lobby_join(user_id):
    value = db.kv_get(f"pending_join:{user_id}")
    if value is None:
        return None
    db.kv_delete(f"pending_join:{user_id}")
    try:
        return int(value)
    except ValueError:
        return None

# ---- PREMIUM EMOJI ----
# Ключ = сам эмодзи (fallback), значение = (emoji_id, fallback).
PREMIUM_EMOJI = {
    "◀️": ("5256247952564825322", "◀️"),
    "⬅️": ("5465332604121140251", "⬅️"),
    "➡️": ("5375444869965709294", "➡️"),
    "🔄": ("5017470156276761427", "🔄"),
    "🕗": ("5389071572835250888", "🕗"),
    "🔎": ("5429419796988970289", "🔎"),
    "➖": ("5418206758365574104", "➖"),
    "✅": ("5429501538806548545", "✅"),
    "❌": ("5210952531676504517", "❌"),
    "🗑": ("5445267414562389170", "🗑"),
    "✔": ("5352537837986163736", "✔"),
    "🚪": ("5452029295109086676", "🚪"),
    "📸": ("5235837920081887219", "📸"),
    "📝": ("5334882760735598374", "📝"),
    "✏️": ("5395444784611480792", "✏️"),
    "🆔": ("5014902839575577394", "🆔"),
    "📥": ("5433811242135331842", "📥"),
    "🔹": ("5280817506255135006", "🔹"),
    "🎮": ("5467583879948803288", "🎮"),
    "🗺": ("5415803062738504079", "🗺"),
    "⏳": ("5258258882022612173", "⏳"),
    "🎯": ("5310278924616356636", "🎯"),
    "🌍": ("5399898266265475100", "🌍"),
    "🔫": ("5222486447306602688", "🔫"),
    "🎲": ("5388644605841387896", "🎲"),
    "👥": ("5190892530438260422", "👥"),
    "✨": ("5325547803936572038", "✨"),
    "❗️": ("5274099962655816924", "❗️"),
    "⚠️": ("5447644880824181073", "⚠️"),
    "⭐️": ("5447644863644320013", "⭐️"),
    "🌟": ("5064709487953183440", "🌟"),
    "🛒": ("5431499171045581032", "🛒"),
    "💵": ("5409048419211682843", "💵"),
    "💲": ("5285484782791177666", "💲"),
    "📆": ("5431897022456145283", "📆"),
    "🗓": ("5413879192267805083", "🗓"),
    "🔍": ("5231012545799666522", "🔍"),
    "📈": ("5244837092042750681", "📈"),
    "📊": ("5231200819986047254", "📊"),
    "🏆": ("5409008750893734809", "🏆"),
    "🎉": ("5461151367559141950", "🎉"),
    "➕": ("5397916757333654639", "➕"),
    "👤": ("5190892530438260422", "👤"),
    "👋": ("5440431182602842059", "👋"),
    "1️⃣": ("5352618497471983387", "1️⃣"),
    "2️⃣": ("5355062479892294112", "2️⃣"),
    "3️⃣": ("5352735814503670154", "3️⃣"),
    "4️⃣": ("5352943794000016773", "4️⃣"),
    "5️⃣": ("5352548674188652122", "5️⃣"),
    "6️⃣": ("5352844395571880506", "6️⃣"),
    "7️⃣": ("5352666540976156937", "7️⃣"),
    "8️⃣": ("5352932180408448516", "8️⃣"),
    "9️⃣": ("5353050734390715246", "9️⃣"),
    "🔟": ("5353005409600842868", "🔟"),
}

def pe(emoji: str) -> str:
    """Премиум-эмодзи для текстов сообщений (HTML).
    Если ID не задан — возвращает обычный fallback-эмодзи."""
    entry = PREMIUM_EMOJI.get(emoji)
    if not entry:
        return emoji
    eid, fb = entry
    if not eid:
        return fb
    return f'<tg-emoji emoji-id="{eid}">{fb}</tg-emoji>'

def pe_icon(emoji: str):
    """Emoji ID для инлайн-кнопки (icon_custom_emoji_id) или None."""
    entry = PREMIUM_EMOJI.get(emoji)
    if not entry or not entry[0]:
        return None
    return entry[0]

def btn(text: str, callback_data: str = None, url: str = None, icon: str = None):
    """Хелпер для InlineKeyboardButton с премиум-иконкой.

    Важно: если задан icon и для него есть премиум-ID, то ведущий эмодзи
    в тексте кнопки убирается — иначе Telegram нарисует и иконку, и
    обычный эмодзи, получится дубликат.
    """
    kwargs = {}
    if callback_data is not None:
        kwargs["callback_data"] = callback_data
    if url is not None:
        kwargs["url"] = url

    clean_text = text
    if icon:
        eid = pe_icon(icon)
        if eid:
            kwargs["icon_custom_emoji_id"] = eid
            # Убираем ведущий эмодзи (и пробел после него), если он совпадает с иконкой.
            # Учитываем, что у некоторых эмодзи есть variation selector (️).
            if clean_text.startswith(icon):
                clean_text = clean_text[len(icon):].lstrip()
            else:
                for suffix in ("️", ""):
                    pref = icon + suffix
                    if clean_text.startswith(pref):
                        clean_text = clean_text[len(pref):].lstrip()
                        break

    return InlineKeyboardButton(text=clean_text, **kwargs)
# ---- END PREMIUM EMOJI ----

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в .env файле")

class PostgresFSMStorage(BaseStorage):
    """Хранит состояния диалогов (регистрация, создание лобби и т.д.) в базе: перезапуск бота их не сбрасывает."""

    def __init__(self, database):
        self.database = database

    @staticmethod
    def _key(key):
        return (
            f"{key.bot_id}:{key.chat_id}:{key.user_id}:"
            f"{getattr(key, 'thread_id', None) or 0}:{getattr(key, 'destiny', 'default')}"
        )

    async def set_state(self, key, state=None):
        value = state.state if isinstance(state, State) else state
        self.database.fsm_set_state(self._key(key), value)

    async def get_state(self, key):
        return self.database.fsm_get(self._key(key))[0]

    async def set_data(self, key, data):
        self.database.fsm_set_data(self._key(key), json.dumps(dict(data), ensure_ascii=False, default=str))

    async def get_data(self, key):
        try:
            return json.loads(self.database.fsm_get(self._key(key))[1] or "{}")
        except ValueError:
            return {}

    async def close(self):
        pass


bot = Bot(token=BOT_TOKEN)
db = Database()
try:
    storage = PostgresFSMStorage(db)
except Exception as _storage_error:
    logger.error(f"FSM storage in database unavailable, using memory: {_storage_error}")
    storage = MemoryStorage()
dp = Dispatcher(storage=storage)

from collections import OrderedDict

class LRUCache:
    def __init__(self, capacity: int = 1000):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key):
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

    def delete(self, key):
        if key in self.cache:
            del self.cache[key]

    def clear(self):
        self.cache.clear()

user_lobby_messages = LRUCache(1000)
user_menu_messages = LRUCache(1000)

MODES = {
    'Pistol DM': {'key': 'pistol_dm', 'weapons': 'G22, USP, P350, TEC-9, F/S, Desert Eagle, Berretas'},
    'Rifle DM': {'key': 'rifle_dm', 'weapons': 'FN FAL, FAMAS, M4, M4A1, AKR, AKR12, M16'},
    'Sniper DM': {'key': 'sniper_dm', 'weapons': 'M40, M110, AWM'},
    'SMGs DM': {'key': 'smgs_dm', 'weapons': 'UMP45, MP5, MP7, P90, МАС10'},
    'Allguns DM': {'key': 'allguns dm', 'weapons': 'ДОСТУПНЫ ВСЕ ОРУЖИЯ'}
}

MAPS = ['🏜 SandStone', '🏘 Province', '🧱 Rust', '🏭 Zone 9', '🏝️ Breeze', '🌸 Sakura']
TIMES = ['3 минуты', '5 минут', '10 минут']
DAMAGE_TYPES = ['По всему телу', 'Только в голову']
REGIONS = ['Москва', 'Германия']

def load_admins():
    try:
        with open('admins.txt', 'r', encoding='utf-8') as f:
            admins = []
            for line in f:
                line = line.strip()
                if line:
                    try:
                        admins.append(int(line))
                    except ValueError as e:
                        logger.error(f"Invalid admin ID format in admins.txt: {line} - {e}")
            logger.info(f"Loaded {len(admins)} admins")
            return admins
    except FileNotFoundError:
        logger.error("Admins file not found")
        return []
    except Exception as e:
        logger.error(f"Error loading admins: {e}")
        return []

ADMINS = load_admins()
FILE_ADMINS = set(ADMINS)  # админы из admins.txt (из панели их убрать нельзя)

# Владелец бота (доступ к панели /kp). Задаётся переменной OWNER_ID, иначе — первый ID из admins.txt
_owner_env = os.getenv('OWNER_ID', '').strip()
if _owner_env.isdigit():
    OWNER_ID = int(_owner_env)
else:
    OWNER_ID = ADMINS[0] if ADMINS else None

def is_owner(user_id):
    return OWNER_ID is not None and user_id == OWNER_ID

# Админы, добавленные через панель владельца, хранятся в базе
try:
    for _uid in db.get_bot_admin_ids():
        if _uid not in ADMINS:
            ADMINS.append(_uid)
except Exception as _e:
    logger.error(f"Error loading admins from database: {_e}")
if OWNER_ID is not None and OWNER_ID not in ADMINS:
    ADMINS.append(OWNER_ID)

class RegistrationStates(StatesGroup):
    waiting_for_nickname = State()
    waiting_for_game_id = State()

class ScreenshotStates(StatesGroup):
    waiting_for_screenshot = State()

class ProfileEditStates(StatesGroup):
    waiting_for_new_nickname = State()
    waiting_for_new_game_id = State()

class CreateLobbyStates(StatesGroup):
    waiting_for_players = State()
    waiting_for_mode = State()
    waiting_for_map = State()
    waiting_for_time = State()
    waiting_for_damage = State()
    waiting_for_region = State()

def get_player_level(rating):
    levels = [
        (1001, "🔟"),
        (871, "9️⃣"),
        (751, "8️⃣"),
        (621, "7️⃣"),
        (521, "6️⃣"),
        (401, "5️⃣"),
        (321, "4️⃣"),
        (221, "3️⃣"),
        (101, "2️⃣"),
        (0, "1️⃣")
    ]
    for threshold, level in levels:
        if rating >= threshold:
            return pe(level)
    return pe("1️⃣")

def get_registration_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[
        btn("📝 Зарегистрироваться", callback_data="register", icon="📝")
    ]])

def get_main_keyboard(user_id=None):
    keyboard = [
        [btn("👤 Мой профиль", callback_data="profile", icon="👤")],
        [
            btn("⭐️ Premium", callback_data="premium_menu", icon="⭐️"),
            btn("🏆 Топ игроков", callback_data="top", icon="🏆"),
        ],
        [btn("🎮 Активные лобби", callback_data="active_lobbies", icon="🎮")],
    ]

    if user_id and can_create_lobby(user_id):
        keyboard.append([btn("➕ Создать лобби", callback_data="create_lobby", icon="➕")])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[
        btn("◀️ Назад", callback_data="back_to_main", icon="◀️")
    ]])

def get_cancel_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[
        btn("❌ Отмена", callback_data="cancel_registration", icon="❌")
    ]])

def get_screenshot_cancel_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[
        btn("❌ Отмена", callback_data="cancel_screenshot", icon="❌")
    ]])

def get_top_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [btn("◀️ Назад в меню", callback_data="back_to_main", icon="◀️")]
    ])

def get_profile_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [btn("🕗 Сыгранные лобби", callback_data="lobby_history", icon="🕗")],
        [btn("✏️ Редактировать профиль", callback_data="edit_profile", icon="✏️")],
        [btn("◀️ Назад", callback_data="back_to_main", icon="◀️")],
    ])

def get_edit_profile_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [btn("✏️ Изменить никнейм", callback_data="edit_profile_nickname", icon="✏️")],
        [btn("🆔 Изменить игровое ID", callback_data="edit_profile_game_id", icon="🆔")],
        [btn("◀️ К профилю", callback_data="profile", icon="◀️")],
    ])

def get_premium_period_keyboard(user_id=None):
    label_30 = "🗓 30 days — $1.01"
    label_90 = "📆 90 days — $2.02"
    if user_id:
        label_30 = f"🗓 30 days — {price_button_label(user_id, PREMIUM_30_DAYS)}"
        label_90 = f"📆 90 days — {price_button_label(user_id, PREMIUM_90_DAYS)}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [btn(label_30, callback_data="premium_pick_30", icon="🗓")],
        [btn(label_90, callback_data="premium_pick_90", icon="📆")],
        [
            btn("🎟 Промокод", callback_data="promo_enter", icon="🎟"),
            btn("🎁 Подарить Premium", callback_data="gift_start", icon="🎁"),
        ],
        [btn("◀️ Назад", callback_data="back_to_main", icon="◀️")],
    ])

def get_premium_payment_keyboard(days: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            btn("💲 CryptoBot", callback_data=f"premium_pay_crypto_{days}", icon="💲"),
            btn("⭐️ Telegram Stars", callback_data=f"premium_pay_stars_{days}", icon="⭐️"),
        ],
        [btn("◀️ Назад", callback_data="premium_menu", icon="◀️")],
    ])

def get_cancel_edit_profile_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[
        btn("❌ Отмена", callback_data="cancel_edit_profile", icon="❌")
    ]])

def get_lobby_history_keyboard(user_id, current_offset=0, has_next=False, total_lobbies=0):
    keyboard = []

    nav_buttons = []

    if current_offset > 0:
        nav_buttons.append(btn("⬅️ Назад", callback_data=f"history_prev_{user_id}_{current_offset - 5}", icon="⬅️"))

    nav_buttons.append(btn("◀️ К профилю", callback_data="profile", icon="◀️"))

    if has_next:
        nav_buttons.append(btn("Вперед ➡️", callback_data=f"history_next_{user_id}_{current_offset + 5}", icon="➡️"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([btn("🔎 Фильтры Premium", callback_data=f"hf|{user_id}|-1|-1|0|0|menu", icon="🔎")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def is_valid_game_id(game_id):
    return Database.is_valid_game_id_format(game_id)

def display_nickname(user_id, nickname):
    name = nickname or ""
    if is_admin(user_id):
        return f"{name} {pe('✔')}"          # админ — только ✔, без ⭐️
    if db.is_premium(user_id):
        return f"{name} {pe('⭐️')}"         # премиум без админки — только ⭐️
    return name

def premium_until_human(premium_until_str):
    if not premium_until_str:
        return ""
    try:
        dt = datetime.strptime(premium_until_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d.%m.%Y %H:%M")
    except ValueError:
        return premium_until_str


def post_purchase_menu_caption(first_name: str, days: int) -> str:
    return (
        f"Благодарим за покупку! Вам выдан Premium на {days} дней.\n\n"
        f"{pe('👋')} Привет, {esc(first_name)}!\n\nВыберите действие:"
    )


async def show_post_purchase_main_menu(chat_id: int, user_id: int, first_name: str, days: int):
    caption = post_purchase_menu_caption(first_name or "друг", days)
    await cleanup_user_messages(user_id)
    await send_message_with_image(chat_id, caption, MENU_IMAGE_URL, get_main_keyboard(user_id))


def _cryptobot_flatten_params(params):
    if not params:
        return {}
    out = {}
    for k, v in params.items():
        if v is None:
            continue
        if isinstance(v, (list, tuple)):
            out[k] = ",".join(str(x) for x in v)
        elif isinstance(v, bool):
            out[k] = "true" if v else "false"
        else:
            out[k] = str(v)
    return out


def cryptobot_api_call(method: str, params=None):
    if not CRYPTOBOT_TOKEN:
        return None, "CRYPTOBOT_TOKEN не задан в .env"
    base = (
        "https://testnet-pay.crypt.bot/api"
        if CRYPTOBOT_USE_TESTNET
        else "https://pay.crypt.bot/api"
    )
    flat = _cryptobot_flatten_params(params or {})
    qs = urlencode(flat)
    url = f"{base}/{method}"
    if qs:
        url = f"{url}?{qs}"
    req = urllib.request.Request(
        url,
        headers={
            "Crypto-Pay-API-Token": CRYPTOBOT_TOKEN.strip(),
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; TelegramBot/1.0)",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8")
        except Exception:
            err_body = str(e)
        msg = f"HTTP {e.code}: {err_body}"
        if e.code == 403 or "1010" in err_body:
            msg += (
                " Токен — из Crypto Pay → «Создать приложение» (@CryptoTestnetBot для testnet). "
                "Если уже так: проверьте .env без пробелов/кавычек вокруг токена."
            )
        return None, msg
    except Exception as e:
        return None, str(e)
    if not data.get("ok"):
        return None, data.get("error", {}).get("name", str(data))
    return data.get("result"), None

def cryptobot_get_invoice(invoice_id: int):
    result, err = cryptobot_api_call(
        "getInvoices", {"invoice_ids": str(invoice_id), "count": 100}
    )
    if err:
        return None, err
    items = []
    if isinstance(result, dict):
        items = result.get("items") or result.get("invoices") or []
    elif isinstance(result, list):
        items = result
    for inv in items:
        iid = inv.get("invoice_id")
        if iid is not None and int(iid) == int(invoice_id):
            return inv, None
    return None, "Счёт не найден в ответе API"

def cryptobot_invoice_is_paid(invoice_id: int):
    inv, err = cryptobot_get_invoice(invoice_id)
    if err:
        return False, err
    return inv.get("status") == "paid", None

async def finalize_premium_purchase(user_id: int, days: int, provider: str, provider_ref: str):
    if not db.is_user_registered(user_id):
        return False
    ok = await asyncio.to_thread(
        db.try_register_premium_payment, user_id, days, provider, provider_ref
    )
    if not ok:
        return False
    try:
        chat = await bot.get_chat(user_id)
        un = chat.username or ""
        line_user = f"{user_id} | @{un}" if un else f"{user_id} |"
        log_text = (
            f"{line_user}\n"
            f"Купил премиум на {days} дней!\n"
            f"Не забудьте выдать покупателю доступ в приватный чат и префикс в чате!\n"
            f"@uglymatthey"
        )
        if PREMIUM_LOG_CHAT_ID:
            await bot.send_message(chat_id=int(PREMIUM_LOG_CHAT_ID), text=log_text)
    except Exception as e:
        logger.error(f"finalize_premium log chat: {e}")
    return True

def get_lobby_actions_keyboard(lobby_id, user_id, is_creator=False, players_count=0, max_players=10, lobby_full=False):
    buttons = []

    if lobby_full:
        buttons.append([btn("📸 Отправить скриншот", callback_data=f"send_screenshot_{lobby_id}", icon="📸")])
    else:
        if is_creator:
            buttons.append([btn("🗑 Удалить лобби", callback_data=f"delete_lobby_{lobby_id}", icon="🗑")])
        else:
            user_in_lobby = db.is_user_in_lobby(user_id, lobby_id)
            if user_in_lobby:
                buttons.append([btn("🚪 Выйти из лобби", callback_data=f"leave_lobby_{lobby_id}", icon="🚪")])
            else:
                buttons.append([btn("✅ Присоединиться", callback_data=f"join_lobby_{lobby_id}", icon="✅")])

        buttons.append([btn("🔄 Обновить", callback_data=f"view_lobby_{lobby_id}", icon="🔄")])

        if is_admin(user_id) and not is_creator:
            buttons.append([btn("🗑 Удалить лобби (админ)", callback_data=f"delete_lobby_{lobby_id}", icon="🗑")])

    if not lobby_full:
        buttons.append([btn("◀️ Назад к списку", callback_data="active_lobbies", icon="◀️")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_lobbies_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[
        btn("🔄 Обновить", callback_data="active_lobbies", icon="🔄"),
        btn("◀️ Назад", callback_data="back_to_main", icon="◀️")
    ]])

def get_lobby_list_keyboard(lobbies):
    keyboard = []
    for lobby in lobbies:
        lobby_id = lobby[0]
        lobby_unique_id = lobby[1]
        player_count = lobby[15]
        max_players = lobby[9]

        keyboard.append([
            btn(
                f"Лобби #{lobby_unique_id} {player_count}/{max_players}",
                callback_data=f"view_lobby_{lobby_id}",
                icon="🎮"
            )
        ])
    keyboard.append([btn("◀️ Назад", callback_data="back_to_main", icon="◀️")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_mode_keyboard():
    keyboard = [[btn(mode, callback_data=f"mode_{MODES[mode]['key']}", icon="🎮")] for mode in MODES]
    keyboard.append([btn("◀️ Назад", callback_data="create_lobby", icon="◀️")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_map_keyboard():
    keyboard = []
    for i in range(0, len(MAPS), 2):
        row = [btn(MAPS[i], callback_data=f"map_{i}")]
        if i + 1 < len(MAPS):
            row.append(btn(MAPS[i + 1], callback_data=f"map_{i + 1}"))
        keyboard.append(row)
    keyboard.append([btn("◀️ Назад", callback_data="create_lobby", icon="◀️")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_time_keyboard():
    keyboard = [[btn(time, callback_data=f"time_{i}", icon="⏳")] for i, time in enumerate(TIMES)]
    keyboard.append([btn("◀️ Назад", callback_data="create_lobby", icon="◀️")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_damage_keyboard():
    keyboard = [[btn(damage_type, callback_data=f"damage_{i}", icon="🎯")] for i, damage_type in enumerate(DAMAGE_TYPES)]
    keyboard.append([btn("◀️ Назад", callback_data="create_lobby", icon="◀️")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_region_keyboard():
    keyboard = [[btn(region, callback_data=f"region_{i}", icon="🌍")] for i, region in enumerate(REGIONS)]
    keyboard.append([btn("◀️ Назад", callback_data="create_lobby", icon="◀️")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def can_create_lobby(user_id):
    """Лобби создают игроки. Администраторы не создают (если не включено ADMINS_CAN_CREATE_LOBBY)."""
    if is_admin(user_id) and not ADMINS_CAN_CREATE_LOBBY:
        return False
    return True

def is_admin(user_id):
    return user_id in ADMINS

async def safe_delete_message(chat_id, message_id):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        return True
    except TelegramBadRequest as e:
        if "message to delete not found" in str(e):
            logger.debug(f"Message {message_id} already deleted")
            return True
        else:
            logger.warning(f"Error deleting message {message_id}: {e}")
            return False
    except Exception as e:
        logger.warning(f"Unexpected error deleting message {message_id}: {e}")
        return False

async def send_message_with_image(chat_id, text, image_url=None, reply_markup=None):
    try:
        if image_url:
            message = await bot.send_photo(chat_id=chat_id, photo=image_url, caption=text, reply_markup=reply_markup, parse_mode='HTML')
        else:
            message = await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode='HTML')

        user_menu_messages.put(chat_id, message.message_id)
        return message
    except Exception as e:
        logger.error(f"Error sending message with image to {chat_id}: {e}")
        try:
            message = await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode='HTML')
            user_menu_messages.put(chat_id, message.message_id)
            return message
        except Exception as e2:
            logger.error(f"Error sending fallback message to {chat_id}: {e2}")
            return None

async def edit_message_with_image_safe(message, text, image_url=None, reply_markup=None):
    try:
        await message.delete()
    except Exception as e:
        logger.warning(f"Error deleting message: {e}")
    return await send_message_with_image(message.chat.id, text, image_url, reply_markup)

async def update_lobby_message_for_all_players(lobby_id):
    try:
        lobby_info = db.get_lobby_by_id(lobby_id)
        if not lobby_info:
            logger.warning(f"Lobby {lobby_id} not found for update")
            return

        players = db.get_lobby_players(lobby_id)
        players_count = len(players)
        max_players = lobby_info[9]
        lobby_full = players_count >= max_players

        creator_user_info = await bot.get_chat(lobby_info[2])
        creator_first_name = creator_user_info.first_name if creator_user_info else None

        lobby_text, _ = format_lobby_info(lobby_info, players, creator_first_name)

        all_users = players + [(lobby_info[2], creator_first_name or "Создатель")]

        for player_id, player_nickname in all_users:
            try:
                is_creator = player_id == lobby_info[2]
                reply_markup = get_lobby_actions_keyboard(
                    lobby_id,
                    player_id,
                    is_creator=is_creator,
                    players_count=players_count,
                    max_players=max_players,
                    lobby_full=lobby_full
                )

                old_message_id = user_lobby_messages.get(player_id)
                if old_message_id:
                    try:
                        await safe_delete_message(chat_id=player_id, message_id=old_message_id)
                    except Exception as e:
                        logger.warning(f"Error deleting old lobby message for user {player_id}: {e}")

                new_message = await bot.send_message(
                    chat_id=player_id,
                    text=lobby_text,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )

                user_lobby_messages.put(player_id, new_message.message_id)

            except Exception as e:
                logger.error(f"Error updating lobby message for user {player_id}: {e}")
                continue
    except Exception as e:
        logger.error(f"Error in update_lobby_message_for_all_players: {e}")

async def redirect_all_players_to_active_lobbies(lobby_id):
    try:
        players = db.get_lobby_players(lobby_id)
        lobby_info = db.get_lobby_by_id(lobby_id)
        lobby_unique_id = lobby_info[1] if lobby_info else "?"

        for player_id, player_nickname in players:
            try:
                await cleanup_lobby_messages(player_id)
                await cleanup_user_messages(player_id)

                try:
                    await bot.send_message(
                        chat_id=player_id,
                        text=f"{pe('❌')} Лобби #{lobby_unique_id} было удалено хостером!",
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logger.warning(f"Error sending alert to {player_id}: {e}")

                message_text = f"{pe('❌')} Лобби #{lobby_unique_id} было удалено хостером!\n\nВозвращаем вас к списку активных лобби..."

                lobbies = db.get_active_lobbies()

                if not lobbies:
                    await send_message_with_image(player_id, f"{message_text}\n\n{pe('❌')} Нет активных лобби", None, get_lobbies_keyboard())
                else:
                    await send_message_with_image(player_id, f"{message_text}\n\nВыберите лобби для просмотра:", None, get_lobby_list_keyboard(lobbies))

            except Exception as e:
                logger.error(f"Error redirecting player {player_id} to active lobbies: {e}")
                continue

    except Exception as e:
        logger.error(f"Error in redirect_all_players_to_active_lobbies: {e}")

async def create_lobby_forum_topic(lobby_unique_id, lobby_info, players):
    try:
        topic_name = f"🎮 Лобби #{lobby_unique_id}"
        result = await bot.create_forum_topic(chat_id=MODERATOR_GROUP_ID, name=topic_name)
        topic_thread_id = result.message_thread_id

        lobby_id, _, creator_id, _, mode, map_name, _, _, _, max_players, _, _, _, creator_name, _ = lobby_info[:15]

        creator_user = await bot.get_chat(creator_id)
        creator_tg_name = creator_user.first_name if creator_user else "Неизвестно"

        topic_text = (
            f"{pe('📥')} Новое лобби #{lobby_unique_id}\n\n"
            f"{pe('🗺')} Карта: {map_name}\n"
            f"{pe('🎮')} Режим: {mode}\n"
            f"{pe('👤')} Хостер: {esc(creator_tg_name)}\n\n"
            f"Игроки:\n"
        )

        for player_id, player_nickname in players:
            player_data = db.get_player_profile(player_id)
            if player_data:
                rating = player_data[5]
                level = get_player_level(rating)
                topic_text += f"{level} {display_nickname(player_id, player_nickname)}\n<code>{player_id}</code>\n"

        topic_text += f"{pe('⏳')} Ожидаем скриншоты с результатами матча....."

        await bot.send_message(
            chat_id=MODERATOR_GROUP_ID,
            message_thread_id=topic_thread_id,
            text=topic_text,
            parse_mode='HTML'
        )

        return topic_thread_id
    except Exception as e:
        logger.error(f"Error creating lobby topic: {e}")
        return None

async def notify_player_about_processing(user_id, lobby_unique_id, kills, deaths, rating_added):
    try:
        if not db.is_user_registered(user_id):
            logger.warning(f"User {user_id} not registered, skipping notification")
            return

        player = db.get_player_profile(user_id)
        if player:
            lines = [
                f"{pe('✅')} Ваш скриншот для лобби #{lobby_unique_id} успешно обработан!",
                "",
                f"{pe('📊')} Начисленная статистика:",
                f"• Убийств: +{kills}",
                f"• Смертей: +{deaths}",
                f"{pe('🏆')} Рейтинг: +{rating_added}",
            ]
            base_r = kills + 1
            if db.is_premium(user_id) and rating_added != base_r:
                lines.append(f"{pe('⭐️')} Premium: к +{base_r} применён множитель ×1.5")
            lines.extend(["", "По вопросам к @uglymatthey"])
            notification_text = "\n".join(lines)
            await bot.send_message(chat_id=user_id, text=notification_text, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Error notifying player {user_id}: {e}")

_bot_username_cache = None

async def get_bot_username():
    """Имя бота для ссылок (берётся у Телеграма, а не зашито в коде)."""
    global _bot_username_cache
    if not _bot_username_cache:
        try:
            me = await bot.get_me()
            _bot_username_cache = me.username
        except Exception as e:
            logger.error(f"Cannot get bot username: {e}")
            _bot_username_cache = (os.getenv('BOT_USERNAME') or '').lstrip('@') or None
    return _bot_username_cache

async def make_bot_link(start_param=None):
    username = await get_bot_username()
    if not username:
        return None
    return f"https://t.me/{username}" + (f"?start={start_param}" if start_param else "")

async def send_lobby_view_to_user(user_id, lobby_id):
    """Отправляет одному игроку актуальное сообщение лобби."""
    lobby_info = db.get_lobby_by_id(lobby_id)
    if not lobby_info:
        return False
    players = db.get_lobby_players(lobby_id)
    players_count = len(players)
    max_players = lobby_info[9]
    creator_user_info = await bot.get_chat(lobby_info[2])
    creator_first_name = creator_user_info.first_name if creator_user_info else None
    lobby_text, _ = format_lobby_info(lobby_info, players, creator_first_name)
    keyboard = get_lobby_actions_keyboard(
        lobby_id, user_id,
        is_creator=(user_id == lobby_info[2]),
        players_count=players_count,
        max_players=max_players,
        lobby_full=players_count >= max_players
    )
    old_message_id = user_lobby_messages.get(user_id)
    if old_message_id:
        await safe_delete_message(chat_id=user_id, message_id=old_message_id)
    new_message = await bot.send_message(chat_id=user_id, text=lobby_text, reply_markup=keyboard, parse_mode='HTML')
    user_lobby_messages.put(user_id, new_message.message_id)
    return True

async def join_lobby_from_link(chat_id, user_id, lobby_id):
    """Вход в лобби по кнопке из канала (ссылка t.me/бот?start=lobby_<id>)."""
    error = None
    lobby_info = db.get_lobby_by_id(lobby_id)
    if not lobby_info:
        error = "Это лобби уже недоступно: оно удалено или завершено."
    elif db.is_user_in_lobby(user_id, lobby_id):
        await send_lobby_view_to_user(user_id, lobby_id)
        return
    else:
        success, error = await perform_lobby_join(user_id, lobby_id)
        if success:
            return  # сообщение с лобби игрок получает из update_lobby_message_for_all_players
    await bot.send_message(chat_id=chat_id, text=f"❌ {error}")
    await send_message_with_image(chat_id, f"{pe('👋')} Выберите действие:", MENU_IMAGE_URL, get_main_keyboard(user_id))

async def send_lobby_to_channel(lobby_id):
    try:
        lobby_info = db.get_lobby_info_for_channel(lobby_id)
        if not lobby_info or not CHANNEL_ID:
            return None

        (lobby_unique_id, mode, map_name, time_limit, damage_type,
         region, creator_username, creator_id) = lobby_info

        mode_weapons = MODES.get(mode, {}).get('weapons', 'Доступны все оружия!')

        creator_user = await bot.get_chat(creator_id)
        creator_tg_name = creator_user.first_name if creator_user else "Неизвестно"

        message_text = (
            f"Лобби #{lobby_unique_id} активно{pe('❗️')}\n\n"
            f"{pe('🎮')} Режим: {mode} от {esc(creator_tg_name)}\n"
            f"Оружия: {mode_weapons}\n"
            f"Карта: {map_name}\n"
            f"{pe('⏳')} Время: {time_limit}\n"
            f"{pe('🎯')} Урон: {damage_type}\n"
            f"{pe('🌍')} Регион: {region}"
        )

        join_url = await make_bot_link(f"lobby_{lobby_id}")
        join_keyboard = None
        plain_keyboard = None
        if join_url:
            join_keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [btn("🎮 Присоединиться к лобби", url=join_url, icon="🎮")]
                ]
            )
            plain_keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🎮 Присоединиться к лобби", url=join_url)]
                ]
            )

        try:
            message = await bot.send_message(
                chat_id=CHANNEL_ID,
                text=message_text,
                reply_markup=join_keyboard,
                parse_mode='HTML'
            )
        except Exception as premium_error:
            # В каналах премиум-эмодзи доступны не всегда — шлём обычную версию
            logger.warning(f"Channel post with premium emoji failed ({premium_error}), sending plain version")
            message = await bot.send_message(
                chat_id=CHANNEL_ID,
                text=html.unescape(re.sub(r"<[^>]+>", "", message_text)),
                reply_markup=plain_keyboard
            )

        return message.message_id
    except Exception as e:
        logger.error(f"Error sending lobby to channel: {e}")
        return None

async def delete_lobby_channel_message(lobby_id):
    try:
        channel_message_id = db.get_lobby_channel_message_id(lobby_id)

        if channel_message_id and CHANNEL_ID:
            await bot.delete_message(chat_id=CHANNEL_ID, message_id=channel_message_id)
            return True
    except Exception as e:
        logger.error(f"Error deleting channel message: {e}")
    return False

def format_lobby_info(lobby_info, players, creator_first_name=None):
    if not lobby_info:
        return f"{pe('❌')} Информация о лобби не найдена", False

    try:
        lobby_id, lobby_unique_id, creator_id, lobby_link, mode, map_name, time_limit, damage_type, region, max_players, current_players, _, _, creator_name, creator_username, _ = lobby_info[:16]

        mode_weapons = MODES.get(mode, {}).get('weapons', 'Доступны все оружия!')

        creator_display = esc(creator_first_name) if creator_first_name else "Неизвестно"
        if is_admin(creator_id):
            creator_display = f"{creator_display} {pe('✅')}"

        creator_game_id = db.get_player_game_id(creator_id) or "Неизвестно"

        players_count = len(players)
        lobby_full = players_count >= max_players

        lobby_text = (
            f"{pe('✨')} Лобби #{lobby_unique_id} {pe('✨')}\n\n"
            f"{pe('🎲')} Режим: {mode} от {creator_display} (id: <code>{creator_game_id}</code>)\n"
            f"{pe('🔫')} Оружия: {mode_weapons}\n"
            f"{pe('🗺')} Карта: {map_name}\n"
            f"{pe('⏳')} Время: {time_limit}\n"
            f"{pe('🎯')} Урон: {damage_type}\n"
            f"{pe('🌍')} Регион: {region}\n\n"
        )

        if lobby_full:
            lobby_text += f"{pe('✅')} Матч готов! Набралось {players_count} игроков\n\n<b>{pe('👥')} Игроки в лобби:</b>\n"
        else:
            lobby_text += f"{pe('👥')} Игроков в лобби: {players_count}/{max_players}\n{pe('⏳')} Ожидаем еще {max_players - players_count} игроков...\n\n<b>{pe('👥')} Игроки в лобби:</b>\n"

        for i, (player_id, player_nickname) in enumerate(players, 1):
            lobby_text += f"{i}. {display_nickname(player_id, player_nickname)}\n"

        if not players:
            lobby_text += "Пока никто не присоединился\n"

        return lobby_text, lobby_full
    except Exception as e:
        logger.error(f"Error formatting lobby info: {e}")
        return f"{pe('❌')} Ошибка при форматировании информации о лобби", False

async def cleanup_user_messages(user_id):
    old_message_id = user_menu_messages.get(user_id)
    if old_message_id:
        try:
            await safe_delete_message(chat_id=user_id, message_id=old_message_id)
            user_menu_messages.delete(user_id)
        except Exception as e:
            logger.warning(f"Error cleaning up user message for {user_id}: {e}")

async def cleanup_lobby_messages(user_id):
    try:
        old_message_id = user_lobby_messages.get(user_id)
        user_lobby_messages.delete(user_id)
        if old_message_id:
            await safe_delete_message(chat_id=user_id, message_id=old_message_id)
    except Exception as e:
        logger.warning(f"Error in cleanup_lobby_messages for {user_id}: {e}")

async def set_menu_button(user_id):
    try:
        await bot.set_chat_menu_button(
            chat_id=user_id,
            menu_button=MenuButtonCommands(type="commands")
        )
    except Exception as e:
        logger.error(f"Error setting menu button: {e}")

async def set_bot_commands_for_user(user_id):
    commands = [
        types.BotCommand(command="start", description="🎮 Главное меню"),
        types.BotCommand(command="profile", description="👤 Мой профиль"),
    ]

    if is_admin(user_id):
        commands.extend([
            types.BotCommand(command="post", description="📢 Рассылка"),
            types.BotCommand(command="botstat", description="📊 Статистика бота"),
            types.BotCommand(command="upd", description="📈 Обновить статистику"),
            types.BotCommand(command="backupd", description="↩️ Откатить статистику"),
            types.BotCommand(command="postmatch", description="📣 Итоги матча в канал")
        ])

    if is_owner(user_id):
        commands.append(types.BotCommand(command="kp", description="🛠 Панель владельца"))

    try:
        await bot.set_my_commands(commands, scope=types.BotCommandScopeChat(chat_id=user_id))
    except Exception as e:
        logger.warning(f"Error setting commands for user {user_id}: {e}")
        try:
            await bot.set_my_commands(commands)
        except Exception as e2:
            logger.error(f"Error setting global commands: {e2}")

async def send_broadcast_message(user_id, text, parse_mode=None):
    try:
        await bot.send_message(
            chat_id=user_id,
            text=text,
            parse_mode=parse_mode,
            disable_web_page_preview=True
        )
        return True
    except Exception as e:
        logger.warning(f"Не удалось отправить сообщение пользователю {user_id}: {e}")
        return False

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    first_name = message.from_user.first_name

    await set_menu_button(user_id)
    await set_bot_commands_for_user(user_id)

    await cleanup_user_messages(user_id)

    args = message.text.split()
    if len(args) > 1 and args[1].startswith("lobby_"):
        try:
            link_lobby_id = int(args[1][len("lobby_"):])
        except ValueError:
            link_lobby_id = None
        if link_lobby_id is not None:
            if db.is_user_registered(user_id):
                await join_lobby_from_link(message.chat.id, user_id, link_lobby_id)
            else:
                set_pending_lobby_join(user_id, link_lobby_id)
                await message.answer(
                    f"{pe('👋')} Привет, {esc(first_name)}!\n\nДля участия в лобби нужно зарегистрироваться. "
                    f"После регистрации я сразу добавлю вас в лобби:",
                    reply_markup=get_registration_keyboard(),
                    parse_mode='HTML'
                )
            return

    if len(args) > 1 and args[1] == "join_lobby":
        if db.is_user_registered(user_id):
            await send_message_with_image(message.chat.id,
                                        f"{pe('🎮')} Добро пожаловать! Вы перешли по ссылке присоединения к лобби.\n\nВыберите действие:",
                                        MENU_IMAGE_URL,
                                        get_main_keyboard(user_id))
        else:
            await message.answer(
                f"{pe('👋')} Привет, {first_name}!\n\nДля участия в лобби нужно зарегистрироваться:",
                reply_markup=get_registration_keyboard(),
                parse_mode='HTML'
            )
        return

    if db.is_user_registered(user_id):
        menu_text = f"{pe('👋')} Привет, {esc(first_name)}!\n\nВыберите действие:"
        await send_message_with_image(
            message.chat.id,
            menu_text,
            MENU_IMAGE_URL,
            get_main_keyboard(user_id)
        )
    else:
        start_message = await message.answer(
            f"{pe('👋')} Привет, {first_name}!\n\nНажмите чтобы зарегистрироваться:",
            reply_markup=get_registration_keyboard(),
            parse_mode='HTML'
        )
        user_menu_messages.put(user_id, start_message.message_id)

@dp.message(Command("profile"))
async def cmd_profile(message: types.Message):
    await show_profile_from_message(message)

async def show_profile_from_message(message: types.Message):
    user_id = message.from_user.id

    if not db.is_user_registered(user_id):
        await message.answer(f"{pe('❌')} Вы не зарегистрированы!", parse_mode='HTML')
        return

    player_data = db.get_player_profile(user_id)
    if not player_data:
        await message.answer(f"{pe('❌')} Профиль не найден!", parse_mode='HTML')
        return

    (
        user_id_db,
        username,
        nickname,
        game_id,
        registration_date,
        rating,
        matches_played,
        kills,
        deaths,
        _nick_ch,
        _gid_ch,
        premium_until,
    ) = player_data

    kd_ratio = kills / deaths if deaths > 0 else kills
    level = get_player_level(rating)

    username_display = display_nickname(user_id, nickname)
    prem_line = ""
    if premium_until and db.is_premium(user_id):
        days_left = db.get_premium_days_left(user_id)
        prem_line = f"\n{pe('⭐️')} Премиум: {days_left} дней\n"

    profile_text = (
        f"{pe('👤')} Ваш профиль | {username_display}\n\n"
        f"{pe('🆔')} <code>{game_id}</code>\n"
        f"{pe('📈')} Уровень: {level}\n"
        f"{pe('🏆')} Рейтинг: {rating}{prem_line}\n"
        f"{pe('📊')} Статистика:\n"
        f"• Убийств: {kills}\n"
        f"• Смертей: {deaths}\n"
        f"• K/D: {kd_ratio:.2f}\n"
        f"• Матчей сыграно: {matches_played}"
    )

    await cleanup_user_messages(user_id)
    await send_message_with_image(user_id, profile_text, PROFILE_IMAGE_URL, get_profile_keyboard())

@dp.message(Command("upd"))
async def cmd_update_stats(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer(f"{pe('❌')} Нет прав для этой команды.", parse_mode='HTML')
        return

    if str(message.chat.id) != MODERATOR_GROUP_ID:
        await message.answer(f"{pe('❌')} Эта команда работает только в группе модераторов!", parse_mode='HTML')
        return

    args = message.text.split()

    if len(args) == 4:
        try:
            tg_id, kills, deaths = int(args[1]), int(args[2]), int(args[3])

            if not (1 <= kills <= 1000) or not (1 <= deaths <= 1000):
                await message.answer(f"{pe('❌')} Количество киллов/смертей должно быть от 1 до 1000", parse_mode='HTML')
                return

            lobby_id = db.get_lobby_id_by_topic_thread_id(message.message_thread_id)
            if not lobby_id:
                await message.answer(f"{pe('❌')} Эта команда работает только в теме лобби!", parse_mode='HTML')
                return

            lobby_info = db.get_lobby_by_id(lobby_id)
            if not lobby_info:
                await message.answer(f"{pe('❌')} Лобби не найдено.", parse_mode='HTML')
                return

            lobby_unique_id = lobby_info[1]

            if not db.is_user_in_lobby(tg_id, lobby_id):
                await message.answer(f"{pe('❌')} Этот игрок не участвовал в этом лобби!", parse_mode='HTML')
                return

            if db.has_stats_been_added(tg_id, lobby_id):
                await message.answer(f"{pe('❌')} Статистика для этого игрока уже была начислена за это лобби. Сначала сделайте откат командой /backupd.", parse_mode='HTML')
                return

            success, rating_added = db.update_player_stats_by_user_id(tg_id, kills, deaths, lobby_id)

            if success:
                await message.answer(f"{pe('✅')} Статистика для игрока {tg_id} в лобби #{lobby_unique_id} обновлена!", parse_mode='HTML')
                await notify_player_about_processing(tg_id, lobby_unique_id, kills, deaths, rating_added)
                try:
                    posted, _ = await maybe_post_match_result(lobby_id)
                    if posted:
                        await message.answer("📣 Результаты матча опубликованы в канале.")
                except Exception as e:
                    logger.error(f"Error posting match result: {e}")
            else:
                await message.answer(f"{pe('❌')} Ошибка при обновлении статистики.", parse_mode='HTML')

        except ValueError:
            await message.answer(f"{pe('❌')} Ошибка в формате чисел.", parse_mode='HTML')
    else:
        await message.answer("Используйте:\n/upd tg_id kills deaths\n\nОграничение: 1-1000 киллов/смертей")

@dp.message(Command("backupd"))
async def cmd_revert_stats(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer(f"{pe('❌')} Нет прав для этой команды.", parse_mode='HTML')
        return

    if str(message.chat.id) != MODERATOR_GROUP_ID:
        await message.answer(f"{pe('❌')} Эта команда работает только в группе модераторов!", parse_mode='HTML')
        return

    args = message.text.split()

    if len(args) == 2:
        try:
            tg_id = int(args[1])

            lobby_id = db.get_lobby_id_by_topic_thread_id(message.message_thread_id)
            if not lobby_id:
                await message.answer(f"{pe('❌')} Эта команда работает только в теме лобби!", parse_mode='HTML')
                return

            lobby_info = db.get_lobby_by_id(lobby_id)
            if not lobby_info:
                await message.answer(f"{pe('❌')} Лобби не найдено.", parse_mode='HTML')
                return

            lobby_unique_id = lobby_info[1]

            history_data = db.get_last_stats_history_by_lobby_user(lobby_id, tg_id)

            if not history_data:
                await message.answer(f"{pe('❌')} История изменений не найдена.", parse_mode='HTML')
                return

            history_id, user_id, screenshot_id, kills_added, deaths_added, rating_added, created_at = history_data

            if db.revert_stats(history_id):
                await message.answer(f"{pe('✅')} Изменения для игрока {tg_id} в лобби #{lobby_unique_id} отменены!", parse_mode='HTML')
                try:
                    notification_text = (
                        f"{pe('⚠️')} Статистика по лобби #{lobby_unique_id} была отменена модератором.\n\n"
                        f"Отмененная статистика:\n"
                        f"• Убийств: -{kills_added}\n"
                        f"• Смертей: -{deaths_added}\n"
                        f"{pe('🏆')} Рейтинг: -{rating_added}\n\n"
                        f"По вопросам к @uglymatthey"
                    )
                    await bot.send_message(chat_id=user_id, text=notification_text, parse_mode='HTML')
                except Exception as e:
                    logger.error(f"Error sending revert notification to {user_id}: {e}")
            else:
                await message.answer(f"{pe('❌')} Ошибка при отмене изменений.", parse_mode='HTML')

        except Exception as e:
            logger.error(f"Error in backupd command: {e}")
            await message.answer(f"{pe('❌')} Ошибка при выполнении команды: {str(e)}", parse_mode='HTML')
    else:
        await message.answer("Используйте: /backupd tg_id")

@dp.message(Command("post"))
async def cmd_post(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer(f"{pe('❌')} Нет прав для этой команды.", parse_mode='HTML')
        return

    args = message.text.split(' ', 1)
    if len(args) < 2:
        await message.answer(
            "📢 Используйте:\n"
            "/post ваш текст\n\n"
            "Поддерживаемое форматирование:\n"
            "• *жирный*\n"
            "• _курсив_\n"
            "• `моноширинный`\n"
            "• [ссылка](https://example.com)"
        )
        return

    broadcast_text = prepare_markdown_v2(args[1])

    await state.update_data(broadcast_text=broadcast_text)

    try:
        await message.answer(f"📢 Тестовое сообщение:\n\n{broadcast_text}", parse_mode='MarkdownV2', disable_web_page_preview=True)
    except Exception as e:
        await message.answer(f"{pe('❌')} Ошибка в форматировании Markdown: {str(e)}\n\nУбедитесь, что вы правильно используете * _ ` для форматирования.", parse_mode='HTML')
        return

    confirm_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            btn("✅ Да, отправить", callback_data="confirm_broadcast", icon="✅"),
            btn("❌ Отмена", callback_data="cancel_broadcast", icon="❌")
        ]
    ])

    await message.answer(
        f"📢 Подтвердите рассылку:\n\n"
        f"Текст: {broadcast_text}\n\n"
        f"Будет отправлено всем пользователям бота.",
        reply_markup=confirm_keyboard
    )

@dp.message(Command("botstat"))
async def cmd_botstat(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer(f"{pe('❌')} Нет прав для этой команды.", parse_mode='HTML')
        return

    try:
        db.cursor.execute("SELECT COUNT(*) FROM players")
        total_users = db.cursor.fetchone()[0]

        today = datetime.now().strftime("%Y-%m-%d")
        db.cursor.execute("SELECT COUNT(*) FROM players WHERE registration_date >= %s", (today,))
        new_users_today = db.cursor.fetchone()[0]

        db.cursor.execute("SELECT COUNT(*) FROM lobbies WHERE status = 'completed'")
        total_lobbies = db.cursor.fetchone()[0]

        stat_text = (
            f"{pe('👥')} Пользователи:\n"
            f"• Всего пользователей: {total_users}\n"
            f"• Новых за сегодня: {new_users_today}\n\n"
            f"{pe('🎮')} Лобби:\n"
            f"• Всего сыгранных лобби: {total_lobbies}"
        )

        await message.answer(stat_text, parse_mode='HTML')

    except Exception as e:
        logger.error(f"Error getting bot stats: {e}")
        await message.answer(f"{pe('❌')} Ошибка при получении статистики: {str(e)}", parse_mode='HTML')

@dp.callback_query(lambda c: c.data == "confirm_broadcast")
async def confirm_broadcast(callback_query: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ Нет прав для этой команды.", show_alert=True)
        return

    data = await state.get_data()
    broadcast_text = data.get('broadcast_text')

    if not broadcast_text:
        await callback_query.message.edit_text("❌ Ошибка: текст рассылки не найден")
        return

    await callback_query.message.edit_text("🔄 Начинаю рассылку...")

    db.cursor.execute("SELECT user_id FROM players")
    users = db.cursor.fetchall()

    total_users = len(users)
    successful_sends = 0
    failed_sends = 0

    progress_message = await callback_query.message.edit_text(
        f"📢 Рассылка начата...\n"
        f"Всего пользователей: {total_users}\n"
        f"✅ Успешно: 0\n"
        f"❌ Ошибок: 0\n"
        f"🔄 В процессе..."
    )

    for i, (user_id,) in enumerate(users):
        success = await send_broadcast_message(user_id, broadcast_text, parse_mode='MarkdownV2')

        if success:
            successful_sends += 1
        else:
            failed_sends += 1

        if (i + 1) % 10 == 0 or (i + 1) == total_users:
            try:
                await progress_message.edit_text(
                    f"📢 Рассылка...\n"
                    f"Всего пользователей: {total_users}\n"
                    f"✅ Успешно: {successful_sends}\n"
                    f"❌ Ошибок: {failed_sends}\n"
                    f"🔄 Прогресс: {i + 1}/{total_users} ({((i + 1) / total_users * 100):.1f}%)"
                )
            except Exception as e:
                logger.warning(f"Error updating progress message: {e}")

        await asyncio.sleep(0.1)

    await progress_message.edit_text(
        f"📬 Рассылка завершена.\n"
        f"Успешно: {successful_sends}\n"
        f"Не доставлено: {failed_sends}"
    )

    await state.clear()
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "cancel_broadcast")
async def cancel_broadcast(callback_query: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ Нет прав для этой команды.", show_alert=True)
        return

    await callback_query.message.edit_text("❌ Рассылка отменена.")
    await state.clear()
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "create_lobby")
async def start_create_lobby(callback_query: types.CallbackQuery, state: FSMContext):
    creator_check_id = callback_query.from_user.id
    if not db.is_user_registered(creator_check_id):
        await callback_query.answer("❌ Сначала зарегистрируйтесь", show_alert=True)
        return

    if not can_create_lobby(creator_check_id):
        await callback_query.answer("❌ Администраторы не создают лобби", show_alert=True)
        return

    if db.get_user_active_lobby(creator_check_id):
        await callback_query.answer("❌ Вы уже находитесь в активном лобби", show_alert=True)
        return

    if db.count_open_lobbies() >= MAX_OPEN_LOBBIES:
        await callback_query.answer(
            f"❌ Сейчас открыто максимум лобби ({MAX_OPEN_LOBBIES}). Подождите, пока освободится место, "
            f"или присоединитесь к одному из них.",
            show_alert=True
        )
        return

    await cleanup_user_messages(callback_query.from_user.id)
    await state.set_state(CreateLobbyStates.waiting_for_players)
    await send_message_with_image(callback_query.from_user.id, f"{pe('🎮')} Создание лобби\n\nВведите количество игроков для лобби (от 3 до 10):", None, get_cancel_keyboard())
    await callback_query.answer()

@dp.message(CreateLobbyStates.waiting_for_players)
async def process_lobby_players(message: types.Message, state: FSMContext):
    try:
        max_players = int(message.text.strip())

        if max_players < 2 or max_players > 10:
            await message.answer(f"{pe('❌')} Введите число от 2 до 10", parse_mode='HTML')
            return

        await state.update_data(max_players=max_players)
        await state.set_state(CreateLobbyStates.waiting_for_mode)

        await cleanup_user_messages(message.from_user.id)
        mode_text = f"{pe('🎮')} Выберите режим игры:\n\n" + "\n".join(f"• {mode}: {info['weapons']}" for mode, info in MODES.items())
        await send_message_with_image(message.from_user.id, mode_text, None, get_mode_keyboard())

    except ValueError:
        await message.answer(f"{pe('❌')} Пожалуйста, введите число от 3 до 10", parse_mode='HTML')

@dp.callback_query(CreateLobbyStates.waiting_for_mode, lambda c: c.data.startswith("mode_"))
async def process_lobby_mode(callback_query: types.CallbackQuery, state: FSMContext):
    mode_key = callback_query.data.replace("mode_", "")
    mode_name = next((name for name, info in MODES.items() if info['key'] == mode_key), None)

    if not mode_name:
        await callback_query.answer("❌ Неверный режим")
        return

    await state.update_data(mode=mode_name)
    await state.set_state(CreateLobbyStates.waiting_for_map)

    await cleanup_user_messages(callback_query.from_user.id)
    await send_message_with_image(callback_query.from_user.id, f"{pe('🗺')} Выберите карта:", None, get_map_keyboard())
    await callback_query.answer()

@dp.callback_query(CreateLobbyStates.waiting_for_map, lambda c: c.data.startswith("map_"))
async def process_lobby_map(callback_query: types.CallbackQuery, state: FSMContext):
    try:
        map_index = int(callback_query.data.replace("map_", ""))

        if 0 <= map_index < len(MAPS):
            await state.update_data(map_name=MAPS[map_index])
            await state.set_state(CreateLobbyStates.waiting_for_time)

            await cleanup_user_messages(callback_query.from_user.id)
            await send_message_with_image(callback_query.from_user.id, f"{pe('⏳')} Выберите время игры:", None, get_time_keyboard())
        else:
            await callback_query.answer("❌ Неверная карта")
    except ValueError:
        await callback_query.answer("❌ Ошибка в данных карты")
    await callback_query.answer()

@dp.callback_query(CreateLobbyStates.waiting_for_time, lambda c: c.data.startswith("time_"))
async def process_lobby_time(callback_query: types.CallbackQuery, state: FSMContext):
    try:
        time_index = int(callback_query.data.replace("time_", ""))

        if 0 <= time_index < len(TIMES):
            await state.update_data(time_limit=TIMES[time_index])
            await state.set_state(CreateLobbyStates.waiting_for_damage)

            await cleanup_user_messages(callback_query.from_user.id)
            await send_message_with_image(callback_query.from_user.id, f"{pe('🎯')} Выберите тип урона:", None, get_damage_keyboard())
        else:
            await callback_query.answer("❌ Неверное время")
    except ValueError:
        await callback_query.answer("❌ Ошибка в данных времени")
    await callback_query.answer()

@dp.callback_query(CreateLobbyStates.waiting_for_damage, lambda c: c.data.startswith("damage_"))
async def process_lobby_damage(callback_query: types.CallbackQuery, state: FSMContext):
    try:
        damage_index = int(callback_query.data.replace("damage_", ""))

        if 0 <= damage_index < len(DAMAGE_TYPES):
            await state.update_data(damage_type=DAMAGE_TYPES[damage_index])
            await state.set_state(CreateLobbyStates.waiting_for_region)

            await cleanup_user_messages(callback_query.from_user.id)
            await send_message_with_image(callback_query.from_user.id, f"{pe('🌍')} Выберите регион:", None, get_region_keyboard())
        else:
            await callback_query.answer("❌ Неверный тип урона")
    except ValueError:
        await callback_query.answer("❌ Ошибка в данных урона")
    await callback_query.answer()

@dp.callback_query(CreateLobbyStates.waiting_for_region, lambda c: c.data.startswith("region_"))
async def process_lobby_region(callback_query: types.CallbackQuery, state: FSMContext):
    try:
        region_index = int(callback_query.data.replace("region_", ""))
        user_id = callback_query.from_user.id

        if 0 <= region_index < len(REGIONS):
            region = REGIONS[region_index]
            data = await state.get_data()

            if db.count_open_lobbies() >= MAX_OPEN_LOBBIES:
                # пока игрок заполнял анкету, свободные места могли закончиться
                await state.clear()
                await cleanup_user_messages(user_id)
                await send_message_with_image(
                    user_id,
                    f"{pe('❌')} Пока вы создавали лобби, свободные места закончились (максимум {MAX_OPEN_LOBBIES}). Попробуйте позже.",
                    None,
                    get_main_keyboard(user_id)
                )
                await callback_query.answer()
                return

            lobby_id, lobby_unique_id = db.create_lobby(
                creator_id=user_id,
                lobby_link="",
                mode=data['mode'],
                map_name=data['map_name'],
                time_limit=data['time_limit'],
                damage_type=data['damage_type'],
                region=region,
                max_players=data.get('max_players', 10)
            )

            if lobby_id and lobby_unique_id:
                channel_message_id = await send_lobby_to_channel(lobby_id)
                if channel_message_id:
                    db.update_lobby_channel_message_id(lobby_id, channel_message_id)

                lobby_info = db.get_lobby_by_id(lobby_id)
                if lobby_info:
                    players = db.get_lobby_players(lobby_id)
                    players_count = len(players)
                    max_players = data.get('max_players', 10)

                    lobby_text, lobby_full = format_lobby_info(lobby_info, players, callback_query.from_user.first_name)

                    await cleanup_user_messages(user_id)

                    new_message = await bot.send_message(
                        chat_id=user_id,
                        text=lobby_text,
                        reply_markup=get_lobby_actions_keyboard(
                            lobby_id,
                            user_id,
                            is_creator=True,
                            players_count=players_count,
                            max_players=max_players,
                            lobby_full=lobby_full
                        ),
                        parse_mode='HTML'
                    )

                    user_lobby_messages.put(user_id, new_message.message_id)

                else:
                    await send_message_with_image(user_id, f"{pe('❌')} Ошибка: информация о лобби не найдена", None, get_main_keyboard(user_id))
            else:
                await send_message_with_image(user_id, f"{pe('❌')} Ошибка при создании лобби", None, get_main_keyboard(user_id))

            await state.clear()
        else:
            await callback_query.answer("❌ Неверный регион")
    except ValueError:
        await callback_query.answer("❌ Ошибка в данных региона")
    except Exception as e:
        logger.error(f"Error creating lobby: {e}")
        await send_message_with_image(callback_query.from_user.id, f"{pe('❌')} Ошибка при создании лобби", None, get_main_keyboard(callback_query.from_user.id))
        await state.clear()
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "active_lobbies")
async def show_active_lobbies(callback_query: types.CallbackQuery):
    lobbies = db.get_active_lobbies()

    await cleanup_user_messages(callback_query.from_user.id)
    await cleanup_lobby_messages(callback_query.from_user.id)

    if not lobbies:
        await send_message_with_image(callback_query.from_user.id, f"{pe('🎮')} Активные лобби\n\n{pe('❌')} Нет активных лобби", None, get_lobbies_keyboard())
    else:
        await send_message_with_image(callback_query.from_user.id, f"{pe('🎮')} Активные лобби\n\nВыберите лобби для просмотра:", None, get_lobby_list_keyboard(lobbies))
    await callback_query.answer()

@dp.callback_query(lambda c: c.data.startswith("view_lobby_"))
async def view_lobby(callback_query: types.CallbackQuery):
    try:
        lobby_id = int(callback_query.data.replace("view_lobby_", ""))
        lobby_info = db.get_lobby_by_id(lobby_id)
        user_id = callback_query.from_user.id

        if not lobby_info:
            await callback_query.answer("❌ Лобби не найдено", show_alert=True)
            return

        players = db.get_lobby_players(lobby_id)

        is_creator = lobby_info[2] == user_id
        players_count = len(players)
        max_players = lobby_info[9]
        lobby_full = players_count >= max_players

        creator_user_info = await bot.get_chat(lobby_info[2])
        creator_first_name = creator_user_info.first_name if creator_user_info else None

        lobby_text, _ = format_lobby_info(lobby_info, players, creator_first_name)

        try:
            await cleanup_lobby_messages(user_id)
            await cleanup_user_messages(user_id)

            new_message = await bot.send_message(
                chat_id=user_id,
                text=lobby_text,
                reply_markup=get_lobby_actions_keyboard(
                    lobby_id,
                    user_id,
                    is_creator=is_creator,
                    players_count=players_count,
                    max_players=max_players,
                    lobby_full=lobby_full
                ),
                parse_mode='HTML'
            )

            user_lobby_messages.put(user_id, new_message.message_id)

        except Exception as e:
            logger.error(f"Error sending new lobby message: {e}")

    except ValueError:
        await callback_query.answer("❌ Неверный ID лобби", show_alert=True)
    except Exception as e:
        logger.error(f"Error viewing lobby: {e}")
        await callback_query.answer("❌ Ошибка при загрузке лобби", show_alert=True)

    await callback_query.answer()

async def perform_lobby_join(user_id, lobby_id, on_joined=None):
    """Добавляет игрока в лобби и запускает всё, что должно произойти дальше.
    Возвращает (успех, текст_ошибки)."""
    try:
        if db.get_user_active_lobby(user_id):
            return False, "Вы уже находитесь в активном лобби"

        success, message = db.join_lobby(user_id, lobby_id)
        if not success:
            return False, message

        if on_joined:
            try:
                await on_joined()
            except Exception as e:
                logger.warning(f"on_joined failed: {e}")

        lobby_info = db.get_lobby_by_id(lobby_id)
        if not lobby_info:
            return False, "Лобби не найдено"

        players = db.get_lobby_players(lobby_id)
        players_count = len(players)
        max_players = lobby_info[9]

        lobby_full = players_count >= max_players

        if lobby_full and lobby_info[1]:
            db.complete_lobby(lobby_id)

            await delete_lobby_channel_message(lobby_id)

            existing_topic_thread_id = db.get_lobby_topic_thread_id(lobby_id)
            if not existing_topic_thread_id:
                topic_thread_id = await create_lobby_forum_topic(lobby_info[1], lobby_info, players)
                if topic_thread_id:
                    db.update_lobby_topic_thread_id(lobby_id, topic_thread_id)

            await update_lobby_message_for_all_players(lobby_id)

            try:
                notification_text = (
                    f"{pe('🎉')} Лобби №{lobby_info[1]} заполнено!\n\n"
                    f"{pe('✅')} Набралось {players_count} игроков\n"
                    f"{pe('🎮')} Скопируйте id хостера чтобы присоединиться.\n"
                    f"{pe('📸')} После игры отправьте скриншот с результатами!"
                )

                for player_id, player_nickname in players:
                    try:
                        await bot.send_message(chat_id=player_id, text=notification_text, parse_mode='HTML')
                    except Exception as e:
                        logger.warning(f"Error sending notification to {player_id}: {e}")
                        continue
            except Exception as e:
                logger.error(f"Error sending lobby filled notification: {e}")
        else:
            await update_lobby_message_for_all_players(lobby_id)

        return True, None
    except Exception as e:
        logger.error(f"Error joining lobby: {e}")
        return False, "Ошибка при присоединении к лобби"

@dp.callback_query(lambda c: c.data.startswith("join_lobby_"))
async def join_lobby(callback_query: types.CallbackQuery):
    try:
        lobby_id = int(callback_query.data.replace("join_lobby_", ""))
    except ValueError:
        await callback_query.answer("❌ Неверный ID лобби", show_alert=True)
        return
    user_id = callback_query.from_user.id

    async def answer_joined():
        await callback_query.answer("✅ Вы присоединились к лобби")

    success, error = await perform_lobby_join(user_id, lobby_id, on_joined=answer_joined)
    if not success:
        try:
            await callback_query.answer(f"❌ {error}", show_alert=True)
        except Exception as e:
            logger.warning(f"Cannot answer join error: {e}")

@dp.callback_query(lambda c: c.data.startswith("leave_lobby_"))
async def leave_lobby(callback_query: types.CallbackQuery):
    try:
        lobby_id = int(callback_query.data.replace("leave_lobby_", ""))
        user_id = callback_query.from_user.id

        lobby_info = db.get_lobby_by_id(lobby_id)
        if not lobby_info:
            await callback_query.answer("❌ Лобби не найдено", show_alert=True)
            return

        if db.leave_lobby(user_id, lobby_id):
            await callback_query.answer("✅ Вы вышли из лобби")

            await update_lobby_message_for_all_players(lobby_id)

            await cleanup_lobby_messages(user_id)
            await cleanup_user_messages(user_id)
            await show_active_lobbies(callback_query)

        else:
            await callback_query.answer("❌ Ошибка при выходе из лобби", show_alert=True)
    except ValueError:
        await callback_query.answer("❌ Неверный ID лобби", show_alert=True)
    except Exception as e:
        logger.error(f"Error leaving lobby: {e}")
        await callback_query.answer("❌ Ошибка при выходе из лобби", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("delete_lobby_"))
async def delete_lobby(callback_query: types.CallbackQuery):
    try:
        lobby_id = int(callback_query.data.replace("delete_lobby_", ""))
        user_id = callback_query.from_user.id

        lobby_info = db.get_lobby_by_id(lobby_id)
        deleted_by_admin = bool(lobby_info) and lobby_info[2] != user_id and is_admin(user_id)
        if not lobby_info or (lobby_info[2] != user_id and not deleted_by_admin):
            await callback_query.answer("❌ Вы не можете удалить это лобби", show_alert=True)
            return

        players = db.get_lobby_players(lobby_id)
        lobby_unique_id = lobby_info[1]
        lobby_creator_id = lobby_info[2]
        deleted_by_text = "администратором" if deleted_by_admin else "хостером"

        await delete_lobby_channel_message(lobby_id)

        success, deleted_lobby_unique_id, player_ids = db.delete_lobby(lobby_id)

        if success:
            await callback_query.answer("✅ Лобби удалено", show_alert=True)

            for player_id, player_nickname in players:
                try:
                    await cleanup_lobby_messages(player_id)
                    await cleanup_user_messages(player_id)

                    await bot.send_message(
                        chat_id=player_id,
                        text=f"{pe('❌')} Лобби #{lobby_unique_id} было удалено {deleted_by_text}!",
                        parse_mode='HTML'
                    )

                    lobbies = db.get_active_lobbies()
                    if not lobbies:
                        await send_message_with_image(
                            player_id,
                            f"{pe('❌')} Нет активных лобби",
                            None,
                            get_lobbies_keyboard()
                        )
                    else:
                        await send_message_with_image(
                            player_id,
                            "Выберите лобби для просмотра:",
                            None,
                            get_lobby_list_keyboard(lobbies)
                        )

                except Exception as e:
                    logger.error(f"Error notifying player {player_id}: {e}")
                    continue

            await cleanup_lobby_messages(user_id)
            await cleanup_user_messages(user_id)

            if deleted_by_admin:
                db.log_admin_action(user_id, "delete_lobby", lobby_creator_id, f"lobby #{lobby_unique_id}")
                if user_id not in [p[0] for p in players]:
                    await send_message_with_image(
                        user_id,
                        f"{pe('✅')} Лобби #{lobby_unique_id} удалено.",
                        None,
                        get_main_keyboard(user_id)
                    )

        else:
            await callback_query.answer("❌ Ошибка при удалении лобби", show_alert=True)
    except ValueError:
        await callback_query.answer("❌ Неверный ID лобби", show_alert=True)
    except Exception as e:
        logger.error(f"Error deleting lobby: {e}")
        await callback_query.answer("❌ Ошибка при удалении лобби", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("send_screenshot_"))
async def start_lobby_screenshot_upload(callback_query: types.CallbackQuery, state: FSMContext):
    try:
        lobby_id = int(callback_query.data.replace("send_screenshot_", ""))
        user_id = callback_query.from_user.id

        if not db.is_user_registered(user_id):
            await callback_query.answer("❌ Вы не зарегистрированы!", show_alert=True)
            return

        if not db.is_user_in_lobby(user_id, lobby_id):
            await callback_query.answer("❌ Вы не в этом лобби!", show_alert=True)
            return

        if db.has_player_submitted_screenshot(user_id, lobby_id):
            await callback_query.answer("❌ Вы уже отправили скриншот для этого лобби!", show_alert=True)
            return

        await state.set_state(ScreenshotStates.waiting_for_screenshot)
        await state.update_data(lobby_id=lobby_id)

        await cleanup_user_messages(user_id)
        await send_message_with_image(
            chat_id=user_id,
            text=f"{pe('📸')} Отправьте скриншот с результатами матча\n\n",
            reply_markup=get_screenshot_cancel_keyboard()
        )
    except ValueError:
        await callback_query.answer("❌ Неверный ID лобби", show_alert=True)
    except Exception as e:
        logger.error(f"Error starting screenshot upload: {e}")
        await callback_query.answer("❌ Ошибка при начале загрузки скриншота", show_alert=True)
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "cancel_screenshot")
async def cancel_screenshot(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback_query.from_user.id

    await cleanup_user_messages(user_id)
    await send_message_with_image(
        callback_query.from_user.id,
        f"{pe('❌')} Отправка скриншота отменена",
        None,
        get_back_keyboard()
    )
    await callback_query.answer()

@dp.message(ScreenshotStates.waiting_for_screenshot)
async def process_screenshot(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    lobby_id = data.get('lobby_id')

    if not message.photo:
        await message.answer(f"{pe('❌')} Пожалуйста, отправьте скриншот как фото.", parse_mode='HTML')
        return

    if not lobby_id:
        await message.answer(f"{pe('❌')} Ошибка: лобби не найдено.", parse_mode='HTML')
        await state.clear()
        return

    lobby_info = db.get_lobby_by_id(lobby_id)
    if not lobby_info:
        await message.answer(f"{pe('❌')} Лобби не найдено.", parse_mode='HTML')
        await state.clear()
        return

    lobby_unique_id = lobby_info[1]

    topic_thread_id = db.get_lobby_topic_thread_id(lobby_id)

    if not topic_thread_id:
        try:
            forum_topics = await bot.get_forum_topics(chat_id=MODERATOR_GROUP_ID)
            for topic in forum_topics.topics:
                if f"#{lobby_unique_id}" in topic.name:
                    topic_thread_id = topic.message_thread_id
                    db.update_lobby_topic_thread_id(lobby_id, topic_thread_id)
                    break
        except Exception as e:
            logger.error(f"Error getting forum topics: {e}")

    if not topic_thread_id:
        await message.answer(f"{pe('❌')} Тема лобби не найдена. Обратитесь к администратору.", parse_mode='HTML')
        await state.clear()
        return

    photo_file_id = message.photo[-1].file_id
    screenshot_id = db.add_screenshot_to_lobby(user_id, lobby_id, topic_thread_id)

    player_data = db.get_player_profile(user_id)
    player_nickname = (
        display_nickname(user_id, player_data[2]) if player_data else "Неизвестно"
    )

    username = f"@{message.from_user.username}" if message.from_user.username else ""
    user_info = f"{username} {user_id}" if username else f"{user_id}"

    caption = (
        f"{pe('📸')}Новый скриншот к лобби #{lobby_unique_id} от {user_info}\n\n"
        f"Требуется ручная обработка{pe('❗️')}\n"
        f"Команды: /upd tg_id kills deaths"
    )

    try:
        await bot.send_photo(
            chat_id=MODERATOR_GROUP_ID,
            message_thread_id=topic_thread_id,
            photo=photo_file_id,
            caption=caption,
            parse_mode='HTML'
        )

        await cleanup_user_messages(user_id)
        await send_message_with_image(
            user_id,
            f"{pe('✅')} Скриншот отправлен на модерацию!\n\n"
            f"{pe('📊')} После проверки статистика будет автоматически обновлена.\n\n"
            "По вопросам к @uglymatthey",
            None,
            get_back_keyboard()
        )
    except Exception as e:
        logger.error(f"Error sending screenshot: {e}")
        await send_message_with_image(user_id, f"{pe('❌')} Ошибка при отправке скриншота. Попробуйте позже.", None, get_back_keyboard())

    await state.clear()

@dp.callback_query(lambda c: c.data == "waiting")
async def waiting_click(callback_query: types.CallbackQuery):
    await callback_query.answer("⏳ Ожидаем игроков...", show_alert=False)

@dp.callback_query(lambda c: c.data == "register")
async def start_registration(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id

    if db.is_user_registered(user_id):
        await callback_query.answer("❌ Вы уже зарегистрированы!", show_alert=True)
        return

    await state.set_state(RegistrationStates.waiting_for_nickname)
    await cleanup_user_messages(user_id)
    await send_message_with_image(user_id, f"{pe('📝')} Введите ваш игровой никнейм:", None, get_cancel_keyboard())
    await callback_query.answer()

@dp.message(RegistrationStates.waiting_for_nickname)
async def process_nickname(message: types.Message, state: FSMContext):
    nickname = message.text.strip()
    user_id = message.from_user.id

    if len(nickname) < 3 or len(nickname) > 16:
        await message.answer(f"{pe('❌')} Никнейм должен быть от 3 до 16 символов", parse_mode='HTML')
        return

    if not re.match(r'^[a-zA-Z0-9_]+$', nickname):
        await message.answer(f"{pe('❌')} Никнейм должен содержать только английские буквы, цифры и подчеркивания", parse_mode='HTML')
        return

    await state.update_data(nickname=nickname)
    await state.set_state(RegistrationStates.waiting_for_game_id)

    await cleanup_user_messages(user_id)
    await send_message_with_image(
        user_id,
        f"{pe('🆔')} Введите ваш игровой ID (2–13 символов: только цифры и латинские буквы):",
        None,
        get_cancel_keyboard()
    )

@dp.message(RegistrationStates.waiting_for_game_id)
async def process_game_id(message: types.Message, state: FSMContext):
    game_id = message.text.strip()
    user_id = message.from_user.id

    if not is_valid_game_id(game_id):
        await message.answer(
            f"{pe('❌')} Игровой ID: от 2 до 13 символов, только цифры и латинские буквы",
            parse_mode='HTML'
        )
        return

    if db.is_game_id_taken(game_id):
        await message.answer(f"{pe('❌')} Этот игровой ID уже занят", parse_mode='HTML')
        return

    data = await state.get_data()
    nickname = data['nickname']
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name

    success, message_text = db.register_player(user_id, username, nickname, game_id)

    if success:
        try:
            await message.delete()
        except Exception as e:
            logger.warning(f"Error deleting game_id message: {e}")

        await cleanup_user_messages(user_id)

        await state.clear()
        menu_text = f"{pe('✅')} Регистрация завершена!\n\n{pe('👋')} Добро пожаловать, {nickname}!\n\nВыберите действие:"
        await send_message_with_image(message.chat.id, menu_text, MENU_IMAGE_URL, get_main_keyboard(user_id))

        pending_lobby_id = pop_pending_lobby_join(user_id)
        if pending_lobby_id is not None:
            try:
                await join_lobby_from_link(message.chat.id, user_id, pending_lobby_id)
            except Exception as e:
                logger.error(f"Error joining lobby after registration: {e}")
    else:
        await message.answer(f"{pe('❌')} {message_text}", parse_mode='HTML')

@dp.callback_query(lambda c: c.data == "cancel_registration")
async def cancel_registration(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback_query.from_user.id
    pop_pending_lobby_join(user_id)
    first_name = callback_query.from_user.first_name

    await cleanup_user_messages(user_id)

    if db.is_user_registered(user_id):
        menu_text = f"{pe('👋')} Привет, {esc(first_name)}!\n\nВыберите действие:"
        await send_message_with_image(user_id, menu_text, MENU_IMAGE_URL, get_main_keyboard(user_id))
    else:
        await send_message_with_image(
            user_id,
            f"{pe('👋')} Привет, {esc(first_name)}!\n\nНажмите чтобы зарегистрироваться:",
            None,
            get_registration_keyboard()
        )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "profile")
async def show_profile(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id

    if not db.is_user_registered(user_id):
        await callback_query.answer("❌ Вы не зарегистрированы!", show_alert=True)
        return

    player_data = db.get_player_profile(user_id)
    if not player_data:
        await callback_query.answer("❌ Профиль не найден!", show_alert=True)
        return

    (
        user_id_db,
        username,
        nickname,
        game_id,
        registration_date,
        rating,
        matches_played,
        kills,
        deaths,
        _nick_ch,
        _gid_ch,
        premium_until,
    ) = player_data

    kd_ratio = kills / deaths if deaths > 0 else kills
    level = get_player_level(rating)

    username_display = display_nickname(user_id, nickname)
    prem_line = ""
    if premium_until and db.is_premium(user_id):
        days_left = db.get_premium_days_left(user_id)
        prem_line = f"\n{pe('⭐️')} Премиум: {days_left} дней\n"

    profile_text = (
        f"{pe('👤')} Ваш профиль | {username_display}\n\n"
        f"{pe('🆔')} <code>{game_id}</code>\n"
        f"{pe('📈')} Уровень: {level}\n"
        f"{pe('🏆')} Рейтинг: {rating}{prem_line}\n"
        f"{pe('📊')} Статистика:\n"
        f"• Убийств: {kills}\n"
        f"• Смертей: {deaths}\n"
        f"• K/D: {kd_ratio:.2f}\n"
        f"• Матчей сыграно: {matches_played}"
    )

    await cleanup_user_messages(user_id)
    await send_message_with_image(user_id, profile_text, PROFILE_IMAGE_URL, get_profile_keyboard())
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "lobby_history")
async def show_lobby_history(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id

    lobbies = db.get_player_lobby_history(user_id, offset=0, limit=5)
    total_lobbies = db.get_player_lobby_history_count(user_id)

    await cleanup_user_messages(user_id)

    if not lobbies:
        history_text = f"{pe('🎮')} История сыгранных лобби\n\n{pe('❌')} Вы еще не играли в заполненных лобби"
        await send_message_with_image(user_id, history_text, None, get_back_keyboard())
        await callback_query.answer()
        return

    limit = 5
    current_page = 1
    total_pages = (total_lobbies + limit - 1) // limit

    history_text = f"{pe('🎮')} Сыгранные лобби (страница {current_page}/{total_pages}):\n\n"

    for i, lobby in enumerate(lobbies, 1):
        (lobby_id, lobby_unique_id, mode, map_name, created_at,
         kills_added, deaths_added, rating_added, stats_date, has_stats) = lobby

        try:
            lobby_date = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y %H:%M")
        except ValueError:
            lobby_date = created_at

        history_text += f"{pe('🔹')}Лобби #{lobby_unique_id}\n"
        history_text += f"{pe('🗺')} Карта: {map_name}\n"
        history_text += f"{pe('🎮')} Режим: {mode}\n"
        history_text += f"{pe('📊')} Статистика:\n"

        if has_stats and kills_added > 0:
            history_text += f"Убийств - {kills_added}\n"
            history_text += f"Смертей - {deaths_added}\n"
            history_text += f"{pe('🏆')} Рейтинг: +{rating_added}\n"
        else:
            history_text += f"{pe('🔎')} На модерации\n"
            history_text += f"{pe('🏆')} Рейтинг: {pe('🕗')} В стадии обработки\n"

        history_text += f"{pe('🕗')} Дата: {lobby_date}\n"

        if i < len(lobbies):
            history_text += f"{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}\n\n"

    has_next = total_lobbies > 5

    await send_message_with_image(
        user_id,
        history_text,
        None,
        get_lobby_history_keyboard(user_id, current_offset=0, has_next=has_next, total_lobbies=total_lobbies)
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data.startswith("history_prev_"))
async def show_prev_lobby_history(callback_query: types.CallbackQuery):
    try:
        data_parts = callback_query.data.split("_")
        user_id = int(data_parts[2])
        offset = int(data_parts[3])

        if callback_query.from_user.id != user_id:
            await callback_query.answer("❌ Вы можете смотреть только свою историю", show_alert=True)
            return

        lobbies = db.get_player_lobby_history(user_id, offset=offset, limit=5)
        total_lobbies = db.get_player_lobby_history_count(user_id)

        if not lobbies:
            await callback_query.answer("❌ Больше лобби нет", show_alert=True)
            return

        limit = 5
        current_page = (offset // limit) + 1
        total_pages = (total_lobbies + limit - 1) // limit

        history_text = f"{pe('🎮')} Сыгранные лобби (страница {current_page}/{total_pages}):\n\n"

        for i, lobby in enumerate(lobbies, 1):
            (lobby_id, lobby_unique_id, mode, map_name, created_at,
             kills_added, deaths_added, rating_added, stats_date, has_stats) = lobby

            try:
                lobby_date = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y %H:%M")
            except ValueError:
                lobby_date = created_at

            history_text += f"{pe('🔹')}Лобби #{lobby_unique_id}\n"
            history_text += f"{pe('🗺')} Карта: {map_name}\n"
            history_text += f"{pe('🎮')} Режим: {mode}\n"
            history_text += f"{pe('📊')} Статистика:\n"

            if has_stats and kills_added > 0:
                history_text += f"Убийств - {kills_added}\n"
                history_text += f"Смертей - {deaths_added}\n"
                history_text += f"{pe('🏆')} Рейтинг: +{rating_added}\n"
            else:
                history_text += f"{pe('🔎')} На модерации\n"
                history_text += f"{pe('🏆')} Рейтинг: {pe('🕗')} В стадии обработки\n"

            history_text += f"{pe('🕗')} Дата: {lobby_date}\n"

            if i < len(lobbies):
                history_text += f"{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}\n\n"

        has_next = total_lobbies > offset + 5

        await cleanup_user_messages(user_id)
        await send_message_with_image(
            user_id,
            history_text,
            None,
            get_lobby_history_keyboard(user_id, current_offset=offset, has_next=has_next, total_lobbies=total_lobbies)
        )
    except (ValueError, IndexError) as e:
        logger.error(f"Error in history_prev: {e}")
        await callback_query.answer("❌ Ошибка при загрузке истории", show_alert=True)
    await callback_query.answer()

@dp.callback_query(lambda c: c.data.startswith("history_next_"))
async def show_next_lobby_history(callback_query: types.CallbackQuery):
    try:
        data_parts = callback_query.data.split("_")
        user_id = int(data_parts[2])
        offset = int(data_parts[3])

        if callback_query.from_user.id != user_id:
            await callback_query.answer("❌ Вы можете смотреть только свою историю", show_alert=True)
            return

        lobbies = db.get_player_lobby_history(user_id, offset=offset, limit=5)
        total_lobbies = db.get_player_lobby_history_count(user_id)

        if not lobbies:
            await callback_query.answer("❌ Больше лобби нет", show_alert=True)
            return

        limit = 5
        current_page = (offset // limit) + 1
        total_pages = (total_lobbies + limit - 1) // limit

        history_text = f"{pe('🎮')} Сыгранные лобби (страница {current_page}/{total_pages}):\n\n"

        for i, lobby in enumerate(lobbies, 1):
            (lobby_id, lobby_unique_id, mode, map_name, created_at,
             kills_added, deaths_added, rating_added, stats_date, has_stats) = lobby

            try:
                lobby_date = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y %H:%M")
            except ValueError:
                lobby_date = created_at

            history_text += f"{pe('🔹')}Лобби #{lobby_unique_id}\n"
            history_text += f"{pe('🗺')} Карта: {map_name}\n"
            history_text += f"{pe('🎮')} Режим: {mode}\n"
            history_text += f"{pe('📊')} Статистика:\n"

            if has_stats and kills_added > 0:
                history_text += f"Убийств - {kills_added}\n"
                history_text += f"Смертей - {deaths_added}\n"
                history_text += f"{pe('🏆')} Рейтинг: +{rating_added}\n"
            else:
                history_text += f"{pe('🔎')} На модерации\n"
                history_text += f"{pe('🏆')} Рейтинг: {pe('🕗')} В стадии обработки\n"

            history_text += f"{pe('🕗')} Дата: {lobby_date}\n"

            if i < len(lobbies):
                history_text += f"{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}{pe('➖')}\n\n"

        has_next = total_lobbies > offset + 5

        await cleanup_user_messages(user_id)
        await send_message_with_image(
            user_id,
            history_text,
            None,
            get_lobby_history_keyboard(user_id, current_offset=offset, has_next=has_next, total_lobbies=total_lobbies)
        )
    except (ValueError, IndexError) as e:
        logger.error(f"Error in history_next: {e}")
        await callback_query.answer("❌ Ошибка при загрузке истории", show_alert=True)
    await callback_query.answer()

def _history_filter_state(user_id, mode_idx=-1, map_idx=-1, days=0, offset=0, action="menu", arg=""):
    return f"hf|{user_id}|{mode_idx}|{map_idx}|{days}|{offset}|{action}|{arg}"


def _history_filter_label(mode, map_name, days):
    parts = []
    if mode:
        parts.append(f"режим: {mode}")
    if map_name:
        parts.append(f"карта: {map_name}")
    if days:
        parts.append(f"период: {days} дн.")
    return ", ".join(parts) if parts else "не выбраны"


def _history_filter_menu_keyboard(user_id, mode_idx, map_idx, days, offset):
    return InlineKeyboardMarkup(inline_keyboard=[
        [btn("🎮 Выбрать режим", callback_data=_history_filter_state(user_id, mode_idx, map_idx, days, offset, "mode"), icon="🎮")],
        [btn("🗺 Выбрать карту", callback_data=_history_filter_state(user_id, mode_idx, map_idx, days, offset, "map"), icon="🗺")],
        [btn("📅 Период", callback_data=_history_filter_state(user_id, mode_idx, map_idx, days, offset, "period"), icon="📅")],
        [btn("♻️ Сбросить фильтры", callback_data=_history_filter_state(user_id, -1, -1, 0, 0, "apply"), icon="♻️")],
        [btn("📋 Показать матчи", callback_data=_history_filter_state(user_id, mode_idx, map_idx, days, 0, "apply"), icon="📋")],
        [btn("◀️ Назад к истории", callback_data="lobby_history", icon="◀️")],
    ])


@dp.callback_query(lambda c: c.data.startswith("hf|"))
async def handle_lobby_history_filters(callback_query: types.CallbackQuery):
    try:
        parts = callback_query.data.split("|", 7)
        _, user_raw, mode_raw, map_raw, days_raw, offset_raw, action = parts[:7]
        arg = parts[7] if len(parts) > 7 else ""
        user_id = int(user_raw)
        mode_idx, map_idx, days, offset = int(mode_raw), int(map_raw), int(days_raw), int(offset_raw)
        if callback_query.from_user.id != user_id:
            await callback_query.answer("❌ Вы можете смотреть только свою историю", show_alert=True)
            return
        if not db.is_premium(user_id):
            await callback_query.answer("⭐️ Фильтры истории доступны с Premium", show_alert=True)
            return

        modes = db.get_player_lobby_filter_options(user_id, "mode")
        maps = db.get_player_lobby_filter_options(user_id, "map_name")
        mode = modes[mode_idx] if 0 <= mode_idx < len(modes) else None
        map_name = maps[map_idx] if 0 <= map_idx < len(maps) else None

        if action == "mode":
            rows = [[btn("Все режимы", callback_data=_history_filter_state(user_id, -1, map_idx, days, 0, "setmode"), icon="🎮")]]
            rows += [[btn(str(value)[:50], callback_data=_history_filter_state(user_id, mode_idx, map_idx, days, 0, "setmode", str(i)), icon="🎮")] for i, value in enumerate(modes)]
            rows.append([btn("◀️ Назад", callback_data=_history_filter_state(user_id, mode_idx, map_idx, days, offset, "menu"), icon="◀️")])
            await cleanup_user_messages(user_id)
            await send_message_with_image(user_id, f"{pe('🎮')} Выберите режим матча:", None, InlineKeyboardMarkup(inline_keyboard=rows))
        elif action == "map":
            rows = [[btn("Все карты", callback_data=_history_filter_state(user_id, mode_idx, -1, days, 0, "setmap"), icon="🗺")]]
            rows += [[btn(str(value)[:50], callback_data=_history_filter_state(user_id, mode_idx, map_idx, days, 0, "setmap", str(i)), icon="🗺")] for i, value in enumerate(maps)]
            rows.append([btn("◀️ Назад", callback_data=_history_filter_state(user_id, mode_idx, map_idx, days, offset, "menu"), icon="◀️")])
            await cleanup_user_messages(user_id)
            await send_message_with_image(user_id, f"{pe('🗺')} Выберите карту:", None, InlineKeyboardMarkup(inline_keyboard=rows))
        elif action == "period":
            rows = [
                [btn("За всё время", callback_data=_history_filter_state(user_id, mode_idx, map_idx, 0, 0, "setdays"), icon="📅")],
                [btn("За 7 дней", callback_data=_history_filter_state(user_id, mode_idx, map_idx, 7, 0, "setdays"), icon="📅")],
                [btn("За 30 дней", callback_data=_history_filter_state(user_id, mode_idx, map_idx, 30, 0, "setdays"), icon="📅")],
                [btn("За 90 дней", callback_data=_history_filter_state(user_id, mode_idx, map_idx, 90, 0, "setdays"), icon="📅")],
                [btn("◀️ Назад", callback_data=_history_filter_state(user_id, mode_idx, map_idx, days, offset, "menu"), icon="◀️")],
            ]
            await cleanup_user_messages(user_id)
            await send_message_with_image(user_id, f"{pe('📅')} Выберите период:", None, InlineKeyboardMarkup(inline_keyboard=rows))
        elif action == "setmode":
            mode_idx = int(arg) if arg else -1
            await cleanup_user_messages(user_id)
            await send_message_with_image(user_id, f"{pe('🔎')} Фильтры: {_history_filter_label(modes[mode_idx] if 0 <= mode_idx < len(modes) else None, map_name, days)}", None, _history_filter_menu_keyboard(user_id, mode_idx, map_idx, days, 0))
        elif action == "setmap":
            map_idx = int(arg) if arg else -1
            await cleanup_user_messages(user_id)
            await send_message_with_image(user_id, f"{pe('🔎')} Фильтры: {_history_filter_label(mode, maps[map_idx] if 0 <= map_idx < len(maps) else None, days)}", None, _history_filter_menu_keyboard(user_id, mode_idx, map_idx, days, 0))
        elif action == "setdays":
            days = days if days in (7, 30, 90) else 0
            await cleanup_user_messages(user_id)
            await send_message_with_image(user_id, f"{pe('🔎')} Фильтры: {_history_filter_label(mode, map_name, days)}", None, _history_filter_menu_keyboard(user_id, mode_idx, map_idx, days, 0))
        elif action == "menu":
            await cleanup_user_messages(user_id)
            await send_message_with_image(user_id, f"{pe('🔎')} Фильтры истории матчей\n\nТекущие фильтры: {_history_filter_label(mode, map_name, days)}", None, _history_filter_menu_keyboard(user_id, mode_idx, map_idx, days, offset))
        elif action == "page":
            offset = max(0, int(arg or 0))
            await _show_filtered_lobby_history(user_id, mode_idx, map_idx, days, offset, modes, maps)
        elif action == "apply":
            await _show_filtered_lobby_history(user_id, mode_idx, map_idx, days, 0, modes, maps)
        await callback_query.answer()
    except (ValueError, IndexError) as e:
        logger.error(f"Error in lobby history filters: {e}")
        await callback_query.answer("❌ Ошибка фильтра истории", show_alert=True)


async def _show_filtered_lobby_history(user_id, mode_idx, map_idx, days, offset, modes=None, maps=None):
    modes = modes if modes is not None else db.get_player_lobby_filter_options(user_id, "mode")
    maps = maps if maps is not None else db.get_player_lobby_filter_options(user_id, "map_name")
    mode = modes[mode_idx] if 0 <= mode_idx < len(modes) else None
    map_name = maps[map_idx] if 0 <= map_idx < len(maps) else None
    lobbies = db.get_filtered_player_lobby_history(user_id, mode, map_name, days, offset, 5)
    total = db.get_filtered_player_lobby_history_count(user_id, mode, map_name, days)
    if not lobbies:
        text = f"{pe('🔎')} По выбранным фильтрам матчей нет.\n\nФильтры: {_history_filter_label(mode, map_name, days)}"
    else:
        pages = max(1, (total + 4) // 5)
        text = f"{pe('🎮')} История матчей ({offset // 5 + 1}/{pages})\nФильтры: {_history_filter_label(mode, map_name, days)}\n\n"
        for i, lobby in enumerate(lobbies):
            (_, unique_id, lobby_mode, lobby_map, created_at, kills, deaths, rating, _, has_stats) = lobby
            try:
                lobby_date = datetime.strptime(str(created_at), "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y %H:%M")
            except (ValueError, TypeError):
                lobby_date = str(created_at)
            text += f"{pe('🔹')} Лобби #{unique_id}\n{pe('🗺')} Карта: {lobby_map}\n{pe('🎮')} Режим: {lobby_mode}\n"
            if has_stats and kills > 0:
                text += f"Убийств: {kills} | Смертей: {deaths}\n{pe('🏆')} Рейтинг: +{rating}\n"
            else:
                text += f"{pe('🔎')} Статистика на модерации\n"
            text += f"{pe('🕗')} Дата: {lobby_date}\n\n"
    rows = []
    if offset > 0 or offset + 5 < total:
        nav = []
        if offset > 0:
            nav.append(btn("⬅️", callback_data=_history_filter_state(user_id, mode_idx, map_idx, days, offset, "page", str(max(0, offset - 5))), icon="⬅️"))
        if offset + 5 < total:
            nav.append(btn("➡️", callback_data=_history_filter_state(user_id, mode_idx, map_idx, days, offset, "page", str(offset + 5)), icon="➡️"))
        rows.append(nav)
    rows.append([btn("🔎 Изменить фильтры", callback_data=_history_filter_state(user_id, mode_idx, map_idx, days, offset, "menu"), icon="🔎")])
    rows.append([btn("◀️ К истории", callback_data="lobby_history", icon="◀️")])
    await cleanup_user_messages(user_id)
    await send_message_with_image(user_id, text, None, InlineKeyboardMarkup(inline_keyboard=rows))


@dp.callback_query(lambda c: c.data in ("top", "top_all_time"))
async def show_all_time_top(callback_query: types.CallbackQuery):
    top_players = db.get_all_time_top_players()
    user_id = callback_query.from_user.id

    has_any_stats = db.get_player_has_any_stats(user_id)

    if not top_players:
        top_text = f"{pe('🏆')} Топ игроков:\n\n{pe('❌')} Пока нет данных"
        await cleanup_user_messages(user_id)
        await send_message_with_image(user_id, top_text, TOP_IMAGE_URL, get_top_keyboard())
        await callback_query.answer()
        return

    top_text = f"{pe('🏆')} Топ игроков за все время:\n\n"

    for i, player_data in enumerate(top_players):
        user_id_player, nickname, rating, matches_played, kills, deaths = player_data

        kd_ratio = kills / deaths if deaths > 0 else kills
        level = get_player_level(rating)

        nick_disp = display_nickname(user_id_player, nickname)
        top_text += f"{i+1}. {nick_disp} | {level}\n{pe('🏆')} {rating} | K/D: {kd_ratio:.2f} | {pe('🎮')} {matches_played}\n\n"

    if has_any_stats:
        player_position = db.get_player_all_time_position(user_id)
        if player_position > 0:
            top_text += f"{pe('🎯')} Ваша позиция: {player_position}"
        else:
            top_text += f"{pe('⚠️')} Вас нет в этом топе"
    else:
        top_text += f"{pe('⚠️')} Вас нет в этом топе"

    await cleanup_user_messages(user_id)
    await send_message_with_image(user_id, top_text, TOP_IMAGE_URL, get_top_keyboard())
    await callback_query.answer()

# ======================================================================
# PREMIUM: цены и скидки, промокоды, подарки, оплата (заказы хранятся в базе)
# ======================================================================
class PromoStates(StatesGroup):
    waiting_for_code = State()


class GiftStates(StatesGroup):
    waiting_for_recipient = State()


def compute_price(user_id, days):
    """Цена для конкретного покупателя с учётом скидки (первая покупка / активированный промокод).
    Скидки не складываются: берётся лучшая."""
    base_usd = Decimal(PREMIUM_PRICE_USD_30 if days == PREMIUM_30_DAYS else PREMIUM_PRICE_USD_90)
    base_stars = PREMIUM_STARS_30 if days == PREMIUM_30_DAYS else PREMIUM_STARS_90
    percent, reason, promo_code = 0, None, None
    if FIRST_PURCHASE_DISCOUNT_PERCENT > 0 and not db.has_paid_premium(user_id):
        percent, reason = FIRST_PURCHASE_DISCOUNT_PERCENT, "первая покупка"
    promo = db.get_best_pending_promo_discount(user_id)
    if promo and promo[1] > percent:
        promo_code, percent, reason = promo[0], promo[1], f"промокод {promo[0]}"
    factor = Decimal(100 - percent) / Decimal(100)
    usd = max(Decimal("0.01"), (base_usd * factor).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    stars = max(1, int((Decimal(base_stars) * factor).to_integral_value(rounding=ROUND_HALF_UP)))
    return {
        "usd": usd, "stars": stars, "base_usd": base_usd, "base_stars": base_stars,
        "percent": percent, "reason": reason, "promo_code": promo_code,
    }


def price_button_label(user_id, days):
    p = compute_price(user_id, days)
    label = f"${p['usd']}"
    if p["percent"]:
        label += f" (−{p['percent']}%)"
    return label


def player_nick(user_id):
    p = db.get_player_profile(user_id)
    return p[2] if p else str(user_id)


def premium_payment_text(uid, days, recipient_id=None):
    price = compute_price(uid, days)
    lines = [f"{pe('🛒')} Покупка: Premium Status ({days} дней)"]
    if recipient_id and recipient_id != uid:
        lines.append(f"🎁 Подарок для: {esc(player_nick(recipient_id))}")
    lines.append(f"{pe('💵')} Цена: ${price['usd']} / {price['stars']} Stars (звёзды в Telegram)")
    if price["percent"]:
        lines.append(
            f"🎁 Скидка {price['percent']}% ({esc(price['reason'])}). "
            f"Без скидки: ${price['base_usd']} / {price['base_stars']} Stars"
        )
    lines += ["", "Выберите способ оплаты:"]
    return "\n".join(lines)


async def send_premium_menu(uid):
    await cleanup_user_messages(uid)
    caption = f"{pe('🌟')} <b>Premium Status</b>\n\nНа сколько ты хочешь купить подписку?"
    p = compute_price(uid, PREMIUM_30_DAYS)
    if p["percent"]:
        caption += f"\n\n🎁 Ваша скидка: {p['percent']}% ({esc(p['reason'])})"
    await send_message_with_image(uid, caption, PREMIUM_IMAGE_URL, get_premium_period_keyboard(uid))


@dp.callback_query(lambda c: c.data == "premium_menu")
async def premium_menu(callback_query: types.CallbackQuery, state: FSMContext):
    uid = callback_query.from_user.id
    if not db.is_user_registered(uid):
        await callback_query.answer("Сначала зарегистрируйтесь в боте.", show_alert=True)
        return
    await state.clear()
    await send_premium_menu(uid)
    await callback_query.answer()


@dp.callback_query(lambda c: c.data in ("premium_pick_30", "premium_pick_90"))
async def premium_pick_period(callback_query: types.CallbackQuery):
    uid = callback_query.from_user.id
    if not db.is_user_registered(uid):
        await callback_query.answer("Сначала зарегистрируйтесь.", show_alert=True)
        return
    days = PREMIUM_30_DAYS if callback_query.data.endswith("30") else PREMIUM_90_DAYS
    await cleanup_user_messages(uid)
    await send_message_with_image(uid, premium_payment_text(uid, days), None, get_premium_payment_keyboard(days))
    await callback_query.answer()


@dp.callback_query(lambda c: c.data.startswith("premium_pick_back_"))
async def premium_pick_back(callback_query: types.CallbackQuery):
    uid = callback_query.from_user.id
    try:
        days = int(callback_query.data.replace("premium_pick_back_", ""))
    except ValueError:
        await callback_query.answer()
        return
    if days not in (PREMIUM_30_DAYS, PREMIUM_90_DAYS):
        await callback_query.answer()
        return
    await cleanup_user_messages(uid)
    await send_message_with_image(uid, premium_payment_text(uid, days), None, get_premium_payment_keyboard(days))
    await callback_query.answer()


# ---------- оплата: общие функции ----------
async def start_crypto_payment(callback_query, days, recipient_id=None):
    uid = callback_query.from_user.id
    recipient_id = recipient_id or uid
    if not db.is_user_registered(uid):
        await callback_query.answer("Сначала зарегистрируйтесь.", show_alert=True)
        return
    if days not in (PREMIUM_30_DAYS, PREMIUM_90_DAYS):
        await callback_query.answer()
        return
    if recipient_id != uid and not db.is_user_registered(recipient_id):
        await callback_query.answer("Получатель не найден.", show_alert=True)
        return
    if not CRYPTOBOT_TOKEN:
        await callback_query.answer(
            "Оплата через CryptoBot не настроена (CRYPTOBOT_TOKEN в .env).",
            show_alert=True,
        )
        return
    price = compute_price(uid, days)
    order_id = db.create_premium_order(
        "cryptobot", uid, recipient_id, days, price["usd"], price["stars"], price["percent"], price["promo_code"]
    )
    if not order_id:
        await callback_query.answer("Не удалось создать заказ, попробуйте позже.", show_alert=True)
        return
    body = {
        "currency_type": "fiat",
        "fiat": "USD",
        "amount": str(price["usd"]),
        "description": f"Premium Status {days} дней",
        "expires_in": 600,
    }
    result, err = await asyncio.to_thread(cryptobot_api_call, "createInvoice", body)
    if err or not result:
        await callback_query.answer(f"CryptoBot: {err or 'ошибка'}", show_alert=True)
        return
    pay_url = result.get("pay_url") or result.get("bot_invoice_url")
    invoice_id = result.get("invoice_id")
    if not pay_url or invoice_id is None:
        await callback_query.answer("Не удалось создать счёт CryptoBot.", show_alert=True)
        return
    db.set_order_invoice(order_id, invoice_id)
    back_cb = f"premium_pick_back_{days}" if recipient_id == uid else f"gift_pick_{recipient_id}_{days}"
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("💲 Оплатить в CryptoBot", url=pay_url, icon="💲")],
            [btn("🔍 Проверить оплату", callback_data=f"pcheck_{order_id}", icon="🔍")],
            [btn("◀️ Назад", callback_data=back_cb, icon="◀️")],
        ]
    )
    lines = [f"{pe('🌟')} Оплата Premium Status ({days} д.)"]
    if recipient_id != uid:
        lines.append(f"🎁 Подарок для: {esc(player_nick(recipient_id))}")
    lines.append(f"{pe('💲')} Сумма: ${price['usd']}")
    lines += ["", f"{pe('⚠️')} У вас 10 минут на оплату."]
    await cleanup_user_messages(uid)
    await bot.send_message(chat_id=uid, text="\n".join(lines), reply_markup=kb, parse_mode='HTML')
    await callback_query.answer()


async def start_stars_payment(callback_query, days, recipient_id=None):
    uid = callback_query.from_user.id
    recipient_id = recipient_id or uid
    if not db.is_user_registered(uid):
        await callback_query.answer("Сначала зарегистрируйтесь.", show_alert=True)
        return
    if days not in (PREMIUM_30_DAYS, PREMIUM_90_DAYS):
        await callback_query.answer()
        return
    if recipient_id != uid and not db.is_user_registered(recipient_id):
        await callback_query.answer("Получатель не найден.", show_alert=True)
        return
    price = compute_price(uid, days)
    order_id = db.create_premium_order(
        "telegram_stars", uid, recipient_id, days, price["usd"], price["stars"], price["percent"], price["promo_code"]
    )
    if not order_id:
        await callback_query.answer("Не удалось создать заказ, попробуйте позже.", show_alert=True)
        return
    description = (
        f"Подписка Premium на {days} дней"
        if recipient_id == uid
        else f"Подарок: Premium на {days} дней для {player_nick(recipient_id)}"
    )
    try:
        await bot.send_invoice(
            chat_id=uid,
            title="Premium Status",
            description=description[:250],
            payload=f"order_{order_id}",
            currency="XTR",
            prices=[LabeledPrice(label=f"Premium {days} дней", amount=price["stars"])],
            provider_token="",
        )
    except Exception as e:
        logger.error(f"send_invoice stars: {e}")
        await callback_query.answer("Не удалось выставить счёт Stars.", show_alert=True)
        return
    await callback_query.answer()


async def finalize_premium_order(order, provider_ref):
    """Выдаёт Premium получателю по оплаченному заказу. Идемпотентно по provider_ref."""
    (order_id, provider, _, _, buyer_id, recipient_id, days, _, _, _, promo_code, _) = order
    granted = db.try_register_premium_payment(recipient_id, days, provider, provider_ref)
    if not granted:
        return False
    db.mark_order_paid(order_id, provider_ref)
    if promo_code:
        db.mark_promo_applied(buyer_id, promo_code, order_id)
    try:
        chat = await bot.get_chat(buyer_id)
        un = chat.username or ""
        line_user = f"{buyer_id} | @{un}" if un else f"{buyer_id} |"
        if recipient_id == buyer_id:
            log_text = (
                f"{line_user}\n"
                f"Купил премиум на {days} дней!\n"
                f"Не забудьте выдать покупателю доступ в приватный чат и префикс в чате!\n"
                f"@uglymatthey"
            )
        else:
            log_text = (
                f"{line_user}\n"
                f"Подарил премиум на {days} дней игроку {player_nick(recipient_id)} ({recipient_id})!\n"
                f"Не забудьте выдать получателю доступ в приватный чат и префикс в чате!\n"
                f"@uglymatthey"
            )
        if PREMIUM_LOG_CHAT_ID:
            await bot.send_message(chat_id=int(PREMIUM_LOG_CHAT_ID), text=log_text)
    except Exception as e:
        logger.error(f"finalize_premium_order log chat: {e}")
    if recipient_id != buyer_id:
        try:
            await bot.send_message(
                chat_id=recipient_id,
                text=(
                    f"{pe('🎁')} Игрок {esc(player_nick(buyer_id))} подарил вам Premium на {days} дней!\n"
                    f"{pe('⭐️')} Приятной игры!"
                ),
                parse_mode='HTML',
            )
        except Exception as e:
            logger.info(f"Cannot notify gift recipient {recipient_id}: {e}")
    return True


async def after_order_paid(chat_id, order, first_name):
    buyer_id, recipient_id, days = order[4], order[5], order[6]
    if recipient_id == buyer_id:
        await show_post_purchase_main_menu(chat_id, buyer_id, first_name, days)
    else:
        await cleanup_user_messages(buyer_id)
        await send_message_with_image(
            chat_id,
            f"{pe('🎁')} Подарок отправлен! Игрок {esc(player_nick(recipient_id))} получил Premium на {days} дней.",
            MENU_IMAGE_URL,
            get_main_keyboard(buyer_id),
        )


@dp.callback_query(lambda c: c.data.startswith("premium_pay_crypto_"))
async def premium_pay_crypto(callback_query: types.CallbackQuery):
    try:
        days = int(callback_query.data.replace("premium_pay_crypto_", ""))
    except ValueError:
        await callback_query.answer()
        return
    await start_crypto_payment(callback_query, days)


@dp.callback_query(lambda c: c.data.startswith("premium_pay_stars_"))
async def premium_pay_stars(callback_query: types.CallbackQuery):
    try:
        days = int(callback_query.data.replace("premium_pay_stars_", ""))
    except ValueError:
        await callback_query.answer()
        return
    await start_stars_payment(callback_query, days)


@dp.callback_query(lambda c: c.data.startswith("pcheck_"))
async def premium_order_check(callback_query: types.CallbackQuery):
    try:
        order_id = int(callback_query.data.replace("pcheck_", ""))
    except ValueError:
        await callback_query.answer()
        return
    uid = callback_query.from_user.id
    order = db.get_premium_order(order_id)
    if not order or order[4] != uid:
        await callback_query.answer("Это не ваш счёт.", show_alert=True)
        return
    _, provider, _, invoice_id, _, _, days, price_usd, _, _, _, status = order
    if status == "paid":
        await callback_query.answer("Платёж уже был учтён.", show_alert=True)
        return
    if provider != "cryptobot" or not invoice_id:
        await callback_query.answer("Счёт не найден. Создайте новый из меню Premium.", show_alert=True)
        return
    inv, err = await asyncio.to_thread(cryptobot_get_invoice, invoice_id)
    if err or not inv:
        await callback_query.answer(f"Ошибка: {err or 'счёт не найден'}", show_alert=True)
        return
    inv_status = inv.get("status")
    if inv_status == "expired":
        await callback_query.answer("Счёт истёк. Создайте новый из меню Premium.", show_alert=True)
        return
    if inv_status != "paid":
        await callback_query.answer("Оплата ещё не поступила.", show_alert=True)
        return
    try:
        amount_ok = Decimal(str(inv.get("amount"))).quantize(Decimal("0.01")) == Decimal(str(price_usd)).quantize(Decimal("0.01"))
        if inv.get("currency_type") == "fiat" and inv.get("fiat") != "USD":
            amount_ok = False
    except Exception:
        amount_ok = False
    if not amount_ok:
        logger.warning(f"CryptoBot invoice {invoice_id}: amount mismatch: {inv.get('amount')} {inv.get('fiat')} vs {price_usd}")
        await callback_query.answer("Сумма счёта не совпадает с заказом.", show_alert=True)
        return
    ok = await finalize_premium_order(order, f"cryptobot_{invoice_id}")
    if ok:
        try:
            await callback_query.message.delete()
        except Exception:
            pass
        await after_order_paid(callback_query.message.chat.id, order, callback_query.from_user.first_name)
        await callback_query.answer()
    else:
        await callback_query.answer("Платёж уже был учтён.", show_alert=True)


@dp.callback_query(lambda c: c.data.startswith("premium_cbcheck:"))
async def premium_cryptobot_check(callback_query: types.CallbackQuery):
    parts = callback_query.data.split(":")
    if len(parts) not in (3, 4):
        await callback_query.answer()
        return
    try:
        invoice_id = int(parts[1])
        owner_id = int(parts[2])
        days = int(parts[3]) if len(parts) == 4 else None
    except ValueError:
        await callback_query.answer()
        return
    if callback_query.from_user.id != owner_id:
        await callback_query.answer("Это не ваш счёт.", show_alert=True)
        return
    if days is None:
        pend = pending_cryptobot_invoices.get(owner_id)
        if not pend or int(pend["invoice_id"]) != invoice_id:
            await callback_query.answer("Создайте новый счёт из меню Premium.", show_alert=True)
            return
        days = pend["days"]
    if days not in (PREMIUM_30_DAYS, PREMIUM_90_DAYS):
        await callback_query.answer()
        return
    inv, err = await asyncio.to_thread(cryptobot_get_invoice, invoice_id)
    if err or not inv:
        await callback_query.answer(f"Ошибка: {err or 'счёт не найден'}", show_alert=True)
        return
    if inv.get("status") != "paid":
        await callback_query.answer("Оплата ещё не поступила.", show_alert=True)
        return
    expected_usd = PREMIUM_PRICE_USD_30 if days == PREMIUM_30_DAYS else PREMIUM_PRICE_USD_90
    try:
        amount_ok = abs(float(inv.get("amount")) - float(expected_usd)) < 0.001
        if inv.get("currency_type") == "fiat" and inv.get("fiat") != "USD":
            amount_ok = False
    except (TypeError, ValueError):
        amount_ok = False
    if not amount_ok:
        logger.warning(f"CryptoBot invoice {invoice_id}: amount mismatch for {days} days: {inv.get('amount')} {inv.get('fiat')}")
        await callback_query.answer("Сумма счёта не совпадает с выбранным тарифом.", show_alert=True)
        return
    ref = f"cryptobot_{invoice_id}"
    ok = await finalize_premium_purchase(owner_id, days, "cryptobot", ref)
    if ok:
        pending_cryptobot_invoices.pop(owner_id, None)
        first_name = callback_query.from_user.first_name
        try:
            await callback_query.message.delete()
        except Exception:
            pass
        await show_post_purchase_main_menu(
            callback_query.message.chat.id, owner_id, first_name, days
        )
        await callback_query.answer()
    else:
        await callback_query.answer("Платёж уже был учтён.", show_alert=True)


@dp.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: types.PreCheckoutQuery):
    payload = pre_checkout_query.invoice_payload or ""
    if payload.startswith("order_"):
        ok = False
        try:
            order = db.get_premium_order(int(payload.replace("order_", "")))
            ok = bool(
                order
                and order[11] == "pending"
                and order[1] == "telegram_stars"
                and order[4] == pre_checkout_query.from_user.id
                and order[8] == pre_checkout_query.total_amount
            )
        except Exception as e:
            logger.error(f"pre_checkout order: {e}")
        if ok:
            await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)
        else:
            await bot.answer_pre_checkout_query(
                pre_checkout_query.id, ok=False, error_message="Заказ не найден или цена изменилась. Создайте счёт заново."
            )
    elif payload.startswith("premium_stars_"):
        # счета, выставленные до обновления
        await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)
    else:
        await bot.answer_pre_checkout_query(
            pre_checkout_query.id,
            ok=False,
            error_message="Неизвестный товар",
        )


async def log_stars_failure(uid, days, charge_id):
    try:
        if PREMIUM_LOG_CHAT_ID:
            await bot.send_message(
                chat_id=int(PREMIUM_LOG_CHAT_ID),
                text=(
                    "⚠️ Платёж Stars не активирован автоматически.\n"
                    f"user_id: {uid}\n"
                    f"Дней: {days}\n"
                    f"charge_id: {charge_id}\n"
                    "Возможно, это повтор уже учтённого платежа — проверьте вручную."
                ),
            )
    except Exception as e:
        logger.error(f"stars failure log: {e}")


@dp.message(F.successful_payment)
async def on_successful_payment(message: types.Message):
    sp = message.successful_payment
    if not sp:
        return
    payload = sp.invoice_payload or ""
    uid = message.from_user.id

    if payload.startswith("order_"):
        try:
            order = db.get_premium_order(int(payload.replace("order_", "")))
        except ValueError:
            order = None
        if not order:
            await log_stars_failure(uid, "?", sp.telegram_payment_charge_id)
            await message.answer("Не удалось найти заказ. Напишите администратору, мы всё проверим.")
            return
        ok = await finalize_premium_order(order, f"stars_{sp.telegram_payment_charge_id}")
        if not ok:
            await log_stars_failure(uid, order[6], sp.telegram_payment_charge_id)
            await message.answer(
                "Не удалось активировать Premium (возможно, платёж уже учтён). "
                "Если Premium не появился, напишите администратору."
            )
            return
        try:
            await message.delete()
        except Exception:
            pass
        await after_order_paid(message.chat.id, order, message.from_user.first_name)
        return

    # счета Stars, выставленные до обновления
    if not payload.startswith("premium_stars_"):
        return
    try:
        days = int(payload.replace("premium_stars_", ""))
    except ValueError:
        return
    if days not in (PREMIUM_30_DAYS, PREMIUM_90_DAYS):
        return
    ref = f"stars_{sp.telegram_payment_charge_id}"
    ok = await finalize_premium_purchase(uid, days, "telegram_stars", ref)
    if not ok:
        await log_stars_failure(uid, days, sp.telegram_payment_charge_id)
        await message.answer(
            "Не удалось активировать Premium (возможно, платёж уже учтён). "
            "Если Premium не появился, напишите администратору."
        )
        return
    try:
        await message.delete()
    except Exception:
        pass
    await show_post_purchase_main_menu(
        message.chat.id, uid, message.from_user.first_name, days
    )


# ---------- промокоды (сторона игрока) ----------
def promo_back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[btn("◀️ Назад", callback_data="premium_menu", icon="◀️")]])


@dp.callback_query(lambda c: c.data == "promo_enter")
async def promo_enter(callback_query: types.CallbackQuery, state: FSMContext):
    uid = callback_query.from_user.id
    if not db.is_user_registered(uid):
        await callback_query.answer("Сначала зарегистрируйтесь.", show_alert=True)
        return
    await state.set_state(PromoStates.waiting_for_code)
    await cleanup_user_messages(uid)
    await send_message_with_image(uid, f"{pe('🎟')} Введите промокод:", None, promo_back_keyboard())
    await callback_query.answer()


@dp.message(PromoStates.waiting_for_code)
async def promo_code_entered(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    code = (message.text or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9_-]{3,20}", code):
        await message.answer("❌ Промокод состоит из букв, цифр, «-» и «_» (3-20 символов). Попробуйте ещё раз.", reply_markup=promo_back_keyboard())
        return
    kind, value, err = db.redeem_promo(uid, code)
    if err:
        await message.answer(f"❌ {err}", reply_markup=promo_back_keyboard())
        return
    await state.clear()
    if kind == "days":
        new_until = db.grant_premium_days(
            uid, value, provider="promo", ref=f"promo_{code}_{uid}", bonus_rating=PROMO_PREMIUM_BONUS_RATING
        )
        if not new_until:
            db.cancel_promo_redemption(uid, code)
            await message.answer("❌ Не удалось начислить Premium, попробуйте ещё раз позже.")
            return
        await message.answer(
            f"🎉 Промокод активирован! Вам начислено {value} дн. Premium (до {fmt_dt(new_until)}) "
            f"и +{PROMO_PREMIUM_BONUS_RATING} рейтинга."
        )
        try:
            chat = await bot.get_chat(uid)
            un = chat.username or ""
            line_user = f"{uid} | @{un}" if un else f"{uid} |"
            if PREMIUM_LOG_CHAT_ID:
                await bot.send_message(
                    chat_id=int(PREMIUM_LOG_CHAT_ID),
                    text=(
                        f"{line_user}\n"
                        f"Активировал промокод {code}: Premium на {value} дней!\n"
                        f"Не забудьте выдать доступ в приватный чат и префикс в чате!\n"
                        f"@uglymatthey"
                    ),
                )
        except Exception as e:
            logger.error(f"promo premium log chat: {e}")
    else:
        await message.answer(f"✅ Промокод активирован: скидка {value}% на следующую покупку Premium.")
    await send_premium_menu(uid)


# ---------- подарок Premium другому игроку ----------
@dp.callback_query(lambda c: c.data == "gift_start")
async def gift_start(callback_query: types.CallbackQuery, state: FSMContext):
    uid = callback_query.from_user.id
    if not db.is_user_registered(uid):
        await callback_query.answer("Сначала зарегистрируйтесь.", show_alert=True)
        return
    await state.set_state(GiftStates.waiting_for_recipient)
    await cleanup_user_messages(uid)
    await send_message_with_image(
        uid,
        f"{pe('🎁')} Кому подарить Premium?\n\n"
        "Отправьте ник, @username или Telegram ID игрока. Он должен быть зарегистрирован в боте.",
        None,
        promo_back_keyboard(),
    )
    await callback_query.answer()


async def show_gift_periods(uid, recipient_id):
    await cleanup_user_messages(uid)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [btn(f"🗓 30 days — {price_button_label(uid, PREMIUM_30_DAYS)}", callback_data=f"gift_pick_{recipient_id}_30", icon="🗓")],
        [btn(f"📆 90 days — {price_button_label(uid, PREMIUM_90_DAYS)}", callback_data=f"gift_pick_{recipient_id}_90", icon="📆")],
        [btn("◀️ Назад", callback_data="premium_menu", icon="◀️")],
    ])
    await send_message_with_image(
        uid,
        f"{pe('🎁')} Подарок для: {esc(player_nick(recipient_id))}\n\nНа сколько подарить Premium?",
        None,
        kb,
    )


@dp.message(GiftStates.waiting_for_recipient)
async def gift_recipient_entered(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    target = db.find_player_id(message.text or "")
    if not target:
        await message.answer("❌ Игрок не найден. Проверьте ник, @username или Telegram ID.", reply_markup=promo_back_keyboard())
        return
    if target == uid:
        await message.answer("Для себя Premium покупается в обычном меню. Отправьте ник друга.", reply_markup=promo_back_keyboard())
        return
    await state.clear()
    await show_gift_periods(uid, target)


@dp.callback_query(lambda c: c.data.startswith("gift_to_"))
async def gift_to(callback_query: types.CallbackQuery):
    uid = callback_query.from_user.id
    try:
        target = int(callback_query.data.replace("gift_to_", ""))
    except ValueError:
        await callback_query.answer()
        return
    if not db.is_user_registered(uid) or not db.is_user_registered(target) or target == uid:
        await callback_query.answer("Получатель недоступен.", show_alert=True)
        return
    await show_gift_periods(uid, target)
    await callback_query.answer()


@dp.callback_query(lambda c: c.data.startswith("gift_pick_"))
async def gift_pick(callback_query: types.CallbackQuery):
    uid = callback_query.from_user.id
    try:
        _, _, target_s, days_s = callback_query.data.split("_")
        target, days = int(target_s), int(days_s)
    except ValueError:
        await callback_query.answer()
        return
    if days not in (PREMIUM_30_DAYS, PREMIUM_90_DAYS) or not db.is_user_registered(uid) \
            or not db.is_user_registered(target) or target == uid:
        await callback_query.answer("Получатель недоступен.", show_alert=True)
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            btn("💲 CryptoBot", callback_data=f"gift_pay_crypto_{target}_{days}", icon="💲"),
            btn("⭐️ Telegram Stars", callback_data=f"gift_pay_stars_{target}_{days}", icon="⭐️"),
        ],
        [btn("◀️ Назад", callback_data=f"gift_to_{target}", icon="◀️")],
    ])
    await cleanup_user_messages(uid)
    await send_message_with_image(uid, premium_payment_text(uid, days, target), None, kb)
    await callback_query.answer()


@dp.callback_query(lambda c: c.data.startswith("gift_pay_crypto_"))
async def gift_pay_crypto(callback_query: types.CallbackQuery):
    try:
        _, _, _, target_s, days_s = callback_query.data.split("_")
        target, days = int(target_s), int(days_s)
    except ValueError:
        await callback_query.answer()
        return
    if target == callback_query.from_user.id:
        await callback_query.answer()
        return
    await start_crypto_payment(callback_query, days, target)


@dp.callback_query(lambda c: c.data.startswith("gift_pay_stars_"))
async def gift_pay_stars(callback_query: types.CallbackQuery):
    try:
        _, _, _, target_s, days_s = callback_query.data.split("_")
        target, days = int(target_s), int(days_s)
    except ValueError:
        await callback_query.answer()
        return
    if target == callback_query.from_user.id:
        await callback_query.answer()
        return
    await start_stars_payment(callback_query, days, target)


@dp.callback_query(lambda c: c.data == "edit_profile")
async def edit_profile_menu(callback_query: types.CallbackQuery):
    uid = callback_query.from_user.id
    if not db.is_user_registered(uid):
        await callback_query.answer("❌ Вы не зарегистрированы!", show_alert=True)
        return
    await cleanup_user_messages(uid)
    await send_message_with_image(
        uid,
        f"{pe('✏️')} Редактирование профиля\n\nВыберите действие:",
        None,
        get_edit_profile_keyboard(),
    )
    await callback_query.answer()


@dp.callback_query(lambda c: c.data == "edit_profile_nickname")
async def edit_profile_nickname_start(callback_query: types.CallbackQuery, state: FSMContext):
    uid = callback_query.from_user.id
    if not db.is_user_registered(uid):
        await callback_query.answer("❌ Вы не зарегистрированы!", show_alert=True)
        return
    rem = await asyncio.to_thread(db.profile_nickname_cooldown_remaining, uid)
    if rem > 0:
        h, m = rem // 3600, (rem % 3600) // 60
        await callback_query.answer(
            f"Смена никнейма доступна через {h}ч {m}м", show_alert=True
        )
        return
    await state.set_state(ProfileEditStates.waiting_for_new_nickname)
    await cleanup_user_messages(uid)
    await send_message_with_image(
        uid,
        f"{pe('✏️')} Введите новый никнейм (как при регистрации):",
        None,
        get_cancel_edit_profile_keyboard(),
    )
    await callback_query.answer()


@dp.callback_query(lambda c: c.data == "edit_profile_game_id")
async def edit_profile_game_id_start(callback_query: types.CallbackQuery, state: FSMContext):
    uid = callback_query.from_user.id
    if not db.is_user_registered(uid):
        await callback_query.answer("❌ Вы не зарегистрированы!", show_alert=True)
        return
    rem = await asyncio.to_thread(db.profile_game_id_cooldown_remaining, uid)
    if rem > 0:
        h, m = rem // 3600, (rem % 3600) // 60
        await callback_query.answer(
            f"Смена игрового ID доступна через {h}ч {m}м", show_alert=True
        )
        return
    await state.set_state(ProfileEditStates.waiting_for_new_game_id)
    await cleanup_user_messages(uid)
    await send_message_with_image(
        uid,
        f"{pe('🆔')} Введите новый игровой ID (2–13 символов: только цифры и латинские буквы):",
        None,
        get_cancel_edit_profile_keyboard(),
    )
    await callback_query.answer()


@dp.callback_query(lambda c: c.data == "cancel_edit_profile")
async def cancel_edit_profile(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    uid = callback_query.from_user.id
    await cleanup_user_messages(uid)
    await send_message_with_image(
        uid,
        f"{pe('✏️')} Редактирование профиля\n\nВыберите действие:",
        None,
        get_edit_profile_keyboard(),
    )
    await callback_query.answer()


@dp.message(ProfileEditStates.waiting_for_new_nickname)
async def process_profile_new_nickname(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    nickname = (message.text or "").strip()
    if len(nickname) < 3 or len(nickname) > 16:
        await message.answer(f"{pe('❌')} Никнейм должен быть от 3 до 16 символов", parse_mode='HTML')
        return
    if not re.match(r"^[a-zA-Z0-9_]+$", nickname):
        await message.answer(
            f"{pe('❌')} Никнейм должен содержать только английские буквы, цифры и подчёркивания",
            parse_mode='HTML'
        )
        return
    ok, err = await asyncio.to_thread(db.update_player_nickname_if_allowed, uid, nickname)
    if not ok:
        await message.answer(f"{pe('❌')} {err}", parse_mode='HTML')
        return
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    await cleanup_user_messages(uid)
    await message.answer(f"{pe('✅')} Никнейм обновлён!", parse_mode='HTML')
    await show_profile_from_message(message)


@dp.message(ProfileEditStates.waiting_for_new_game_id)
async def process_profile_new_game_id(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    game_id = (message.text or "").strip()
    if not is_valid_game_id(game_id):
        await message.answer(
            f"{pe('❌')} Игровой ID: от 2 до 13 символов, только цифры и латинские буквы",
            parse_mode='HTML'
        )
        return
    ok, err = await asyncio.to_thread(db.update_player_game_id_if_allowed, uid, game_id)
    if not ok:
        await message.answer(f"{pe('❌')} {err}", parse_mode='HTML')
        return
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    await cleanup_user_messages(uid)
    await message.answer(f"{pe('✅')} Игровой ID обновлён!", parse_mode='HTML')
    await show_profile_from_message(message)


@dp.callback_query(lambda c: c.data == "back_to_main")
async def back_to_main(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback_query.from_user.id
    first_name = callback_query.from_user.first_name

    await cleanup_user_messages(user_id)
    await cleanup_lobby_messages(user_id)

    menu_text = f"{pe('👋')} Привет, {esc(first_name)}!\n\nВыберите действие:"
    await send_message_with_image(user_id, menu_text, MENU_IMAGE_URL, get_main_keyboard(user_id))
    await callback_query.answer()

# ======================================================================
# ПАНЕЛЬ ВЛАДЕЛЬЦА (/kp) — скрытая команда, отвечает только владельцу
# ======================================================================
class OwnerStates(StatesGroup):
    waiting_for_search = State()
    waiting_for_rating = State()
    waiting_for_admin = State()
    waiting_for_promo = State()


def strip_tags(text):
    """Убирает HTML-теги (в т.ч. <tg-emoji>), оставляя обычный эмодзи-запасной вариант."""
    return html.unescape(re.sub(r"<[^>]+>", "", str(text)))


def fmt_dt(value):
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y %H:%M")
    except (TypeError, ValueError):
        return value or "—"


def player_label(user_id):
    p = db.get_player_profile(user_id)
    nick = p[2] if p else "?"
    return f"{nick} ({user_id})"


def owner_main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="own_stats")],
        [InlineKeyboardButton(text="🔍 Найти игрока", callback_data="own_search")],
        [InlineKeyboardButton(text="👮 Админы", callback_data="own_admins")],
        [InlineKeyboardButton(text="🎟 Промокоды", callback_data="own_promos")],
        [InlineKeyboardButton(text="❌ Закрыть", callback_data="own_close")],
    ])


def owner_card_keyboard(uid):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✏️ Рейтинг", callback_data=f"own_rate_{uid}"),
            InlineKeyboardButton(text="🔄 Обновить", callback_data=f"own_card_{uid}"),
        ],
        [
            InlineKeyboardButton(text="⭐ +30 дн.", callback_data=f"own_prem_{uid}_30"),
            InlineKeyboardButton(text="⭐ +90 дн.", callback_data=f"own_prem_{uid}_90"),
            InlineKeyboardButton(text="🗑 Снять", callback_data=f"own_unprem_{uid}"),
        ],
        [InlineKeyboardButton(text="◀️ В панель", callback_data="own_menu")],
    ])


def build_player_card(user_id):
    p = db.get_player_profile(user_id)
    if not p:
        return None
    _, username, nickname, game_id, reg_date, rating, matches, kills, deaths, _, _, premium_until = p
    rating, matches, kills, deaths = rating or 0, matches or 0, kills or 0, deaths or 0
    kd = kills / deaths if deaths > 0 else kills
    days_left = db.get_premium_days_left(user_id)
    premium_text = f"до {fmt_dt(premium_until)} ({days_left} дн.)" if days_left > 0 else "нет"
    role = " (админ)" if is_admin(user_id) else ""
    return (
        f"👤 {nickname}{role}\n"
        f"Telegram ID: {user_id}\n"
        f"Игровой ID: {game_id}\n"
        f"Username: {username or '—'}\n"
        f"Регистрация: {fmt_dt(reg_date)}\n\n"
        f"🏆 Рейтинг: {rating} {strip_tags(get_player_level(rating))}\n"
        f"🎮 Матчей: {matches}\n"
        f"🎯 Убийств/смертей: {kills}/{deaths} (K/D {kd:.2f})\n\n"
        f"⭐️ Premium: {premium_text}"
    )


async def owner_guard(callback_query):
    if not is_owner(callback_query.from_user.id):
        await callback_query.answer("Только для владельца.", show_alert=True)
        return False
    return True


async def owner_edit(callback_query, text, keyboard):
    try:
        await callback_query.message.edit_text(text, reply_markup=keyboard)
    except TelegramBadRequest as e:
        if "not modified" not in str(e):
            await callback_query.message.answer(text, reply_markup=keyboard)


@dp.message(Command("kp"))
async def cmd_panel(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return  # команда скрытая: остальным бот не отвечает
    if message.chat.type != "private":
        await message.answer("Панель владельца доступна только в личных сообщениях с ботом.")
        return
    await state.clear()
    await message.answer("🛠 Панель владельца", reply_markup=owner_main_keyboard())


@dp.callback_query(lambda c: c.data == "own_menu")
async def own_menu(callback_query: types.CallbackQuery, state: FSMContext):
    if not await owner_guard(callback_query):
        return
    await state.clear()
    await owner_edit(callback_query, "🛠 Панель владельца", owner_main_keyboard())
    await callback_query.answer()


@dp.callback_query(lambda c: c.data == "own_close")
async def own_close(callback_query: types.CallbackQuery, state: FSMContext):
    if not await owner_guard(callback_query):
        return
    await state.clear()
    try:
        await callback_query.message.delete()
    except Exception:
        pass
    await callback_query.answer()


@dp.callback_query(lambda c: c.data == "own_stats")
async def own_stats(callback_query: types.CallbackQuery):
    if not await owner_guard(callback_query):
        return
    s = db.get_admin_stats()
    if not s:
        await callback_query.answer("Ошибка при получении статистики.", show_alert=True)
        return
    text = (
        "📊 Статистика бота\n\n"
        f"👥 Игроков: {s['players_total']}\n"
        f"   • за 24 часа: +{s['players_24h']}\n"
        f"   • за 7 дней: +{s['players_7d']}\n\n"
        f"🎮 Лобби всего: {s['lobbies_total']}\n"
        f"   • сегодня создано: {s['lobbies_today']}\n"
        f"   • набираются сейчас: {s['lobbies_active']}\n"
        f"   • сыграно: {s['lobbies_completed']}\n"
        f"   • начислений статистики: {s['stats_entries']}\n\n"
        f"⭐️ Premium активен у: {s['premium_active']}\n"
        f"   • покупок за 30 дней: {s['premium_buys_30d']}\n"
        f"   • покупок всего: {s['premium_buys_total']}"
    )
    await owner_edit(callback_query, text, InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="own_stats")],
        [InlineKeyboardButton(text="◀️ В панель", callback_data="own_menu")],
    ]))
    await callback_query.answer()


@dp.callback_query(lambda c: c.data == "own_search")
async def own_search(callback_query: types.CallbackQuery, state: FSMContext):
    if not await owner_guard(callback_query):
        return
    await state.set_state(OwnerStates.waiting_for_search)
    await owner_edit(
        callback_query,
        "🔍 Отправьте ник, Telegram ID, игровой ID или @username игрока.",
        InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Отмена", callback_data="own_menu")]]),
    )
    await callback_query.answer()


@dp.message(OwnerStates.waiting_for_search)
async def own_search_query(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    target = db.find_player_id(message.text or "")
    if not target:
        await message.answer("❌ Игрок не найден. Пришлите другой запрос или откройте /kp.")
        return
    await state.clear()
    await message.answer(build_player_card(target), reply_markup=owner_card_keyboard(target))


@dp.callback_query(lambda c: c.data.startswith("own_card_"))
async def own_card(callback_query: types.CallbackQuery, state: FSMContext):
    if not await owner_guard(callback_query):
        return
    await state.clear()
    try:
        target = int(callback_query.data.replace("own_card_", ""))
    except ValueError:
        await callback_query.answer()
        return
    card = build_player_card(target)
    if not card:
        await callback_query.answer("Игрок не найден.", show_alert=True)
        return
    await owner_edit(callback_query, card, owner_card_keyboard(target))
    await callback_query.answer()


@dp.callback_query(lambda c: c.data.startswith("own_prem_"))
async def own_grant_premium(callback_query: types.CallbackQuery):
    if not await owner_guard(callback_query):
        return
    try:
        _, _, uid_s, days_s = callback_query.data.split("_")
        target, days = int(uid_s), int(days_s)
    except ValueError:
        await callback_query.answer()
        return
    if days not in (30, 90):
        await callback_query.answer()
        return
    new_until = db.grant_premium_days(target, days)
    if not new_until:
        await callback_query.answer("Не удалось выдать Premium.", show_alert=True)
        return
    db.log_admin_action(callback_query.from_user.id, "grant_premium", target, f"+{days} дн. до {new_until}")
    try:
        await bot.send_message(chat_id=target, text=f"⭐️ Вам выдан Premium на {days} дн.")
    except Exception:
        pass
    await owner_edit(callback_query, build_player_card(target), owner_card_keyboard(target))
    await callback_query.answer(f"Выдано {days} дн.")


@dp.callback_query(lambda c: c.data.startswith("own_unprem_"))
async def own_remove_premium(callback_query: types.CallbackQuery):
    if not await owner_guard(callback_query):
        return
    try:
        target = int(callback_query.data.replace("own_unprem_", ""))
    except ValueError:
        await callback_query.answer()
        return
    if db.remove_premium(target):
        db.log_admin_action(callback_query.from_user.id, "remove_premium", target, None)
    await owner_edit(callback_query, build_player_card(target), owner_card_keyboard(target))
    await callback_query.answer("Premium снят")


@dp.callback_query(lambda c: c.data.startswith("own_rate_"))
async def own_rate_start(callback_query: types.CallbackQuery, state: FSMContext):
    if not await owner_guard(callback_query):
        return
    try:
        target = int(callback_query.data.replace("own_rate_", ""))
    except ValueError:
        await callback_query.answer()
        return
    p = db.get_player_profile(target)
    if not p:
        await callback_query.answer("Игрок не найден.", show_alert=True)
        return
    await state.set_state(OwnerStates.waiting_for_rating)
    await state.update_data(rating_target=target)
    await owner_edit(
        callback_query,
        f"✏️ Рейтинг игрока {p[2]}: {p[5] or 0}\n\n"
        "Отправьте новое значение (например 500) или изменение со знаком (+50 / -30).",
        InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Отмена", callback_data=f"own_card_{target}")]]),
    )
    await callback_query.answer()


@dp.message(OwnerStates.waiting_for_rating)
async def own_rate_value(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    data = await state.get_data()
    target = data.get("rating_target")
    raw = (message.text or "").strip().replace(" ", "")
    if not target or not re.fullmatch(r"[+-]?\d{1,6}", raw):
        await message.answer("❌ Нужно число, например 500, +50 или -30.")
        return
    p = db.get_player_profile(target)
    if not p:
        await state.clear()
        await message.answer("❌ Игрок не найден.")
        return
    old = p[5] or 0
    new = old + int(raw) if raw[0] in "+-" else int(raw)
    new = max(0, min(new, 100000))
    if not db.set_player_rating(target, new):
        await message.answer("❌ Не удалось изменить рейтинг.")
        return
    db.log_admin_action(message.from_user.id, "set_rating", target, f"{old} -> {new}")
    await state.clear()
    await message.answer(f"✅ Рейтинг изменён: {old} → {new}")
    await message.answer(build_player_card(target), reply_markup=owner_card_keyboard(target))


# ---------- управление админами ----------
@dp.callback_query(lambda c: c.data == "own_admins")
async def own_admins(callback_query: types.CallbackQuery, state: FSMContext = None):
    if not await owner_guard(callback_query):
        return
    if state is not None:
        await state.clear()
    lines = ["👮 Администраторы бота\n"]
    rows = []
    for uid in ADMINS:
        p = db.get_player_profile(uid)
        nick = p[2] if p else "не зарегистрирован"
        if is_owner(uid):
            tag = " — владелец"
        elif uid in FILE_ADMINS:
            tag = " — из admins.txt"
        else:
            tag = ""
        lines.append(f"• {nick} ({uid}){tag}")
        if not is_owner(uid) and uid not in FILE_ADMINS:
            rows.append([InlineKeyboardButton(text=f"❌ Убрать: {nick}", callback_data=f"own_admin_del_{uid}")])
    lines.append("\nАдмины из admins.txt убираются только правкой файла.")
    rows.append([InlineKeyboardButton(text="➕ Добавить админа", callback_data="own_admin_add")])
    rows.append([InlineKeyboardButton(text="◀️ В панель", callback_data="own_menu")])
    await owner_edit(callback_query, "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows))
    await callback_query.answer()


@dp.callback_query(lambda c: c.data == "own_admin_add")
async def own_admin_add(callback_query: types.CallbackQuery, state: FSMContext):
    if not await owner_guard(callback_query):
        return
    await state.set_state(OwnerStates.waiting_for_admin)
    await owner_edit(
        callback_query,
        "➕ Отправьте Telegram ID нового админа (число), либо ник или @username игрока из бота.",
        InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Отмена", callback_data="own_admins")]]),
    )
    await callback_query.answer()


@dp.message(OwnerStates.waiting_for_admin)
async def own_admin_add_value(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    raw = (message.text or "").strip()
    if raw.isdigit():
        target = int(raw)   # число считаем Telegram ID
    else:
        target = db.find_player_id(raw)
    if not target:
        await message.answer("❌ Не нашла такого игрока. Пришлите Telegram ID (число) или ник игрока.")
        return
    if target in ADMINS:
        await message.answer("ℹ️ Этот пользователь уже администратор.")
        return
    if not db.add_bot_admin(target, message.from_user.id):
        await message.answer("❌ Не удалось сохранить администратора.")
        return
    ADMINS.append(target)
    db.log_admin_action(message.from_user.id, "add_admin", target, None)
    await state.clear()
    try:
        await bot.send_message(chat_id=target, text="👮 Вам выданы права администратора бота.")
    except Exception:
        pass
    try:
        await set_bot_commands_for_user(target)
    except Exception as e:
        logger.warning(f"Cannot refresh commands for new admin {target}: {e}")
    await message.answer(
        f"✅ Админ добавлен: {player_label(target)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👮 К списку админов", callback_data="own_admins")]]),
    )


@dp.callback_query(lambda c: c.data.startswith("own_admin_del_"))
async def own_admin_del(callback_query: types.CallbackQuery, state: FSMContext):
    if not await owner_guard(callback_query):
        return
    try:
        target = int(callback_query.data.replace("own_admin_del_", ""))
    except ValueError:
        await callback_query.answer()
        return
    if is_owner(target) or target in FILE_ADMINS:
        await callback_query.answer("Этого админа нельзя убрать из панели.", show_alert=True)
        return
    if db.remove_bot_admin(target):
        if target in ADMINS:
            ADMINS.remove(target)
        db.log_admin_action(callback_query.from_user.id, "remove_admin", target, None)
        try:
            await bot.send_message(chat_id=target, text="ℹ️ Права администратора бота сняты.")
        except Exception:
            pass
        try:
            await set_bot_commands_for_user(target)
        except Exception as e:
            logger.warning(f"Cannot refresh commands for {target}: {e}")
    await own_admins(callback_query, state)


# ======================================================================
# ИТОГИ МАТЧА В КАНАЛ
# ======================================================================
async def maybe_post_match_result(lobby_id, force=False):
    """Публикует итоги матча в канал, когда статистика начислена всем игрокам лобби.
    force=True — опубликовать с теми, кому статистика уже начислена. Возвращает (опубликовано, причина_отказа)."""
    if not CHANNEL_ID:
        return False, "CHANNEL_ID не задан"
    results = db.get_lobby_match_results(lobby_id)
    if not results:
        return False, "По этому лобби ещё нет начисленной статистики"
    players = db.get_lobby_players(lobby_id)
    if not force and len(results) < len(players):
        return False, "Статистика начислена ещё не всем игрокам"
    lobby_info = db.get_lobby_by_id(lobby_id)
    if not lobby_info:
        return False, "Лобби не найдено"
    if not db.mark_match_posted(lobby_id):
        return False, "Результаты этого матча уже публиковались"

    medals = ["🥇", "🥈", "🥉"]
    lines = [
        f"🏁 Матч #{lobby_info[1]} завершён",
        f"🎲 Режим: {lobby_info[4]}",
        f"🗺 Карта: {lobby_info[5]}",
        "",
    ]
    for i, (user_id, nickname, kills, deaths, rating_added) in enumerate(results[:10]):
        kd = kills / deaths if deaths > 0 else kills
        place = medals[i] if i < len(medals) else f"{i + 1}."
        lines.append(f"{place} {strip_tags(nickname)} — {kills}/{deaths} (K/D {kd:.2f}) · +{rating_added}")
    lines += ["", f"👥 Игроков: {len(results)}"]

    keyboard = None
    link = await make_bot_link("join_lobby")
    if link:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎮 Играть в боте", url=link)]
        ])
    try:
        await bot.send_message(chat_id=CHANNEL_ID, text="\n".join(lines), reply_markup=keyboard)
    except Exception as e:
        db.unmark_match_posted(lobby_id)
        logger.error(f"Error posting match result for lobby {lobby_id}: {e}")
        return False, f"Не удалось отправить в канал: {e}"
    return True, None


@dp.message(Command("postmatch"))
async def cmd_postmatch(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Нет прав для этой команды.")
        return
    lobby_id = db.get_lobby_id_by_topic_thread_id(message.message_thread_id) if message.message_thread_id else None
    if not lobby_id:
        await message.answer("❌ Эта команда работает только в теме лобби!")
        return
    ok, reason = await maybe_post_match_result(lobby_id, force=True)
    await message.answer("📣 Результаты матча опубликованы в канале." if ok else f"❌ {reason}")


# ---------- промокоды (панель владельца) ----------
PROMO_KIND_ALIASES = {
    "days": "days", "day": "days", "дни": "days", "дней": "days", "дн": "days",
    "percent": "percent", "процент": "percent", "процентов": "percent", "%": "percent",
}


def generate_promo_code():
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(random.choice(alphabet) for _ in range(8))


def parse_promo_input(text):
    """КОД ТИП ЗНАЧЕНИЕ [ЛИМИТ] [ДНЕЙ_ДЕЙСТВИЯ] -> ((code, kind, value, max_uses, valid_days), None) или (None, ошибка)."""
    parts = (text or "").split()
    if len(parts) < 3:
        return None, "Нужно минимум три слова: КОД ТИП ЗНАЧЕНИЕ"
    code = parts[0].upper()
    if code in ("AUTO", "АВТО"):
        code = generate_promo_code()
    if not re.fullmatch(r"[A-Z0-9_-]{3,20}", code):
        return None, "Код: 3-20 символов, только латинские буквы, цифры, «-» и «_»."
    kind = PROMO_KIND_ALIASES.get(parts[1].lower())
    if not kind:
        return None, "Тип: days (дни Premium) или percent (скидка в %)."
    try:
        value = int(parts[2])
        max_uses = int(parts[3]) if len(parts) > 3 else 1
        valid_days = int(parts[4]) if len(parts) > 4 else None
    except ValueError:
        return None, "Значение, лимит и срок должны быть числами."
    if kind == "days" and not 1 <= value <= 365:
        return None, "Дней Premium должно быть от 1 до 365."
    if kind == "percent" and not 1 <= value <= 90:
        return None, "Скидка должна быть от 1 до 90 процентов."
    if not 0 <= max_uses <= 100000:
        return None, "Лимит использований: от 0 (без лимита) до 100000."
    if valid_days is not None and not 1 <= valid_days <= 3650:
        return None, "Срок действия в днях: от 1 до 3650."
    return (code, kind, value, max_uses, valid_days), None


def format_promo_line(code, kind, value, max_uses, uses, expires_at, active):
    what = f"+{value} дн. Premium" if kind == "days" else f"скидка {value}%"
    limit = f"{uses}/{max_uses}" if max_uses else f"{uses}/∞"
    until = f", до {fmt_dt(expires_at)}" if expires_at else ""
    return f"{'🟢' if active else '🔴'} {code} — {what} — {limit}{until}"


@dp.callback_query(lambda c: c.data == "own_promos")
async def own_promos(callback_query: types.CallbackQuery, state: FSMContext = None):
    if not await owner_guard(callback_query):
        return
    if state is not None:
        await state.clear()
    promos = db.list_promos(15)
    lines = ["🎟 Промокоды (последние 15)\n"]
    rows = []
    if not promos:
        lines.append("Пока нет ни одного промокода.")
    for code, kind, value, max_uses, uses, expires_at, active in promos:
        lines.append(format_promo_line(code, kind, value, max_uses, uses, expires_at, active))
        rows.append([InlineKeyboardButton(
            text=f"{'⏸ Выключить' if active else '▶️ Включить'} {code}",
            callback_data=f"own_promo_tgl_{code}",
        )])
    rows.append([InlineKeyboardButton(text="➕ Создать промокод", callback_data="own_promo_new")])
    rows.append([InlineKeyboardButton(text="◀️ В панель", callback_data="own_menu")])
    await owner_edit(callback_query, "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows))
    await callback_query.answer()


@dp.callback_query(lambda c: c.data == "own_promo_new")
async def own_promo_new(callback_query: types.CallbackQuery, state: FSMContext):
    if not await owner_guard(callback_query):
        return
    await state.set_state(OwnerStates.waiting_for_promo)
    await owner_edit(
        callback_query,
        "➕ Новый промокод. Отправьте одной строкой:\n\n"
        "КОД ТИП ЗНАЧЕНИЕ [ЛИМИТ] [ДНЕЙ_ДЕЙСТВИЯ]\n\n"
        "Примеры:\n"
        "SUMMER days 7 50 30 — 7 дней Premium, 50 использований, действует 30 дней\n"
        "SALE percent 20 100 — скидка 20% на покупку, 100 использований\n"
        "auto days 3 — случайный код, 3 дня Premium, одно использование\n\n"
        "Лимит по умолчанию 1, 0 = без лимита. Скидки не суммируются с «первой покупкой»: берётся лучшая.",
        InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Отмена", callback_data="own_promos")]]),
    )
    await callback_query.answer()


@dp.message(OwnerStates.waiting_for_promo)
async def own_promo_value(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return
    parsed, err = parse_promo_input(message.text or "")
    if err:
        await message.answer(f"❌ {err}")
        return
    code, kind, value, max_uses, valid_days = parsed
    ok, create_err = db.create_promo(code, kind, value, max_uses, valid_days, message.from_user.id)
    if not ok:
        await message.answer(f"❌ {create_err}")
        return
    db.log_admin_action(message.from_user.id, "create_promo", None, f"{code} {kind} {value} limit={max_uses}")
    await state.clear()
    await message.answer(
        f"✅ Промокод создан: {code}\n"
        f"{'+' + str(value) + ' дн. Premium' if kind == 'days' else 'Скидка ' + str(value) + '%'}, "
        f"лимит: {max_uses or 'без лимита'}"
        f"{', действует ' + str(valid_days) + ' дн.' if valid_days else ''}\n\n"
        "Игроки вводят его в меню Premium → «Промокод».",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎟 К списку промокодов", callback_data="own_promos")]]),
    )


@dp.callback_query(lambda c: c.data.startswith("own_promo_tgl_"))
async def own_promo_toggle(callback_query: types.CallbackQuery, state: FSMContext):
    if not await owner_guard(callback_query):
        return
    code = callback_query.data.replace("own_promo_tgl_", "")
    new_state = db.toggle_promo(code)
    if new_state is None:
        await callback_query.answer("Промокод не найден.", show_alert=True)
        return
    db.log_admin_action(callback_query.from_user.id, "toggle_promo", None, f"{code} -> {'on' if new_state else 'off'}")
    await own_promos(callback_query, state)


async def cleanup_old_messages():
    try:
        lobby_size = len(user_lobby_messages.cache)
        menu_size = len(user_menu_messages.cache)
        if lobby_size > 900 or menu_size > 900:
            logger.info(f"Cache sizes - Lobby: {lobby_size}, Menu: {menu_size}")
    except Exception as e:
        logger.error(f"Error in cleanup_old_messages: {e}")

async def expire_empty_lobbies_once():
    """Удаляет лобби, в которое за LOBBY_EMPTY_TIMEOUT_MIN минут никто не вступил (кроме создателя)."""
    expired = db.get_expired_empty_lobbies(LOBBY_EMPTY_TIMEOUT_MIN)
    for lobby_id, lobby_unique_id, creator_id in expired:
        try:
            if len(db.get_lobby_players(lobby_id)) > 1:
                continue  # кто-то успел вступить

            await delete_lobby_channel_message(lobby_id)
            success, _, _ = db.delete_lobby(lobby_id)
            if not success:
                continue
            logger.info(f"Lobby {lobby_id} (#{lobby_unique_id}) deleted: nobody joined in {LOBBY_EMPTY_TIMEOUT_MIN} min")

            try:
                await cleanup_lobby_messages(creator_id)
                await cleanup_user_messages(creator_id)
                await send_message_with_image(
                    creator_id,
                    f"{pe('⏰')} Лобби #{lobby_unique_id} удалено: за {LOBBY_EMPTY_TIMEOUT_MIN} минут к нему никто не присоединился.",
                    None,
                    get_main_keyboard(creator_id)
                )
            except Exception as e:
                logger.warning(f"Cannot notify creator {creator_id} about expired lobby: {e}")
        except Exception as e:
            logger.error(f"Error expiring lobby {lobby_id}: {e}")

async def expire_empty_lobbies_loop():
    while True:
        try:
            await expire_empty_lobbies_once()
        except Exception as e:
            logger.error(f"Error in expire_empty_lobbies_loop: {e}")
        await asyncio.sleep(60)

async def main():
    commands = [
        types.BotCommand(command="start", description="🎮 Главное меню"),
        types.BotCommand(command="profile", description="👤 Мой профиль"),
    ]

    try:
        await bot.set_my_commands(commands)
        logger.info("Bot commands set successfully")
    except Exception as e:
        logger.error(f"Error setting bot commands: {e}")

    print("Бот запущен...")
    logger.info("Bot starting...")

    expiry_task = asyncio.create_task(expire_empty_lobbies_loop())

    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        expiry_task.cancel()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
