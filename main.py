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
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv
from database import Database

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
MODERATOR_GROUP_ID = os.getenv('MODERATOR_GROUP_ID')
CHANNEL_ID = os.getenv('CHANNEL_ID')
MENU_IMAGE_URL = os.getenv('MENU_IMAGE_URL')
PROFILE_IMAGE_URL = os.getenv('PROFILE_IMAGE_URL')
TOP_IMAGE_URL = os.getenv('TOP_IMAGE_URL')
SCREENSHOT_IMAGE_URL = os.getenv('SCREENSHOT_IMAGE_URL')
LOBBY_IMAGE_URL = os.getenv('LOBBY_IMAGE_URL', 'https://example.com/lobby.jpg')
PREMIUM_IMAGE_URL = os.getenv('PREMIUM_IMAGE_URL')
PREMIUM_LOG_CHAT_ID = os.getenv('PREMIUM_LOG_CHAT_ID') or MODERATOR_GROUP_ID
# Токен и API должны совпадать: testnet → токен из @CryptoTestnetBot + testnet-pay.crypt.bot;
# продакшен → токен из @CryptoBot + pay.crypt.bot (CRYPTOBOT_USE_TESTNET=false).
CRYPTOBOT_TOKEN = os.getenv("CRYPTOBOT_TOKEN", "").strip().strip('"').strip("'")
CRYPTOBOT_USE_TESTNET = os.getenv('CRYPTOBOT_USE_TESTNET', 'true').lower() in ('1', 'true', 'yes')

PREMIUM_30_DAYS = 30
PREMIUM_90_DAYS = 90
PREMIUM_PRICE_USD_30 = '1.32'
PREMIUM_PRICE_USD_90 = '2.64'
PREMIUM_STARS_30 = 70
PREMIUM_STARS_90 = 140

# Ожидающие счета CryptoBot: user_id -> {"invoice_id": int, "days": int}
pending_cryptobot_invoices = {}

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в .env файле")

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
db = Database()

# Используем LRU-кэш для управления памятью
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
    'Pistol DM': {'key': 'pistol_dm', 'weapons': 'G22, USP, P350, TEC-9, F/S, Desert Eagle'},
    'Rifle DM': {'key': 'rifle_dm', 'weapons': 'FN FAL, FAMAS, M4, AKR, AKR12, M16'},
    'Sniper DM': {'key': 'sniper_dm', 'weapons': 'M40, M110, AWM'},
    'SMGs DM': {'key': 'smgs_dm', 'weapons': 'UMP45, MP5, MP7, P90'},
    'Allguns DM': {'key': 'allguns dm', 'weapons': 'ДОСТУПНЫ ВСЕ ОРУЖИЯ'}
}

MAPS = ['🏜 SandStone', '🏘 Province', '🧱 Rust', '🏭 Zone 9']
TIMES = ['10 минут']
DAMAGE_TYPES = ['по всему телу']
REGIONS = ['Россия']

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
            return level
    return "1️⃣"

def get_registration_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📝 Зарегистрироваться", callback_data="register")
    ]])

def get_main_keyboard(user_id=None):
    keyboard = [
        [InlineKeyboardButton(text="👤 Мой профиль", callback_data="profile")],
        [
            InlineKeyboardButton(text="⭐️ Premium", callback_data="premium_menu"),
            InlineKeyboardButton(text="🏆 Топ игроков", callback_data="top"),
        ],
        [InlineKeyboardButton(text="🎮 Активные лобби", callback_data="active_lobbies")],
    ]
    
    if user_id and is_admin(user_id):
        keyboard.append([InlineKeyboardButton(text="➕ Создать лобби", callback_data="create_lobby")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_main")
    ]])

def get_cancel_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_registration")
    ]])

def get_screenshot_cancel_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_screenshot")
    ]])

def get_top_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏆 Еженедельный топ", callback_data="top_weekly")],
        [InlineKeyboardButton(text="⭐ Постоянный топ", callback_data="top_all_time")],
        [InlineKeyboardButton(text="◀️ Назад в меню", callback_data="back_to_main")]
    ])

def get_profile_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🕗 Сыгранные лобби", callback_data="lobby_history")],
        [InlineKeyboardButton(text="✏️ Редактировать профиль", callback_data="edit_profile")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_main")],
    ])

def get_edit_profile_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить никнейм", callback_data="edit_profile_nickname")],
        [InlineKeyboardButton(text="🆔 Изменить игровое ID", callback_data="edit_profile_game_id")],
        [InlineKeyboardButton(text="◀️ К профилю", callback_data="profile")],
    ])

def get_premium_period_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗓 30 days — $1.32", callback_data="premium_pick_30")],
        [InlineKeyboardButton(text="📆 90 days — $2.64", callback_data="premium_pick_90")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_main")],
    ])

def get_premium_payment_keyboard(days: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💲 CryptoBot", callback_data=f"premium_pay_crypto_{days}"),
            InlineKeyboardButton(text="⭐️ Telegram Stars", callback_data=f"premium_pay_stars_{days}"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="premium_menu")],
    ])

def get_cancel_edit_profile_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit_profile")
    ]])

def get_lobby_history_keyboard(user_id, current_offset=0, has_next=False, total_lobbies=0):
    keyboard = []
    
    limit = 5
    current_page = (current_offset // limit) + 1
    total_pages = (total_lobbies + limit - 1) // limit
    
    nav_buttons = []
    
    if current_offset > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"history_prev_{user_id}_{current_offset - limit}"))
    
    nav_buttons.append(InlineKeyboardButton(text="◀️ К профилю", callback_data="profile"))
    
    if has_next:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"history_next_{user_id}_{current_offset + limit}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def is_valid_game_id(game_id):
    """Игровой ID: 2–13 символов, только цифры и латинские буквы."""
    return Database.is_valid_game_id_format(game_id)

def display_nickname(user_id, nickname):
    """Никнейм с отметками админа (✅) и Premium (⭐️)."""
    name = nickname or ""
    parts = [name]
    if is_admin(user_id):
        parts.append("✅")
    if db.is_premium(user_id):
        parts.append("⭐️")
    if len(parts) == 1:
        return name
    return " ".join(parts)

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
        f"\U0001f44b Привет, {first_name}!\n\nВыберите действие:"
    )


async def show_post_purchase_main_menu(chat_id: int, user_id: int, first_name: str, days: int):
    """Главное меню с картинкой после успешной оплаты Premium."""
    caption = post_purchase_menu_caption(first_name or "друг", days)
    await cleanup_user_messages(user_id)
    await send_message_with_image(chat_id, caption, MENU_IMAGE_URL, get_main_keyboard(user_id))


def _cryptobot_flatten_params(params):
    """Crypto Pay ожидает плоские query-параметры (как в официальных клиентах), не JSON POST."""
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
    """Синхронный вызов Crypto Pay API: GET + query (как go-cryptopay), не POST JSON."""
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
            # Python-urllib по умолчанию часто режется Cloudflare (403 / 1010)
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

def cryptobot_invoice_is_paid(invoice_id: int):
    """Проверка статуса счёта CryptoBot (True если оплачен)."""
    # В API параметр invoice_ids — строка id через запятую
    result, err = cryptobot_api_call(
        "getInvoices", {"invoice_ids": str(invoice_id), "count": 100}
    )
    if err:
        return False, err
    items = []
    if isinstance(result, dict):
        items = result.get("items") or result.get("invoices") or []
    elif isinstance(result, list):
        items = result
    for inv in items:
        iid = inv.get("invoice_id")
        if iid is not None and int(iid) == int(invoice_id):
            return inv.get("status") == "paid", None
    return False, "Счёт не найден в ответе API"

async def finalize_premium_purchase(user_id: int, days: int, provider: str, provider_ref: str):
    """Выдача премиума после оплаты и лог в чат модерации."""
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
            f"@bosin1337, @blesswayknow"
        )
        if PREMIUM_LOG_CHAT_ID:
            await bot.send_message(chat_id=int(PREMIUM_LOG_CHAT_ID), text=log_text)
    except Exception as e:
        logger.error(f"finalize_premium log chat: {e}")
    return True

def get_lobby_actions_keyboard(lobby_id, user_id, is_creator=False, players_count=0, max_players=10, lobby_full=False):
    buttons = []
    
    if lobby_full:
        buttons.append([InlineKeyboardButton(text="📸 Отправить скриншот", callback_data=f"send_screenshot_{lobby_id}")])
    else:
        if is_creator:
            buttons.append([InlineKeyboardButton(text="🗑 Удалить лобби", callback_data=f"delete_lobby_{lobby_id}")])
        else:
            user_in_lobby = db.is_user_in_lobby(user_id, lobby_id)
            if user_in_lobby:
                buttons.append([InlineKeyboardButton(text="🚪 Выйти из лобби", callback_data=f"leave_lobby_{lobby_id}")])
            else:
                buttons.append([InlineKeyboardButton(text="✅ Присоединиться", callback_data=f"join_lobby_{lobby_id}")])
        
        buttons.append([InlineKeyboardButton(text="🔄 Обновить", callback_data=f"view_lobby_{lobby_id}")])
    
    if not lobby_full:
        buttons.append([InlineKeyboardButton(text="◀️ Назад к списку", callback_data="active_lobbies")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_lobbies_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔄 Обновить", callback_data="active_lobbies"),
        InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_main")
    ]])

def get_lobby_list_keyboard(lobbies):
    keyboard = []
    for lobby in lobbies:
        lobby_id = lobby[0]
        lobby_unique_id = lobby[1]
        player_count = lobby[15]
        max_players = lobby[9]
        
        keyboard.append([
            InlineKeyboardButton(
                text=f"Лобби #{lobby_unique_id} {player_count}/{max_players}",
                callback_data=f"view_lobby_{lobby_id}"
            )
        ])
    keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_mode_keyboard():
    keyboard = [[InlineKeyboardButton(text=mode, callback_data=f"mode_{MODES[mode]['key']}")] for mode in MODES]
    keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data="create_lobby")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_map_keyboard():
    keyboard = []
    for i in range(0, len(MAPS), 2):
        row = [InlineKeyboardButton(text=MAPS[i], callback_data=f"map_{i}")]
        if i + 1 < len(MAPS):
            row.append(InlineKeyboardButton(text=MAPS[i + 1], callback_data=f"map_{i + 1}"))
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data="create_lobby")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_time_keyboard():
    keyboard = [[InlineKeyboardButton(text=time, callback_data=f"time_{i}")] for i, time in enumerate(TIMES)]
    keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data="create_lobby")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_damage_keyboard():
    keyboard = [[InlineKeyboardButton(text=damage_type, callback_data=f"damage_{i}")] for i, damage_type in enumerate(DAMAGE_TYPES)]
    keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data="create_lobby")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_region_keyboard():
    keyboard = [[InlineKeyboardButton(text=region, callback_data=f"region_{i}")] for i, region in enumerate(REGIONS)]
    keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data="create_lobby")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def is_admin(user_id):
    return user_id in ADMINS

async def safe_delete_message(chat_id, message_id):
    """Безопасное удаление сообщения с обработкой ошибок"""
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
                
                # Показываем alert сообщение
                try:
                    await bot.send_message(
                        chat_id=player_id,
                        text=f"❌ Лобби #{lobby_unique_id} было удалено хостером!"
                    )
                except Exception as e:
                    logger.warning(f"Error sending alert to {player_id}: {e}")
                
                message_text = f"❌ Лобби #{lobby_unique_id} было удалено хостером!\n\nВозвращаем вас к списку активных лобби..."
                
                lobbies = db.get_active_lobbies()
                
                if not lobbies:
                    await send_message_with_image(player_id, f"{message_text}\n\n❌ Нет активных лобби", None, get_lobbies_keyboard())
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
            f"📥 Новое лобби #{lobby_unique_id}\n\n"
            f"🗺 Карта: {map_name}\n"
            f"🎮 Режим: {mode}\n"
            f"👤 Хостер: {creator_tg_name}\n\n"
            f"Игроки:\n"
        )
        
        for player_id, player_nickname in players:
            player_data = db.get_player_profile(player_id)
            if player_data:
                rating = player_data[5]
                level = get_player_level(rating)
                topic_text += f"{level} {display_nickname(player_id, player_nickname)}\n<code>{player_id}</code>\n"
        
        topic_text += "⏳ Ожидаем скриншоты с результатами матча....."
        
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
                f"✅ Ваш скриншот для лобби #{lobby_unique_id} успешно обработан!",
                "",
                "📊 Начисленная статистика:",
                f"• Убийств: +{kills}",
                f"• Смертей: +{deaths}",
                f"🏆 Рейтинг: +{rating_added}",
            ]
            base_r = kills + 1
            if db.is_premium(user_id) and rating_added != base_r:
                lines.append(f"⭐️ Premium: к +{base_r} применён множитель ×1.5")
            lines.extend(["", "По вопросам к @bosin1337"])
            notification_text = "\n".join(lines)
            await bot.send_message(chat_id=user_id, text=notification_text)
    except Exception as e:
        logger.error(f"Error notifying player {user_id}: {e}")

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
            f"Лобби #{lobby_unique_id} активно❗️\n\n"
            f"🎮 Режим: {mode} от {creator_tg_name}\n"
            f"Оружия: {mode_weapons}\n"
            f"Карта: {map_name}\n"
            f"⏳ Время: {time_limit}\n"
            f"🎯 Урон: {damage_type}\n"
            f"🌍 Регион: {region}"
        )
        
        join_keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🎮 Присоединиться к лобби", url="https://t.me/KingDM_robot?start=join_lobby")]
            ]
        )
        
        message = await bot.send_message(
            chat_id=CHANNEL_ID,
            text=message_text,
            reply_markup=join_keyboard
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
        return "❌ Информация о лобби не найдена", False
    
    try:
        lobby_id, lobby_unique_id, creator_id, lobby_link, mode, map_name, time_limit, damage_type, region, max_players, current_players, _, _, creator_name, creator_username, _ = lobby_info[:16]
        
        mode_weapons = MODES.get(mode, {}).get('weapons', 'Доступны все оружия!')
        
        creator_display = creator_first_name if creator_first_name else "Неизвестно"
        if is_admin(creator_id):
            creator_display = f"{creator_display} ✅"
        
        # Получаем игровой ID создателя
        creator_game_id = db.get_player_game_id(creator_id) or "Неизвестно"
        
        players_count = len(players)
        lobby_full = players_count >= max_players
        
        lobby_text = (
            f"✨ Лобби #{lobby_unique_id} ✨\n\n"
            f"🎲 Режим: {mode} от {creator_display} (id: <code>{creator_game_id}</code>)\n"
            f"🔫 Оружия: {mode_weapons}\n"
            f"🗺 Карта: {map_name}\n"
            f"⏳ Время: {time_limit}\n"
            f"🎯 Урон: {damage_type}\n"
            f"🌍 Регион: {region}\n\n"
        )
        
        if lobby_full:
            lobby_text += f"✅ Матч готов! Набралось {players_count} игроков\n\n<b>👥 Игроки в лобби:</b>\n"
        else:
            lobby_text += f"👥 Игроков в лобби: {players_count}/{max_players}\n⏳ Ожидаем еще {max_players - players_count} игроков...\n\n<b>👥 Игроки в лобби:</b>\n"
        
        for i, (player_id, player_nickname) in enumerate(players, 1):
            lobby_text += f"{i}. {display_nickname(player_id, player_nickname)}\n"
        
        if not players:
            lobby_text += "Пока никто не присоединился\n"
        
        return lobby_text, lobby_full
    except Exception as e:
        logger.error(f"Error formatting lobby info: {e}")
        return "❌ Ошибка при форматировании информации о лобби", False

async def cleanup_user_messages(user_id):
    """Очистка меню сообщений пользователя"""
    old_message_id = user_menu_messages.get(user_id)
    if old_message_id:
        try:
            await safe_delete_message(chat_id=user_id, message_id=old_message_id)
            user_menu_messages.delete(user_id)
        except Exception as e:
            logger.warning(f"Error cleaning up user message for {user_id}: {e}")

async def cleanup_lobby_messages(user_id):
    """Полная очистка всех сообщений лобби для пользователя"""
    try:
        # Получаем message_id из кэша
        old_message_id = user_lobby_messages.get(user_id)
        
        # Удаляем сообщения из кэша
        user_lobby_messages.delete(user_id)
        
        # Пытаемся удалить сообщение
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
            types.BotCommand(command="backupd", description="↩️ Откатить статистику")
        ])
    
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
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    first_name = message.from_user.first_name
    
    await set_menu_button(user_id)
    await set_bot_commands_for_user(user_id)
    
    await cleanup_user_messages(user_id)
    
    args = message.text.split()
    if len(args) > 1 and args[1] == "join_lobby":
        if db.is_user_registered(user_id):
            await send_message_with_image(message.chat.id, 
                                        "🎮 Добро пожаловать! Вы перешли по ссылке присоединения к лобби.\n\nВыберите действие:", 
                                        MENU_IMAGE_URL, 
                                        get_main_keyboard(user_id))
        else:
            await message.answer(
                f"👋 Привет, {first_name}!\n\nДля участия в лобби нужно зарегистрироваться:", 
                reply_markup=get_registration_keyboard()
            )
        return
    
    if db.is_user_registered(user_id):
        menu_text = f"👋 Привет, {first_name}!\n\nВыберите действие:"
        await send_message_with_image(
            message.chat.id, 
            menu_text, 
            MENU_IMAGE_URL, 
            get_main_keyboard(user_id)
        )
    else:
        start_message = await message.answer(
            f"👋 Привет, {first_name}!\n\nНажмите чтобы зарегистрироваться:", 
            reply_markup=get_registration_keyboard()
        )
        user_menu_messages.put(user_id, start_message.message_id)

@dp.message(Command("profile"))
async def cmd_profile(message: types.Message):
    await show_profile_from_message(message)

async def show_profile_from_message(message: types.Message):
    user_id = message.from_user.id
    
    if not db.is_user_registered(user_id):
        await message.answer("❌ Вы не зарегистрированы!")
        return
    
    player_data = db.get_player_profile(user_id)
    if not player_data:
        await message.answer("❌ Профиль не найден!")
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
        prem_line = f"\n⭐️ Премиум: {days_left} дней\n"
    
    profile_text = (
        f"👤 Ваш профиль | {username_display}\n\n"
        f"🆔 <code>{game_id}</code>\n"
        f"📈 Уровень: {level}\n"
        f"🏆 Рейтинг: {rating}{prem_line}\n"
        f"📊 Статистика:\n"
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
        await message.answer("❌ Нет прав для этой команды.")
        return
    
    # Проверяем, что команда выполняется в группе модераторов
    if str(message.chat.id) != MODERATOR_GROUP_ID:
        await message.answer("❌ Эта команда работает только в группе модераторов!")
        return
    
    args = message.text.split()
    
    if len(args) == 4:
        try:
            tg_id, kills, deaths = int(args[1]), int(args[2]), int(args[3])
            
            if not (1 <= kills <= 1000) or not (1 <= deaths <= 1000):
                await message.answer("❌ Количество киллов/смертей должно быть от 1 до 1000")
                return
            
            lobby_id = db.get_lobby_id_by_topic_thread_id(message.message_thread_id)
            if not lobby_id:
                await message.answer("❌ Эта команда работает только в теме лобби!")
                return
            
            lobby_info = db.get_lobby_by_id(lobby_id)
            if not lobby_info:
                await message.answer("❌ Лобби не найдено.")
                return
            
            lobby_unique_id = lobby_info[1]
            
            if not db.is_user_in_lobby(tg_id, lobby_id):
                await message.answer("❌ Этот игрок не участвовал в этом лобби!")
                return
            
            if db.has_stats_been_added(tg_id, lobby_id):
                await message.answer("❌ Статистика для этого игрока уже была начислена за это лобби. Сначала сделайте откат командой /backupd.")
                return
            
            success, rating_added = db.update_player_stats_by_user_id(tg_id, kills, deaths, lobby_id)
            
            if success:
                await message.answer(f"✅ Статистика для игрока {tg_id} в лобби #{lobby_unique_id} обновлена!")
                await notify_player_about_processing(tg_id, lobby_unique_id, kills, deaths, rating_added)
            else:
                await message.answer("❌ Ошибка при обновлении статистики.")
                
        except ValueError:
            await message.answer("❌ Ошибка в формате чисел.")
    else:
        await message.answer("Используйте:\n/upd tg_id kills deaths\n\nОграничение: 1-1000 киллов/смертей")

@dp.message(Command("backupd"))
async def cmd_revert_stats(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Нет прав для этой команды.")
        return
    
    # Проверяем, что команда выполняется в группе модераторов
    if str(message.chat.id) != MODERATOR_GROUP_ID:
        await message.answer("❌ Эта команда работает только в группе модераторов!")
        return
    
    args = message.text.split()
    
    if len(args) == 2:
        try:
            tg_id = int(args[1])
            
            lobby_id = db.get_lobby_id_by_topic_thread_id(message.message_thread_id)
            if not lobby_id:
                await message.answer("❌ Эта команда работает только в теме лобби!")
                return
            
            lobby_info = db.get_lobby_by_id(lobby_id)
            if not lobby_info:
                await message.answer("❌ Лобби не найдено.")
                return
            
            lobby_unique_id = lobby_info[1]
            
            history_data = db.get_last_stats_history_by_lobby_user(lobby_id, tg_id)
            
            if not history_data:
                await message.answer("❌ История изменений не найдена.")
                return
            
            history_id, user_id, screenshot_id, kills_added, deaths_added, rating_added, created_at = history_data
            
            if db.revert_stats(history_id):
                await message.answer(f"✅ Изменения для игрока {tg_id} в лобби #{lobby_unique_id} отменены!")
                try:
                    notification_text = (
                        f"⚠️ Статистика по лобби #{lobby_unique_id} была отменена модератором.\n\n"
                        f"Отмененная статистика:\n"
                        f"• Убийств: -{kills_added}\n"
                        f"• Смертей: -{deaths_added}\n"
                        f"🏆 Рейтинг: -{rating_added}\n\n"
                        f"По вопросам к @bosin1337"
                    )
                    await bot.send_message(chat_id=user_id, text=notification_text)
                except Exception as e:
                    logger.error(f"Error sending revert notification to {user_id}: {e}")
            else:
                await message.answer("❌ Ошибка при отмене изменений.")
                
        except Exception as e:
            logger.error(f"Error in backupd command: {e}")
            await message.answer(f"❌ Ошибка при выполнении команды: {str(e)}")
    else:
        await message.answer("Используйте: /backupd tg_id")

# УДАЛЕНЫ КОМАНДЫ ОЧИСТКИ СТАТИСТИКИ
# /clear_stats, /clear_weekly, /clear_lobbies больше нет

@dp.message(Command("post"))
async def cmd_post(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Нет прав для этой команды.")
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
            "• [ссылка](t.me/KingDM_robot)"
        )
        return
    
    broadcast_text = args[1]
    
    await state.update_data(broadcast_text=broadcast_text)
    
    try:
        await message.answer(f"📢 Тестовое сообщение:\n\n{broadcast_text}", parse_mode='MarkdownV2', disable_web_page_preview=True)
    except Exception as e:
        await message.answer(f"❌ Ошибка в форматировании Markdown: {str(e)}\n\nУбедитесь, что вы правильно используете * _ ` для форматирования.")
        return
    
    confirm_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, отправить", callback_data="confirm_broadcast"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_broadcast")
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
        await message.answer("❌ Нет прав для этой команды.")
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
            f"👥 Пользователи:\n"
            f"• Всего пользователей: {total_users}\n"
            f"• Новых за сегодня: {new_users_today}\n\n"
            f"🎮 Лобби:\n"
            f"• Всего сыгранных лобби: {total_lobbies}"
        )
        
        await message.answer(stat_text)
        
    except Exception as e:
        logger.error(f"Error getting bot stats: {e}")
        await message.answer(f"❌ Ошибка при получении статистики: {str(e)}")

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
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("❌ Нет прав для создания лобби", show_alert=True)
        return
    
    if db.get_user_active_lobby(callback_query.from_user.id):
        await callback_query.answer("❌ Вы уже находитесь в активном лобби", show_alert=True)
        return
    
    await cleanup_user_messages(callback_query.from_user.id)
    await state.set_state(CreateLobbyStates.waiting_for_players)
    await send_message_with_image(callback_query.from_user.id, "🎮 Создание лобби\n\nВведите количество игроков для лобби (от 3 до 10):", None, get_cancel_keyboard())
    await callback_query.answer()

@dp.message(CreateLobbyStates.waiting_for_players)
async def process_lobby_players(message: types.Message, state: FSMContext):
    try:
        max_players = int(message.text.strip())
        
        if max_players < 3 or max_players > 10:
            await message.answer("❌ Введите число от 3 до 10")
            return
        
        await state.update_data(max_players=max_players)
        await state.set_state(CreateLobbyStates.waiting_for_mode)
        
        await cleanup_user_messages(message.from_user.id)
        mode_text = "🎮 Выберите режим игры:\n\n" + "\n".join(f"• {mode}: {info['weapons']}" for mode, info in MODES.items())
        await send_message_with_image(message.from_user.id, mode_text, None, get_mode_keyboard())
        
    except ValueError:
        await message.answer("❌ Пожалуйста, введите число от 3 до 10")

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
    await send_message_with_image(callback_query.from_user.id, "🗺 Выберите карта:", None, get_map_keyboard())
    await callback_query.answer()

@dp.callback_query(CreateLobbyStates.waiting_for_map, lambda c: c.data.startswith("map_"))
async def process_lobby_map(callback_query: types.CallbackQuery, state: FSMContext):
    try:
        map_index = int(callback_query.data.replace("map_", ""))
        
        if 0 <= map_index < len(MAPS):
            await state.update_data(map_name=MAPS[map_index])
            await state.set_state(CreateLobbyStates.waiting_for_time)
            
            await cleanup_user_messages(callback_query.from_user.id)
            await send_message_with_image(callback_query.from_user.id, "⏰ Выберите время игры:", None, get_time_keyboard())
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
            await send_message_with_image(callback_query.from_user.id, "🎯 Выберите тип урона:", None, get_damage_keyboard())
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
            await send_message_with_image(callback_query.from_user.id, "🌍 Выберите регион:", None, get_region_keyboard())
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
            
            # Создаем лобби без ссылки
            lobby_id, lobby_unique_id = db.create_lobby(
                creator_id=user_id,
                lobby_link="",  # Пустая ссылка
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
                    await send_message_with_image(user_id, "❌ Ошибка: информация о лобби не найдена", None, get_main_keyboard(user_id))
            else:
                await send_message_with_image(user_id, "❌ Ошибка при создании лобби", None, get_main_keyboard(user_id))
            
            await state.clear()
        else:
            await callback_query.answer("❌ Неверный регион")
    except ValueError:
        await callback_query.answer("❌ Ошибка в данных региона")
    except Exception as e:
        logger.error(f"Error creating lobby: {e}")
        await send_message_with_image(callback_query.from_user.id, "❌ Ошибка при создании лобби", None, get_main_keyboard(callback_query.from_user.id))
        await state.clear()
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "active_lobbies")
async def show_active_lobbies(callback_query: types.CallbackQuery):
    lobbies = db.get_active_lobbies()
    
    await cleanup_user_messages(callback_query.from_user.id)
    await cleanup_lobby_messages(callback_query.from_user.id)
    
    if not lobbies:
        await send_message_with_image(callback_query.from_user.id, "🎮 Активные лобби\n\n❌ Нет активных лобби", None, get_lobbies_keyboard())
    else:
        await send_message_with_image(callback_query.from_user.id, "🎮 Активные лобби\n\nВыберите лобби для просмотра:", None, get_lobby_list_keyboard(lobbies))
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

@dp.callback_query(lambda c: c.data.startswith("join_lobby_"))
async def join_lobby(callback_query: types.CallbackQuery):
    try:
        lobby_id = int(callback_query.data.replace("join_lobby_", ""))
        user_id = callback_query.from_user.id
        
        if db.get_user_active_lobby(user_id):
            await callback_query.answer("❌ Вы уже находитесь в активном лобби", show_alert=True)
            return
        
        success, message = db.join_lobby(user_id, lobby_id)
        
        if success:
            await callback_query.answer("✅ Вы присоединились к лобби")
            
            lobby_info = db.get_lobby_by_id(lobby_id)
            if not lobby_info:
                await callback_query.answer("❌ Лобби не найдено", show_alert=True)
                return
                
            players = db.get_lobby_players(lobby_id)
            players_count = len(players)
            max_players = lobby_info[9]
            
            is_creator = lobby_info[2] == user_id
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
                    notification_text = f"🎉 Лобби №{lobby_info[1]} заполнено!\n\n✅ Набралось {players_count} игроков\n🎮 Скопируйте id хостера чтобы присоединиться.\n📸 После игры отправьте скриншот с результатами!"
                    
                    for player_id, player_nickname in players:
                        try:
                            await bot.send_message(chat_id=player_id, text=notification_text)
                        except Exception as e:
                            logger.warning(f"Error sending notification to {player_id}: {e}")
                            continue
                except Exception as e:
                    logger.error(f"Error sending lobby filled notification: {e}")
            else:
                await update_lobby_message_for_all_players(lobby_id)
                
        else:
            await callback_query.answer(f"❌ {message}", show_alert=True)
    except ValueError:
        await callback_query.answer("❌ Неверный ID лобби", show_alert=True)
    except Exception as e:
        logger.error(f"Error joining lobby: {e}")
        await callback_query.answer("❌ Ошибка при присоединении к лобби", show_alert=True)

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
        if not lobby_info or lobby_info[2] != user_id:
            await callback_query.answer("❌ Вы не можете удалить это лобби", show_alert=True)
            return
        
        # Получаем информацию об игроках перед удалением
        players = db.get_lobby_players(lobby_id)
        lobby_unique_id = lobby_info[1]
        
        await delete_lobby_channel_message(lobby_id)
        
        # Удаляем лобби и получаем информацию
        success, deleted_lobby_unique_id, player_ids = db.delete_lobby(lobby_id)
        
        if success:
            await callback_query.answer("✅ Лобби удалено", show_alert=True)
            
            # Уведомляем всех игроков
            for player_id, player_nickname in players:
                try:
                    # Очищаем сообщения лобби для этого игрока
                    await cleanup_lobby_messages(player_id)
                    await cleanup_user_messages(player_id)
                    
                    # Отправляем уведомление
                    await bot.send_message(
                        chat_id=player_id,
                        text=f"❌ Лобби #{lobby_unique_id} было удалено хостером!"
                    )
                    
                    # Перенаправляем к активным лобби
                    lobbies = db.get_active_lobbies()
                    if not lobbies:
                        await send_message_with_image(
                            player_id, 
                            "❌ Нет активных лобби", 
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
            
            # Также очищаем сообщения для создателя
            await cleanup_lobby_messages(user_id)
            await cleanup_user_messages(user_id)
            
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
            text="📸 Отправьте скриншот с результатами матча\n\n",
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
        "❌ Отправка скриншота отменена",
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
        await message.answer("❌ Пожалуйста, отправьте скриншот как фото.")
        return
    
    if not lobby_id:
        await message.answer("❌ Ошибка: лобби не найдено.")
        await state.clear()
        return
    
    lobby_info = db.get_lobby_by_id(lobby_id)
    if not lobby_info:
        await message.answer("❌ Лобби не найдено.")
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
        await message.answer("❌ Тема лобби не найдена. Обратитесь к администратору.")
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
        f"📸Новый скриншот к лобби #{lobby_unique_id} от {user_info}\n\n"
        f"Требуется ручная обработка❗️\n"
        f"Команды: /upd tg_id kills deaths"
    )
    
    try:
        await bot.send_photo(
            chat_id=MODERATOR_GROUP_ID,
            message_thread_id=topic_thread_id,
            photo=photo_file_id,
            caption=caption
        )
            
        await cleanup_user_messages(user_id)
        await send_message_with_image(
            user_id,
            "✅ Скриншот отправлен на модерацию!\n\n"
            "📊 После проверки статистика будет автоматически обновлена.\n\n"
            "По вопросам к @bosin1337",
            None,
            get_back_keyboard()
        )
    except Exception as e:
        logger.error(f"Error sending screenshot: {e}")
        await send_message_with_image(user_id, "❌ Ошибка при отправке скриншота. Попробуйте позже.", None, get_back_keyboard())
    
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
    await send_message_with_image(user_id, "📝 Введите ваш игровой никнейм:", None, get_cancel_keyboard())
    await callback_query.answer()

@dp.message(RegistrationStates.waiting_for_nickname)
async def process_nickname(message: types.Message, state: FSMContext):
    nickname = message.text.strip()
    user_id = message.from_user.id
    
    if len(nickname) < 3 or len(nickname) > 16:
        await message.answer("❌ Никнейм должен быть от 3 до 16 символов")
        return
    
    if not re.match(r'^[a-zA-Z0-9_]+$', nickname):
        await message.answer("❌ Никнейм должен содержать только английские буквы, цифры и подчеркивания")
        return
    
    await state.update_data(nickname=nickname)
    await state.set_state(RegistrationStates.waiting_for_game_id)
    
    await cleanup_user_messages(user_id)
    await send_message_with_image(
        user_id,
        "🆔 Введите ваш игровой ID (2–13 символов: только цифры и латинские буквы):",
        None,
        get_cancel_keyboard()
    )

@dp.message(RegistrationStates.waiting_for_game_id)
async def process_game_id(message: types.Message, state: FSMContext):
    game_id = message.text.strip()
    user_id = message.from_user.id
    
    if not is_valid_game_id(game_id):
        await message.answer(
            "❌ Игровой ID: от 2 до 13 символов, только цифры и латинские буквы"
        )
        return
    
    if db.is_game_id_taken(game_id):
        await message.answer("❌ Этот игровой ID уже занят")
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
        menu_text = f"✅ Регистрация завершена!\n\n👋 Добро пожаловать, {nickname}!\n\nВыберите действие:"
        await send_message_with_image(message.chat.id, menu_text, MENU_IMAGE_URL, get_main_keyboard(user_id))
    else:
        await message.answer(f"❌ {message_text}")

@dp.callback_query(lambda c: c.data == "cancel_registration")
async def cancel_registration(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback_query.from_user.id
    first_name = callback_query.from_user.first_name
    
    await cleanup_user_messages(user_id)
    
    if db.is_user_registered(user_id):
        menu_text = f"👋 Привет, {first_name}!\n\nВыберите действие:"
        await send_message_with_image(user_id, menu_text, MENU_IMAGE_URL, get_main_keyboard(user_id))
    else:
        await send_message_with_image(
            user_id,
            f"👋 Привет, {first_name}!\n\nНажмите чтобы зарегистрироваться:",
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
        prem_line = f"\n⭐️ Премиум: {days_left} дней\n"
    
    profile_text = (
        f"👤 Ваш профиль | {username_display}\n\n"
        f"🆔 <code>{game_id}</code>\n"
        f"📈 Уровень: {level}\n"
        f"🏆 Рейтинг: {rating}{prem_line}\n"
        f"📊 Статистика:\n"
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
        history_text = "🎮 История сыгранных лобби\n\n❌ Вы еще не играли в заполненных лобби"
        await send_message_with_image(user_id, history_text, None, get_back_keyboard())
        await callback_query.answer()
        return
    
    limit = 5
    current_page = 1
    total_pages = (total_lobbies + limit - 1) // limit
    
    history_text = f"🎮 Сыгранные лобби (страница {current_page}/{total_pages}):\n\n"
    
    for i, lobby in enumerate(lobbies, 1):
        (lobby_id, lobby_unique_id, mode, map_name, created_at, 
         kills_added, deaths_added, rating_added, stats_date, has_stats) = lobby
        
        try:
            lobby_date = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y %H:%M")
        except ValueError:
            lobby_date = created_at
        
        history_text += f"🔹Лобби #{lobby_unique_id}\n"
        history_text += f"🗺 Карта: {map_name}\n"
        history_text += f"🎮 Режим: {mode}\n"
        history_text += f"📊 Статистика:\n"
        
        if has_stats and kills_added > 0:
            history_text += f"Убийств - {kills_added}\n"
            history_text += f"Смертей - {deaths_added}\n"
            history_text += f"🏆 Рейтинг: +{rating_added}\n"
        else:
            history_text += f"🔎 На модерации\n"
            history_text += f"🏆 Рейтинг: 🕗 В стадии обработки\n"
        
        history_text += f"🗓 Дата: {lobby_date}\n"
        
        if i < len(lobbies):
            history_text += "➖➖➖➖➖➖➖➖➖➖➖\n\n"
    
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
        
        history_text = f"🎮 Сыгранные лобби (страница {current_page}/{total_pages}):\n\n"
        
        for i, lobby in enumerate(lobbies, 1):
            (lobby_id, lobby_unique_id, mode, map_name, created_at, 
             kills_added, deaths_added, rating_added, stats_date, has_stats) = lobby
            
            try:
                lobby_date = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y %H:%M")
            except ValueError:
                lobby_date = created_at
            
            history_text += f"🔹Лобби #{lobby_unique_id}\n"
            history_text += f"🗺 Карта: {map_name}\n"
            history_text += f"🎮 Режим: {mode}\n"
            history_text += f"📊 Статистика:\n"
            
            if has_stats and kills_added > 0:
                history_text += f"Убийств - {kills_added}\n"
                history_text += f"Смертей - {deaths_added}\n"
                history_text += f"🏆 Рейтинг: +{rating_added}\n"
            else:
                history_text += f"🔎 На модерации\n"
                history_text += f"🏆 Рейтинг: 🕗 В стадии обработки\n"
            
            history_text += f"🗓 Дата: {lobby_date}\n"
            
            if i < len(lobbies):
                history_text += "➖➖➖➖➖➖➖➖➖➖➖\n\n"
        
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
        
        history_text = f"🎮 Сыгранные лобби (страница {current_page}/{total_pages}):\n\n"
        
        for i, lobby in enumerate(lobbies, 1):
            (lobby_id, lobby_unique_id, mode, map_name, created_at, 
             kills_added, deaths_added, rating_added, stats_date, has_stats) = lobby
            
            try:
                lobby_date = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y %H:%M")
            except ValueError:
                lobby_date = created_at
            
            history_text += f"🔹Лобби #{lobby_unique_id}\n"
            history_text += f"🗺 Карта: {map_name}\n"
            history_text += f"🎮 Режим: {mode}\n"
            history_text += f"📊 Статистика:\n"
            
            if has_stats and kills_added > 0:
                history_text += f"Убийств - {kills_added}\n"
                history_text += f"Смертей - {deaths_added}\n"
                history_text += f"🏆 Рейтинг: +{rating_added}\n"
            else:
                history_text += f"🔎 На модерации\n"
                history_text += f"🏆 Рейтинг: 🕗 В стадии обработки\n"
            
            history_text += f"🗓 Дата: {lobby_date}\n"
            
            if i < len(lobbies):
                history_text += "➖➖➖➖➖➖➖➖➖➖➖\n\n"
        
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

@dp.callback_query(lambda c: c.data == "top")
async def show_top_menu(callback_query: types.CallbackQuery):
    top_menu_text = "🏆 Выберите тип топа:"
    await cleanup_user_messages(callback_query.from_user.id)
    await send_message_with_image(callback_query.from_user.id, top_menu_text, TOP_IMAGE_URL, get_top_keyboard())
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "top_weekly")
async def show_weekly_top(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    
    top_players = db.get_weekly_top_players()
    
    top_text = "🏆 Еженедельный топ (топ-10):\n\n"
    
    if not top_players:
        top_text += "❌ Пока нет игроков с статистикой\n\n"
    else:
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        
        for i, player_data in enumerate(top_players):
            user_id_player, nickname, rating, kills, deaths, matches = player_data
            
            kd_ratio = kills / deaths if deaths > 0 else kills
            level = get_player_level(rating)
            
            medal = medals[i] if i < len(medals) else f"{i+1}."
            
            nick_disp = display_nickname(user_id_player, nickname)
            top_text += f"{medal} {nick_disp} | {level}\n🏆 {rating} | K/D: {kd_ratio:.2f} | 🎮 {matches}\n\n"
    
    player_position = db.get_player_weekly_position(user_id)
    
    if player_position > 0:
        top_text += f"🎯 Ваша позиция: {player_position}\n\n"
    else:
        top_text += "⚠️ Вас нет в этом топе\n\n"
    
    top_text += "💰 Призовой фонд: 5.000 голды\n🔄 Обновляется каждый понедельник"
    
    await cleanup_user_messages(user_id)
    await send_message_with_image(user_id, top_text, TOP_IMAGE_URL, get_top_keyboard())
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "top_all_time")
async def show_all_time_top(callback_query: types.CallbackQuery):
    top_players = db.get_all_time_top_players()
    user_id = callback_query.from_user.id
    
    has_any_stats = db.get_player_has_any_stats(user_id)
    
    if not top_players:
        top_text = "🏆 Топ игроков:\n\n❌ Пока нет данных"
        await cleanup_user_messages(user_id)
        await send_message_with_image(user_id, top_text, TOP_IMAGE_URL, get_top_keyboard())
        return
    
    top_text = "🏆 Топ игроков за все время:\n\n"
    
    for i, player_data in enumerate(top_players):
        user_id_player, nickname, rating, matches_played, kills, deaths = player_data
        
        kd_ratio = kills / deaths if deaths > 0 else kills
        level = get_player_level(rating)
        
        nick_disp = display_nickname(user_id_player, nickname)
        top_text += f"{i+1}. {nick_disp} | {level}\n🏆 {rating} | K/D: {kd_ratio:.2f} | 🎮 {matches_played}\n\n"
    
    if has_any_stats:
        player_position = db.get_player_all_time_position(user_id)
        if player_position > 0:
            top_text += f"🎯 Ваша позиция: {player_position}"
        else:
            top_text += "⚠️ Вас нет в этом топе"
    else:
        top_text += "⚠️ Вас нет в этом топе"
    
    await cleanup_user_messages(user_id)
    await send_message_with_image(user_id, top_text, TOP_IMAGE_URL, get_top_keyboard())
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "premium_menu")
async def premium_menu(callback_query: types.CallbackQuery):
    uid = callback_query.from_user.id
    if not db.is_user_registered(uid):
        await callback_query.answer("Сначала зарегистрируйтесь в боте.", show_alert=True)
        return
    await cleanup_user_messages(uid)
    caption = "🌟 <b>Premium Status</b>\n\nНа сколько ты хочешь купить подписку?"
    await send_message_with_image(
        uid,
        caption,
        PREMIUM_IMAGE_URL,
        get_premium_period_keyboard(),
    )
    await callback_query.answer()


@dp.callback_query(lambda c: c.data in ("premium_pick_30", "premium_pick_90"))
async def premium_pick_period(callback_query: types.CallbackQuery):
    uid = callback_query.from_user.id
    if not db.is_user_registered(uid):
        await callback_query.answer("Сначала зарегистрируйтесь.", show_alert=True)
        return
    days = PREMIUM_30_DAYS if callback_query.data.endswith("30") else PREMIUM_90_DAYS
    usd = PREMIUM_PRICE_USD_30 if days == 30 else PREMIUM_PRICE_USD_90
    stars = PREMIUM_STARS_30 if days == 30 else PREMIUM_STARS_90
    text = (
        f"🛒 Покупка: Premium Status ({days} дней)\n"
        f"💵 Цена: ${usd} / {stars} Stars (звёзды в Telegram)\n\n"
        f"Выберите способ оплаты:"
    )
    await cleanup_user_messages(uid)
    await send_message_with_image(uid, text, None, get_premium_payment_keyboard(days))
    await callback_query.answer()


@dp.callback_query(lambda c: c.data.startswith("premium_pay_crypto_"))
async def premium_pay_crypto(callback_query: types.CallbackQuery):
    uid = callback_query.from_user.id
    if not db.is_user_registered(uid):
        await callback_query.answer("Сначала зарегистрируйтесь.", show_alert=True)
        return
    try:
        days = int(callback_query.data.replace("premium_pay_crypto_", ""))
    except ValueError:
        await callback_query.answer()
        return
    if days not in (PREMIUM_30_DAYS, PREMIUM_90_DAYS):
        await callback_query.answer()
        return
    if not CRYPTOBOT_TOKEN:
        await callback_query.answer(
            "Оплата через CryptoBot не настроена (CRYPTOBOT_TOKEN в .env).",
            show_alert=True,
        )
        return
    usd = PREMIUM_PRICE_USD_30 if days == 30 else PREMIUM_PRICE_USD_90
    body = {
        "currency_type": "fiat",
        "fiat": "USD",
        "amount": str(usd),
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
    pending_cryptobot_invoices[uid] = {"invoice_id": int(invoice_id), "days": days}
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💲 Оплатить в CryptoBot", url=pay_url)],
            [
                InlineKeyboardButton(
                    text="🔍 Проверить оплату",
                    callback_data=f"premium_cbcheck:{invoice_id}:{uid}",
                )
            ],
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"premium_pick_back_{days}")],
        ]
    )
    await cleanup_user_messages(uid)
    await bot.send_message(
        chat_id=uid,
        text=(
            f"🌟 Оплата Premium Status ({days} д.)\n"
            f"💲 Сумма: ${usd}\n\n"
            f"⚠️ У вас 10 минут на оплату."
        ),
        reply_markup=kb,
    )
    await callback_query.answer()


@dp.callback_query(lambda c: c.data.startswith("premium_pick_back_"))
async def premium_pick_back(callback_query: types.CallbackQuery):
    """Возврат к выбору способа оплаты с тем же периодом."""
    uid = callback_query.from_user.id
    try:
        days = int(callback_query.data.replace("premium_pick_back_", ""))
    except ValueError:
        await callback_query.answer()
        return
    usd = PREMIUM_PRICE_USD_30 if days == 30 else PREMIUM_PRICE_USD_90
    stars = PREMIUM_STARS_30 if days == 30 else PREMIUM_STARS_90
    text = (
        f"🛒 Покупка: Premium Status ({days} дней)\n"
        f"💵 Цена: ${usd} / {stars} Stars (звёзды в Telegram)\n\n"
        f"Выберите способ оплаты:"
    )
    await cleanup_user_messages(uid)
    await send_message_with_image(uid, text, None, get_premium_payment_keyboard(days))
    await callback_query.answer()


@dp.callback_query(lambda c: c.data.startswith("premium_cbcheck:"))
async def premium_cryptobot_check(callback_query: types.CallbackQuery):
    parts = callback_query.data.split(":")
    if len(parts) != 3:
        await callback_query.answer()
        return
    try:
        invoice_id = int(parts[1])
        owner_id = int(parts[2])
    except ValueError:
        await callback_query.answer()
        return
    if callback_query.from_user.id != owner_id:
        await callback_query.answer("Это не ваш счёт.", show_alert=True)
        return
    pend = pending_cryptobot_invoices.get(owner_id)
    if not pend or int(pend["invoice_id"]) != invoice_id:
        await callback_query.answer("Создайте новый счёт из меню Premium.", show_alert=True)
        return
    days = pend["days"]
    paid, err = await asyncio.to_thread(cryptobot_invoice_is_paid, invoice_id)
    if err and not paid:
        await callback_query.answer(f"Ошибка: {err}", show_alert=True)
        return
    if not paid:
        await callback_query.answer("Оплата ещё не поступила.", show_alert=True)
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


@dp.callback_query(lambda c: c.data.startswith("premium_pay_stars_"))
async def premium_pay_stars(callback_query: types.CallbackQuery):
    uid = callback_query.from_user.id
    if not db.is_user_registered(uid):
        await callback_query.answer("Сначала зарегистрируйтесь.", show_alert=True)
        return
    try:
        days = int(callback_query.data.replace("premium_pay_stars_", ""))
    except ValueError:
        await callback_query.answer()
        return
    if days not in (PREMIUM_30_DAYS, PREMIUM_90_DAYS):
        await callback_query.answer()
        return
    stars = PREMIUM_STARS_30 if days == 30 else PREMIUM_STARS_90
    try:
        await bot.send_invoice(
            chat_id=uid,
            title="Premium Status",
            description=f"Подписка Premium на {days} дней",
            payload=f"premium_stars_{days}",
            currency="XTR",
            prices=[LabeledPrice(label=f"Premium {days} дней", amount=stars)],
            provider_token="",
        )
    except Exception as e:
        logger.error(f"send_invoice stars: {e}")
        await callback_query.answer("Не удалось выставить счёт Stars.", show_alert=True)
        return
    await callback_query.answer()


@dp.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: types.PreCheckoutQuery):
    payload = pre_checkout_query.invoice_payload or ""
    if payload.startswith("premium_stars_"):
        await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)
    else:
        await bot.answer_pre_checkout_query(
            pre_checkout_query.id,
            ok=False,
            error_message="Неизвестный товар",
        )


@dp.message(F.successful_payment)
async def on_successful_payment(message: types.Message):
    sp = message.successful_payment
    if not sp:
        return
    payload = sp.invoice_payload or ""
    if not payload.startswith("premium_stars_"):
        return
    try:
        days = int(payload.replace("premium_stars_", ""))
    except ValueError:
        return
    if days not in (PREMIUM_30_DAYS, PREMIUM_90_DAYS):
        return
    uid = message.from_user.id
    ref = f"stars_{sp.telegram_payment_charge_id}"
    ok = await finalize_premium_purchase(uid, days, "telegram_stars", ref)
    if not ok:
        await message.answer("Не удалось активировать Premium (возможно, платёж уже учтён).")
        return
    try:
        await message.delete()
    except Exception:
        pass
    await show_post_purchase_main_menu(
        message.chat.id, uid, message.from_user.first_name, days
    )


@dp.callback_query(lambda c: c.data == "edit_profile")
async def edit_profile_menu(callback_query: types.CallbackQuery):
    uid = callback_query.from_user.id
    if not db.is_user_registered(uid):
        await callback_query.answer("❌ Вы не зарегистрированы!", show_alert=True)
        return
    await cleanup_user_messages(uid)
    await send_message_with_image(
        uid,
        "✏️ Редактирование профиля\n\nВыберите действие:",
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
        "✏️ Введите новый никнейм (как при регистрации):",
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
        "🆔 Введите новый игровой ID (2–13 символов: только цифры и латинские буквы):",
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
        "✏️ Редактирование профиля\n\nВыберите действие:",
        None,
        get_edit_profile_keyboard(),
    )
    await callback_query.answer()


@dp.message(ProfileEditStates.waiting_for_new_nickname)
async def process_profile_new_nickname(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    nickname = (message.text or "").strip()
    if len(nickname) < 3 or len(nickname) > 16:
        await message.answer("❌ Никнейм должен быть от 3 до 16 символов")
        return
    if not re.match(r"^[a-zA-Z0-9_]+$", nickname):
        await message.answer(
            "❌ Никнейм должен содержать только английские буквы, цифры и подчёркивания"
        )
        return
    ok, err = await asyncio.to_thread(db.update_player_nickname_if_allowed, uid, nickname)
    if not ok:
        await message.answer(f"❌ {err}")
        return
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    await cleanup_user_messages(uid)
    await message.answer("✅ Никнейм обновлён!")
    await show_profile_from_message(message)


@dp.message(ProfileEditStates.waiting_for_new_game_id)
async def process_profile_new_game_id(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    game_id = (message.text or "").strip()
    if not is_valid_game_id(game_id):
        await message.answer(
            "❌ Игровой ID: от 2 до 13 символов, только цифры и латинские буквы"
        )
        return
    ok, err = await asyncio.to_thread(db.update_player_game_id_if_allowed, uid, game_id)
    if not ok:
        await message.answer(f"❌ {err}")
        return
    await state.clear()
    try:
        await message.delete()
    except Exception:
        pass
    await cleanup_user_messages(uid)
    await message.answer("✅ Игровой ID обновлён!")
    await show_profile_from_message(message)


@dp.callback_query(lambda c: c.data == "back_to_main")
async def back_to_main(callback_query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback_query.from_user.id
    first_name = callback_query.from_user.first_name
    
    await cleanup_user_messages(user_id)
    await cleanup_lobby_messages(user_id)
    
    menu_text = f"👋 Привет, {first_name}!\n\nВыберите действие:"
    await send_message_with_image(user_id, menu_text, MENU_IMAGE_URL, get_main_keyboard(user_id))
    await callback_query.answer()

async def cleanup_old_messages():
    """Очистка старых сообщений из кэша"""
    # LRUCache автоматически управляет размером, поэтому просто логируем
    try:
        lobby_size = len(user_lobby_messages.cache)
        menu_size = len(user_menu_messages.cache)
        if lobby_size > 900 or menu_size > 900:
            logger.info(f"Cache sizes - Lobby: {lobby_size}, Menu: {menu_size}")
    except Exception as e:
        logger.error(f"Error in cleanup_old_messages: {e}")

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
    
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
