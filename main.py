import sqlite3
import asyncio
import logging
import json
import io
from datetime import datetime

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton,
    InputMediaPhoto, InputMediaVideo, BufferedInputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties

# ==========================================
# ⚙️ НАСТРОЙКИ КОНФИГУРАЦИИ
# ==========================================
CLIENT_BOT_TOKEN = '8308242609:AAFlq_DN5HAiqROdUVBDL9IvYdgGjD4AoQM'
ADMIN_BOT_TOKEN = '8577361834:AAHBRxOenUqFk_cZcCWdZmycXFiTlBLdsGs'

# ID группы менеджеров (куда приходят отчеты)
MANAGERS_GROUP_ID = -1003528230429

# ID Канала-архива (для хранения медиафайлов)
STORAGE_CHANNEL_ID = -1003719357983

# ID Главного администратора (резервный доступ)
INITIAL_ADMIN_ID = 1748938261

# Логирование (запись событий в консоль)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger("BeatonSystem")

# Инициализация ботов
bot_client = Bot(token=CLIENT_BOT_TOKEN, default=DefaultBotProperties(parse_mode="Markdown"))
bot_admin = Bot(token=ADMIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode="Markdown"))
dp = Dispatcher()

# Глобальные переменные
active_chats = {}  # Активные диалоги: {user_id: admin_id}
admin_bot_username = ""  # Заполняется при старте

# Словари для маппинга фильтров
FILTER_MAP = {
    "prod_beton": "Бетон",
    "prod_asfalt": "Асфальт",
    "cat_proud": "Горжусь результатом!",
    "cat_process": "Рабочий процесс",
    "cat_nuance": "Есть нюанс...",
    "con_yes": "Да, разрешаю",
    "con_no": "Только для служебного использования"
}


# ==========================================
# 🗄️ РАБОТА С БАЗОЙ ДАННЫХ (SQLite)
# ==========================================
def init_db():
    """Создает таблицы в базе данных, если их нет."""
    conn = sqlite3.connect('beaton_factory.db')
    cur = conn.cursor()

    # Таблица отчетов
    cur.execute('''CREATE TABLE IF NOT EXISTS reports 
                   (id TEXT PRIMARY KEY, product TEXT, intent TEXT, 
                    comment TEXT, consent TEXT, photos TEXT, video TEXT, 
                    username TEXT, user_id INTEGER, status TEXT DEFAULT 'new', 
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    client_name TEXT,
                    storage_msg_ids TEXT)''')

    # Обновление таблицы (миграция для старых версий БД)
    try:
        cur.execute("ALTER TABLE reports ADD COLUMN storage_msg_ids TEXT")
    except sqlite3.OperationalError:
        pass

    # Таблица настроек
    cur.execute('''CREATE TABLE IF NOT EXISTS settings 
                   (key TEXT PRIMARY KEY, value TEXT)''')

    # Таблица администраторов
    cur.execute('''CREATE TABLE IF NOT EXISTS admins 
                   (user_id INTEGER PRIMARY KEY)''')

    # Добавление главного админа по умолчанию
    cur.execute("SELECT count(*) FROM admins")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (INITIAL_ADMIN_ID,))
        logger.info(f"DB init: Added default admin {INITIAL_ADMIN_ID}")

    conn.commit()
    conn.close()


def is_admin(user_id):
    """Проверяет, является ли пользователь администратором."""
    conn = sqlite3.connect('beaton_factory.db')
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM admins WHERE user_id = ?", (user_id,))
    res = cur.fetchone()
    conn.close()
    return res is not None or user_id == INITIAL_ADMIN_ID


# --- Вспомогательные функции БД ---

def save_dashboard_id(msg_id):
    conn = sqlite3.connect('beaton_factory.db')
    cur = conn.cursor()
    if msg_id == 0:
        cur.execute("DELETE FROM settings WHERE key='last_dashboard_id'")
    else:
        cur.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('last_dashboard_id', ?)", (str(msg_id),))
    conn.commit()
    conn.close()


def get_dashboard_id():
    conn = sqlite3.connect('beaton_factory.db')
    cur = conn.cursor()
    cur.execute("SELECT value FROM settings WHERE key='last_dashboard_id'")
    res = cur.fetchone()
    conn.close()
    return int(res[0]) if res else None


def generate_report_id():
    conn = sqlite3.connect('beaton_factory.db')
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM reports")
    count = cur.fetchone()[0]
    conn.close()
    return f"B-{count + 1:03d}"


def get_dashboard_data():
    conn = sqlite3.connect('beaton_factory.db')
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM reports")
    total = cur.fetchone()[0]
    cur.execute(
        "SELECT id, product, comment, username FROM reports WHERE intent='Есть нюанс...' AND status='new' ORDER BY timestamp DESC")
    issues = cur.fetchall()
    conn.close()
    return total, issues


# ==========================================
# ⌨️ КЛАВИАТУРЫ И МЕНЮ
# ==========================================
def get_admin_main_menu():
    kb = [
        [KeyboardButton(text="🔎 Поиск по фильтрам"), KeyboardButton(text="📋 Просмотр отзывов")],
        [KeyboardButton(text="👥 Группа Beaton"), KeyboardButton(text="⚙️ Настройки")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def get_settings_menu():
    kb = [
        [KeyboardButton(text="➕ Добавить админа"), KeyboardButton(text="➖ Удалить админа")],
        [KeyboardButton(text="🔙 Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def get_chat_control_menu():
    kb = [[KeyboardButton(text="🔴 Завершить чат")]]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def get_nav_menu():
    kb = [[KeyboardButton(text="🏠 В главное меню")]]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


# ==========================================
# 📊 ПАНЕЛЬ УПРАВЛЕНИЯ (DASHBOARD)
# ==========================================
async def update_global_dashboard():
    """Обновляет закрепленное сообщение со статистикой в группе менеджеров."""
    total, issues = get_dashboard_data()

    text = "🏢 **BEATON | ПАНЕЛЬ УПРАВЛЕНИЯ**\n"
    text += "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
    text += f"📊 **Всего получено отчетов: {total}**\n"
    text += "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"

    builder = InlineKeyboardBuilder()

    if issues:
        text += "🚨 **ТРЕБУЮТ ВНИМАНИЯ (Нюансы):**\n"
        for rid, product, comment, user in issues:
            short_comment = (comment[:25] + '..') if len(comment) > 25 else comment
            text += f"• `{rid}` | {product} | {user}\n   _{short_comment}_\n\n"
            link = f"https://t.me/{admin_bot_username}?start=take_{rid}"
            builder.row(InlineKeyboardButton(text=f"⚡️ Решить {rid}", url=link))
    else:
        text += "✅ **Все нюансы отработаны.**\n"

    builder.row(
        InlineKeyboardButton(text="🔍 Поиск по фильтрам", url=f"https://t.me/{admin_bot_username}?start=filters"))
    builder.row(
        InlineKeyboardButton(text="💬 Просмотр отзывов клиента", url=f"https://t.me/{admin_bot_username}?start=list_0"))

    kb = builder.as_markup()
    msg_id = get_dashboard_id()

    # Попытка редактирования старого сообщения
    if msg_id:
        try:
            await bot_admin.edit_message_text(
                text=text, chat_id=MANAGERS_GROUP_ID, message_id=msg_id,
                reply_markup=kb, parse_mode="Markdown"
            )
            return
        except Exception as e:
            if "message is not modified" in str(e): return
            try:
                await bot_admin.delete_message(chat_id=MANAGERS_GROUP_ID, message_id=msg_id)
            except:
                pass

    # Отправка нового, если старое недоступно
    try:
        new_msg = await bot_admin.send_message(
            chat_id=MANAGERS_GROUP_ID, text=text, reply_markup=kb, parse_mode="Markdown"
        )
        save_dashboard_id(new_msg.message_id)
    except Exception as e:
        logger.error(f"Dashboard send error: {e}")


# ==========================================
# 🔄 МАШИНА СОСТОЯНИЙ (FSM)
# ==========================================
class ReportState(StatesGroup):
    choosing_product = State()
    choosing_intent = State()
    uploading_media = State()
    writing_comment = State()
    naming = State()
    granting_consent = State()
    in_chat = State()


class AdminManage(StatesGroup):
    waiting_for_add_id = State()
    waiting_for_del_id = State()


# ==========================================
# 📨 ЛОГИКА ОТПРАВКИ ОТЧЕТОВ
# ==========================================
async def send_full_report(chat_id, report_id):
    conn = sqlite3.connect('beaton_factory.db')
    cur = conn.cursor()
    cur.execute("SELECT * FROM reports WHERE id=?", (report_id,))
    r = cur.fetchone()
    conn.close()

    if not r:
        return await bot_admin.send_message(chat_id, f"❌ Ошибка: отчет {report_id} не найден.")

    username = r[7] if r[7] else "Не указан"
    client_name = r[11]

    full_caption = (
        f"📦 **ОТЧЕТ {r[0]}**\n"
        f"🏗 {r[1]} | {r[2]}\n"
        f"👤 **Логин:** {username}\n"
    )
    if client_name: full_caption += f"📛 **Имя:** {client_name}\n"
    full_caption += (f"⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n💬 {r[3]}\n📄 Статус: {r[4]}")

    p_ids = json.loads(r[5]) if r[5] else []
    v_ids = json.loads(r[6]) if r[6] else []

    media_group = []

    for i, fid in enumerate(p_ids):
        if i == 0:
            media_group.append(InputMediaPhoto(media=fid, caption=full_caption, parse_mode="Markdown"))
        else:
            media_group.append(InputMediaPhoto(media=fid))

    for i, vid in enumerate(v_ids):
        if not media_group and i == 0:
            media_group.append(InputMediaVideo(media=vid, caption=full_caption, parse_mode="Markdown"))
        else:
            media_group.append(InputMediaVideo(media=vid))

    if media_group:
        if len(media_group) == 1:
            if isinstance(media_group[0], InputMediaPhoto):
                await bot_admin.send_photo(chat_id, media_group[0].media, caption=full_caption)
            else:
                await bot_admin.send_video(chat_id, media_group[0].media, caption=full_caption)
        else:
            await bot_admin.send_media_group(chat_id, media_group)
    else:
        await bot_admin.send_message(chat_id, full_caption)


# ==========================================
# 🤖 ХЕНДЛЕРЫ АДМИН-БОТА
# ==========================================

# Триггер обновления панели в группе (по слову "beaton")
@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text)
async def group_dashboard_call(message: types.Message):
    text = message.text.lower()
    if "beaton" in text or text.startswith("/panel"):
        try:
            await message.delete()
        except:
            pass

        old_id = get_dashboard_id()
        if old_id:
            try:
                await bot_admin.delete_message(chat_id=MANAGERS_GROUP_ID, message_id=old_id)
            except:
                pass
            save_dashboard_id(0)

        await update_global_dashboard()


# Команда /start для Админа
@dp.message(Command("start"), F.bot.id == bot_admin.id)
async def admin_start_handler(message: types.Message, command: CommandObject, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    # Сбрасываем любые зависшие состояния при старте
    await state.clear()

    arg = command.args

    if not arg:
        return await message.answer("🛠 Главное меню системы Beaton:", reply_markup=get_admin_main_menu())

    # Логика принятия отчета в работу (take_ID)
    if arg.startswith("take_"):
        rid = arg.split("_")[1]
        conn = sqlite3.connect('beaton_factory.db')
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM reports WHERE id=?", (rid,))
        res = cur.fetchone()

        if res:
            uid = res[0]
            cur.execute("UPDATE reports SET status='in_progress' WHERE id=?", (rid,))
            conn.commit()

            active_chats[uid] = message.from_user.id
            active_chats[message.from_user.id] = uid

            await dp.fsm.resolve_context(bot_client, uid, uid).set_state(ReportState.in_chat)
            conn.close()

            await message.answer(
                f"⚡️ **Вы взяли в работу отчет {rid}**\nВы подключены к пользователю. Все, что вы напишете здесь, отправится ему.",
                reply_markup=get_chat_control_menu()
            )
            try:
                await bot_client.send_message(uid, "👨‍💼 Менеджер Beaton подключился к диалогу.")
            except:
                await message.answer("⚠️ Не удалось отправить сообщение пользователю.")

            await update_global_dashboard()
        else:
            conn.close()
            await message.answer("❌ Отчет не найден.")
        return

    # Логика фильтров
    if arg == "filters":
        await state.update_data(selected=[])
        await message.answer("Меню навигации обновлено.", reply_markup=get_nav_menu())
        return await message.answer("🔎 **ФИЛЬТРЫ ОТЧЕТОВ BEATON**", reply_markup=get_filter_keyboard([]))

    # Просмотр списка
    if arg.startswith("list_"):
        try:
            offset = int(arg.split("_")[1])
            await message.answer("Меню навигации обновлено.", reply_markup=get_nav_menu())
            return await list_reviews_paginated(message, offset, is_callback=False)
        except:
            pass

    # Просмотр конкретного отчета
    if arg.startswith("view_"):
        rid = arg.split("_")[1]
        await send_full_report(message.chat.id, rid)


# --- НАВИГАЦИЯ МЕНЮ (С ВЫСОКИМ ПРИОРИТЕТОМ) ---
# Используем StateFilter('*'), чтобы эти кнопки работали ДАЖЕ если админ находится в процессе ввода ID

@dp.message(F.text == "🔎 Поиск по фильтрам", F.bot.id == bot_admin.id, StateFilter('*'))
async def menu_filters(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.clear()  # Сброс состояния
    await state.update_data(selected=[])
    await message.answer("Вы перешли в раздел фильтров.", reply_markup=get_nav_menu())
    await message.answer("🔎 **ФИЛЬТРЫ ОТЧЕТОВ BEATON**", reply_markup=get_filter_keyboard([]))


@dp.message(F.text == "📋 Просмотр отзывов", F.bot.id == bot_admin.id, StateFilter('*'))
async def menu_list(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.clear()  # Сброс состояния
    await message.answer("Вы перешли в список отзывов.", reply_markup=get_nav_menu())
    await list_reviews_paginated(message, 0, is_callback=False)


@dp.message(F.text == "👥 Группа Beaton", F.bot.id == bot_admin.id, StateFilter('*'))
async def menu_group(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.clear()
    await message.answer(f"Перейти в рабочую группу менеджеров:\nhttps://t.me/c/{str(MANAGERS_GROUP_ID)[4:]}/1")


@dp.message(F.text == "⚙️ Настройки", F.bot.id == bot_admin.id, StateFilter('*'))
async def menu_settings(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.clear()
    await message.answer("⚙️ **Управление доступом:**", reply_markup=get_settings_menu())


@dp.message(F.text.in_({"🔙 Назад", "🏠 В главное меню"}), F.bot.id == bot_admin.id, StateFilter('*'))
async def back_to_main(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.clear()  # Важно: сбрасываем любые зависшие вводы
    await message.answer("🛠 Главное меню:", reply_markup=get_admin_main_menu())


# --- УПРАВЛЕНИЕ АДМИНАМИ ---

@dp.message(F.text == "➕ Добавить админа", F.bot.id == bot_admin.id, StateFilter('*'))
async def add_admin_start(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.clear()  # Сброс предыдущих действий
    await message.answer("✏️ **Введите Telegram ID нового администратора:**")
    await state.set_state(AdminManage.waiting_for_add_id)


@dp.message(AdminManage.waiting_for_add_id, F.bot.id == bot_admin.id)
async def add_admin_process(message: types.Message, state: FSMContext):
    # Проверка на случайное нажатие кнопок меню обрабатывается фильтрами выше
    if not message.text or not message.text.isdigit():
        return await message.answer("❌ Ошибка формата. Пришлите ID цифрами.")

    new_admin_id = int(message.text)
    conn = sqlite3.connect('beaton_factory.db')
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM admins WHERE user_id = ?", (new_admin_id,))

    if cur.fetchone():
        conn.close()
        return await message.answer(f"⚠️ Пользователь `{new_admin_id}` уже является админом.")

    cur.execute("INSERT INTO admins (user_id) VALUES (?)", (new_admin_id,))
    conn.commit()
    conn.close()
    await message.answer(f"✅ Пользователь `{new_admin_id}` успешно добавлен!", reply_markup=get_settings_menu())
    await state.clear()


@dp.message(F.text == "➖ Удалить админа", F.bot.id == bot_admin.id, StateFilter('*'))
async def del_admin_start(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return

    # ИСПРАВЛЕНИЕ: Сбрасываем старое состояние (например, добавления) перед началом удаления
    await state.clear()

    conn = sqlite3.connect('beaton_factory.db')
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM admins")
    admins = [str(r[0]) for r in cur.fetchall()]
    conn.close()

    list_text = "\n".join([f"- `{a}`" for a in admins])
    await message.answer(f"✏️ **Введите Telegram ID администратора для удаления:**\n\n{list_text}")
    await state.set_state(AdminManage.waiting_for_del_id)


@dp.message(AdminManage.waiting_for_del_id, F.bot.id == bot_admin.id)
async def del_admin_process(message: types.Message, state: FSMContext):
    if not message.text or not message.text.isdigit():
        return await message.answer("❌ Ошибка формата. Введите ID цифрами.")

    target_id = int(message.text)

    conn = sqlite3.connect('beaton_factory.db')
    cur = conn.cursor()

    # Проверка, существует ли админ (опционально, для красоты вывода)
    cur.execute("SELECT user_id FROM admins WHERE user_id = ?", (target_id,))
    if not cur.fetchone():
        conn.close()
        return await message.answer(f"⚠️ Администратор `{target_id}` не найден в базе.")

    cur.execute("DELETE FROM admins WHERE user_id = ?", (target_id,))
    conn.commit()
    conn.close()

    await message.answer(f"🗑 Администратор `{target_id}` успешно удален.", reply_markup=get_settings_menu())
    await state.clear()


# ==========================================
# 🗂 СПИСКИ И ФИЛЬТРАЦИЯ
# ==========================================
def get_filter_keyboard(selected_keys):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="─── ПРОДУКЦИЯ ───", callback_data="none"))

    p_btns = []
    for k, v in {"prod_beton": "Бетон", "prod_asfalt": "Асфальт"}.items():
        mark = "✅ " if k in selected_keys else ""
        p_btns.append(InlineKeyboardButton(text=f"{mark}{v}", callback_data=f"tgl_{k}"))
    builder.row(*p_btns)

    builder.row(InlineKeyboardButton(text="─── КАТЕГОРИИ ───", callback_data="none"))
    i_btns = []
    for k, v in {"cat_proud": "Горжусь!", "cat_process": "Процесс", "cat_nuance": "Нюанс"}.items():
        mark = "✅ " if k in selected_keys else ""
        i_btns.append(InlineKeyboardButton(text=f"{mark}{v}", callback_data=f"tgl_{k}"))
    builder.row(*i_btns)

    builder.row(InlineKeyboardButton(text="─── ИСПОЛЬЗОВАНИЕ ───", callback_data="none"))
    c_btns = []
    for k, v in {"con_yes": "Разрешено ✅", "con_no": "Служебное 🔒"}.items():
        mark = "🔹 " if k in selected_keys else ""
        c_btns.append(InlineKeyboardButton(text=f"{mark}{v}", callback_data=f"tgl_{k}"))
    builder.row(*c_btns)

    builder.row(InlineKeyboardButton(text="❌ Очистить", callback_data="f_clear"),
                InlineKeyboardButton(text="🔍 ПРИМЕНИТЬ", callback_data="f_apply"))
    builder.row(InlineKeyboardButton(text="🏠 В главное меню", callback_data="close_list"))

    return builder.as_markup()


async def list_reviews_paginated(message: types.Message, offset: int = 0, is_callback: bool = False):
    conn = sqlite3.connect('beaton_factory.db')
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM reports")
    total_count = cur.fetchone()[0]

    cur.execute("SELECT id, product, consent FROM reports ORDER BY timestamp DESC LIMIT 10 OFFSET ?", (offset,))
    records = cur.fetchall()
    conn.close()

    if not records:
        text = "Отчетов пока нет."
        return await (message.edit_text(text) if is_callback else message.answer(text))

    text = f"📂 **СПИСОК ОТЗЫВОВ ({offset + 1}-{offset + len(records)} из {total_count}):**\n⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
    for r in records:
        status_icon = "✅" if r[2] == "Да, разрешаю" else "🔒"
        text += f"• `{r[0]}` | {r[1]} | {status_icon} — [открыть](https://t.me/{admin_bot_username}?start=view_{r[0]})\n"

    builder = InlineKeyboardBuilder()
    nav_btns = []
    if offset > 0:
        nav_btns.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"pag_list_{max(0, offset - 10)}"))

    nav_btns.append(InlineKeyboardButton(text="🏠 В главное меню", callback_data="close_list"))

    if offset + 10 < total_count:
        nav_btns.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"pag_list_{offset + 10}"))

    builder.row(*nav_btns)

    if is_callback:
        await message.edit_text(text, reply_markup=builder.as_markup(), disable_web_page_preview=True)
    else:
        await message.answer(text, reply_markup=builder.as_markup(), disable_web_page_preview=True)


# --- Callback хендлеры для списков и фильтров ---

@dp.callback_query(F.data.startswith("pag_list_"))
async def pag_list_cb(callback: types.CallbackQuery):
    offset = int(callback.data.split("_")[2])
    await list_reviews_paginated(callback.message, offset, is_callback=True)
    await callback.answer()


@dp.callback_query(F.data == "close_list")
async def close_list_cb(callback: types.CallbackQuery):
    await callback.message.delete()
    await callback.message.answer("🛠 Главное меню системы Beaton:", reply_markup=get_admin_main_menu())
    await callback.answer()


@dp.callback_query(F.data == "f_apply")
async def filter_apply(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    keys = data.get("selected", [])
    if not keys: return await callback.answer("Выберите параметры!", show_alert=True)

    vals = [FILTER_MAP[k] for k in keys if k in FILTER_MAP]

    conn = sqlite3.connect('beaton_factory.db')
    cur = conn.cursor()

    q = "SELECT id, product, consent FROM reports WHERE 1=1"
    params = []

    pr = [v for v in vals if v in ["Бетон", "Асфальт"]]
    it = [v for v in vals if v in ["Горжусь результатом!", "Рабочий процесс", "Есть нюанс..."]]
    co = [v for v in vals if v in ["Да, разрешаю", "Только для служебного использования"]]

    if pr:
        q += f" AND product IN ({','.join(['?'] * len(pr))})"
        params.extend(pr)
    if it:
        q += f" AND intent IN ({','.join(['?'] * len(it))})"
        params.extend(it)
    if co:
        q += f" AND consent IN ({','.join(['?'] * len(co))})"
        params.extend(co)

    cur.execute(q + " ORDER BY timestamp DESC", params)
    res = cur.fetchall()
    conn.close()

    if not res:
        return await callback.answer("Ничего не найдено", show_alert=True)

    await state.update_data(found_ids=[r[0] for r in res])

    text = f"🔎 **Результаты ({len(res)}):**\n"
    for r in res[:10]:
        icon = "✅" if r[2] == "Да, разрешаю" else "🔒"
        text += f"• `{r[0]}` | {r[1]} | {icon} — [открыть](https://t.me/{admin_bot_username}?start=view_{r[0]})\n"

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🚀 ОТКРЫТЬ ВСЕ (ПО 10)", callback_data="show_bulk_0"))
    builder.row(InlineKeyboardButton(text="⬅️ Назад к фильтрам", callback_data="filters_back"))
    builder.row(InlineKeyboardButton(text="🏠 В главное меню", callback_data="close_list"))

    await callback.message.edit_text(text, reply_markup=builder.as_markup(), disable_web_page_preview=True)


@dp.callback_query(F.data.startswith("show_bulk_"))
async def show_bulk_reports(callback: types.CallbackQuery, state: FSMContext):
    offset = int(callback.data.split("_")[2])
    data = await state.get_data()
    ids = data.get("found_ids", [])
    chunk = ids[offset:offset + 10]

    for rid in chunk:
        await send_full_report(callback.message.chat.id, rid)
        await asyncio.sleep(0.3)

    if len(ids) > offset + 10:
        builder = InlineKeyboardBuilder()
        builder.add(InlineKeyboardButton(text="➡️ Еще 10", callback_data=f"show_bulk_{offset + 10}"))
        await bot_admin.send_message(callback.message.chat.id, "Продолжить?", reply_markup=builder.as_markup())

    await callback.answer()


@dp.callback_query(F.data.startswith("tgl_"))
async def toggle_filter(callback: types.CallbackQuery, state: FSMContext):
    key = callback.data.replace("tgl_", "")
    data = await state.get_data()
    selected = data.get("selected", [])

    if key in selected:
        selected.remove(key)
    else:
        selected.append(key)

    await state.update_data(selected=selected)
    await callback.message.edit_reply_markup(reply_markup=get_filter_keyboard(selected))


@dp.callback_query(F.data == "f_clear")
async def filter_clear_cb(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(selected=[])
    await callback.message.edit_reply_markup(reply_markup=get_filter_keyboard([]))


@dp.callback_query(F.data == "filters_back")
async def filters_back_cb(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await callback.message.edit_text("🔎 **ФИЛЬТРЫ ОТЧЕТОВ BEATON**",
                                     reply_markup=get_filter_keyboard(data.get("selected", [])))


# ==========================================
# 📱 ЛОГИКА КЛИЕНТСКОГО БОТА (Сбор отчетов)
# ==========================================

@dp.message(Command("start"), F.bot.id == bot_client.id)
@dp.message(F.text == "📝 Отправить новый отчет", F.bot.id == bot_client.id)
async def cmd_start_client(message: types.Message, state: FSMContext):
    await state.clear()
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Бетон"), KeyboardButton(text="Асфальт")]],
                             resize_keyboard=True)
    await message.answer("Выберите продукт Beaton:", reply_markup=kb)
    await state.set_state(ReportState.choosing_product)


@dp.message(ReportState.choosing_product)
async def prod_chosen(msg: types.Message, state: FSMContext):
    if msg.text not in ["Бетон", "Асфальт"]: return await msg.answer("❌ Выберите продукт кнопкой.")
    await state.update_data(product=msg.text)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Горжусь результатом!")], [KeyboardButton(text="Рабочий процесс")],
                  [KeyboardButton(text="Есть нюанс...")]], resize_keyboard=True)
    await msg.answer("Цель сообщения?", reply_markup=kb)
    await state.set_state(ReportState.choosing_intent)


@dp.message(ReportState.choosing_intent)
async def intent_chosen(msg: types.Message, state: FSMContext):
    if msg.text not in ["Горжусь результатом!", "Рабочий процесс", "Есть нюанс..."]:
        return await msg.answer("❌ Используйте кнопки.")

    await state.update_data(intent=msg.text, temp_photos=[], temp_videos=[], is_processing=False)
    await msg.answer("Прикрепите до 3 фото или 1 видео:", reply_markup=ReplyKeyboardRemove())
    await state.set_state(ReportState.uploading_media)


@dp.message(ReportState.uploading_media)
async def process_media(msg: types.Message, state: FSMContext):
    # Логирование входящего медиа
    user_id = msg.from_user.id
    logger.info(f"USER {user_id}: Received media part. Type: {'Photo' if msg.photo else 'Video'}")

    # 1. Сбор медиа (без мгновенной проверки лимитов)
    data = await state.get_data()
    p, v = data.get('temp_photos', []), data.get('temp_videos', [])

    if msg.photo:
        p.append(msg.photo[-1].file_id)
        await state.update_data(temp_photos=p)
    elif msg.video or msg.animation:
        fid = msg.animation.file_id if msg.animation else msg.video.file_id
        v.append(fid)
        await state.update_data(temp_videos=v)
    else:
        return await msg.answer("❌ Пожалуйста, отправьте фото или видео.")

    # 2. Ожидание остальных частей медиа-группы
    await asyncio.sleep(1.5)

    # 3. Финальная обработка
    final_data = await state.get_data()
    final_p = final_data.get('temp_photos', [])
    final_v = final_data.get('temp_videos', [])

    # Блокировка повторных срабатываний для одной группы файлов
    if final_data.get('is_processing'):
        return

    # 4. Валидация лимитов
    has_error = False
    err_text = ""

    if len(final_p) > 3:
        has_error = True
        err_text = f"❌ ОШИБКА: Максимум 3 фото. Вы отправили {len(final_p)}."
    elif len(final_v) > 1:
        has_error = True
        err_text = f"❌ ОШИБКА: Максимум 1 видео. Вы отправили {len(final_v)}."
    elif len(final_p) > 0 and len(final_v) > 0:
        has_error = True
        err_text = "❌ ОШИБКА: Нельзя смешивать фото и видео."

    if has_error:
        await state.update_data(is_processing=True)
        await msg.answer(err_text + "\nПопробуйте снова.")
        await state.update_data(temp_photos=[], temp_videos=[], is_processing=False)
        return

    # 5. Успешный переход дальше
    if final_p or final_v:
        logger.info(f"USER {user_id}: Batch accepted.")
        await state.update_data(is_processing=True)
        await msg.answer("✅ Принято. Напишите комментарий:")
        await state.set_state(ReportState.writing_comment)


@dp.message(ReportState.writing_comment)
async def comm_written(msg: types.Message, state: FSMContext):
    if not msg.text: return await msg.answer("❌ Напишите текст комментария.")
    await state.update_data(comment=msg.text)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Пропустить")]], resize_keyboard=True)
    await msg.answer("Как к вам обращаться?", reply_markup=kb)
    await state.set_state(ReportState.naming)


@dp.message(ReportState.naming)
async def name_written(msg: types.Message, state: FSMContext):
    if not msg.text: return await msg.answer("❌ Напишите имя.")
    name = None if msg.text == "Пропустить" else msg.text
    await state.update_data(client_name=name)

    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Да, разрешаю")], [KeyboardButton(text="Только для служебного использования")]],
        resize_keyboard=True)
    await msg.answer("Разрешаете использование в соцсетях?", reply_markup=kb)
    await state.set_state(ReportState.granting_consent)


@dp.message(ReportState.granting_consent)
async def final_proc(msg: types.Message, state: FSMContext):
    if msg.text not in ["Да, разрешаю", "Только для служебного использования"]:
        return await msg.answer("❌ Используйте кнопки.")

    data = await state.get_data()
    rid = generate_report_id()
    uname = f"@{msg.from_user.username}" if msg.from_user.username else msg.from_user.full_name

    # Сохранение медиа в канал-архив
    storage_msg_ids = []
    media_group = []
    caption_text = f"Report: {rid}"

    if data.get('temp_photos'):
        for i, ph in enumerate(data['temp_photos']):
            if i == 0:
                media_group.append(InputMediaPhoto(media=ph, caption=caption_text))
            else:
                media_group.append(InputMediaPhoto(media=ph))

    if data.get('temp_videos'):
        for i, vd in enumerate(data['temp_videos']):
            if not media_group and i == 0:
                media_group.append(InputMediaVideo(media=vd, caption=caption_text))
            else:
                media_group.append(InputMediaVideo(media=vd))

    try:
        if STORAGE_CHANNEL_ID and media_group:
            if len(media_group) > 1:
                sent_msgs = await bot_client.send_media_group(chat_id=STORAGE_CHANNEL_ID, media=media_group)
                storage_msg_ids = [m.message_id for m in sent_msgs]
            else:
                if isinstance(media_group[0], InputMediaPhoto):
                    sent = await bot_client.send_photo(chat_id=STORAGE_CHANNEL_ID, photo=media_group[0].media,
                                                       caption=caption_text)
                else:
                    sent = await bot_client.send_video(chat_id=STORAGE_CHANNEL_ID, video=media_group[0].media,
                                                       caption=caption_text)
                storage_msg_ids = [sent.message_id]
    except Exception as e:
        logger.error(f"Failed to save to storage channel: {e}")

    # Запись в БД
    conn = sqlite3.connect('beaton_factory.db')
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO reports (id, product, intent, comment, consent, photos, video, username, user_id, client_name, storage_msg_ids) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (rid, data['product'], data['intent'], data['comment'], msg.text,
         json.dumps(data.get('temp_photos', [])),
         json.dumps(data.get('temp_videos', [])),
         uname, msg.from_user.id, data.get('client_name'),
         json.dumps(storage_msg_ids))
    )
    conn.commit()
    conn.close()

    await msg.answer(f"✅ Отчет #{rid} успешно отправлен!",
                     reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📝 Отправить новый отчет")]],
                                                      resize_keyboard=True))
    await state.clear()
    await update_global_dashboard()


# ==========================================
# 💬 ЧАТ МЕНЕДЖЕР <-> КЛИЕНТ
# ==========================================

# Сообщения от АДМИНА к Клиенту
@dp.message(F.chat.type == "private", F.bot.id == bot_admin.id)
async def manager_msg(msg: types.Message):
    if not is_admin(msg.from_user.id): return
    mid = msg.from_user.id
    if mid in active_chats:
        uid = active_chats[mid]

        if msg.text == "/stop" or msg.text == "🔴 Завершить чат":
            active_chats.pop(mid, None)
            active_chats.pop(uid, None)
            await msg.answer("🔴 Диалог завершен.", reply_markup=get_admin_main_menu())
            try:
                await bot_client.send_message(uid, "Диалог с менеджером завершен.")
                await dp.fsm.resolve_context(bot_client, uid, uid).clear()
            except:
                pass
            return

        if msg.text:
            try:
                await bot_client.send_message(uid, f"👨‍💼 **Менеджер:**\n{msg.text}")
            except Exception as e:
                await msg.answer(f"❌ Не удалось отправить (клиент заблокировал бота?): {e}")


# Сообщения от КЛИЕНТА к Админу
@dp.message(ReportState.in_chat, F.bot.id == bot_client.id)
async def client_msg_handler(msg: types.Message):
    uid = msg.from_user.id
    if uid in active_chats:
        admin_id = active_chats[uid]

        if msg.text:
            await bot_admin.send_message(admin_id, f"👤 **Клиент:**\n{msg.text}")

        # Обработка фото (через скачивание, чтобы не терять доступ к файлу)
        elif msg.photo:
            try:
                file_io = io.BytesIO()
                await bot_client.download(msg.photo[-1], destination=file_io)
                file_io.seek(0)
                input_file = BufferedInputFile(file_io.read(), filename="client_photo.jpg")
                await bot_admin.send_photo(admin_id, photo=input_file, caption="👤 **Клиент прислал фото**")
            except Exception as e:
                logger.error(f"Bridge photo error: {e}")
                await bot_admin.send_message(admin_id, "👤 Клиент прислал фото (ошибка пересылки).")
        else:
            await bot_admin.send_message(admin_id, "👤 **Клиент** прислал файл (формат не поддерживается).")
    else:
        await msg.answer("Диалог не активен.")


# ==========================================
# 🚀 ЗАПУСК СИСТЕМЫ
# ==========================================
async def main():
    init_db()

    global admin_bot_username
    bot_info = await bot_admin.get_me()
    admin_bot_username = bot_info.username

    logger.info(f"Система запущена. Admin Bot: @{admin_bot_username}")
    logger.info(f"Initial Admin ID: {INITIAL_ADMIN_ID}")

    await dp.start_polling(bot_client, bot_admin)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):

        logger.info("Бот остановлен!")
