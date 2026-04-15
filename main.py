import asyncio
import logging
import os
import sqlite3
from contextlib import closing
from datetime import datetime
from html import escape
from typing import List, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)


# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8588546241:AAGuHivJBcMueKkk6nrklygRviTbsb7-Qho")
ADMIN_ID = int(os.getenv("ADMIN_ID", "6954213997"))
SHOP_NAME = os.getenv("SHOP_NAME", "ShopBron")
SHOP_TAGLINE = os.getenv("SHOP_TAGLINE", "Премиальный магазин прямо в Telegram")
SHOP_PHONE = os.getenv("SHOP_PHONE", "+7 700 000 00 00")
MANAGER_NAME = os.getenv("MANAGER_NAME", "Персональный менеджер")
MANAGER_USERNAME = os.getenv("MANAGER_USERNAME", "shopbron_manager").replace("@", "")
CURRENCY = os.getenv("CURRENCY", "₸")
DB_PATH = os.getenv("DB_PATH", "premium_shop.db")

DELIVERY_TEXT = """🚚 <b>Доставка</b>

• По городу: в день заказа или на следующий день
• По Казахстану: 2–5 рабочих дней
• Оплата: перевод, карта, наличные при получении
• После оформления менеджер свяжется с вами"""

REVIEWS_TEXT = """⭐ <b>Отзывы</b>

— Всё пришло быстро и красиво упаковано
— Качество отличное, менеджер отвечает быстро
— Оформление заказа заняло меньше минуты"""

ABOUT_TEXT = f"""ℹ️ <b>{SHOP_NAME}</b>

{SHOP_TAGLINE}

📞 Телефон: {SHOP_PHONE}
💬 Менеджер: @{MANAGER_USERNAME}"""


# =========================
# DEMO PRODUCTS
# photo: можно вставить прямую ссылку на фото товара
# =========================
DEMO_PRODUCTS = [
    {
        "title": "Смарт-часы FitTime Pro",
        "price": 31990,
        "category": "Гаджеты",
        "description": "Стильные смарт-часы с уведомлениями, шагомером, мониторингом сна и премиальным дизайном.",
        "photo": "",
        "is_hit": 1,
        "is_new": 0,
    },
    {
        "title": "Наушники AirSound X1",
        "price": 24990,
        "category": "Аудио",
        "description": "Беспроводные наушники с глубоким звуком, шумоподавлением и удобной посадкой.",
        "photo": "",
        "is_hit": 1,
        "is_new": 0,
    },
    {
        "title": "Портативная колонка BeatBox Mini",
        "price": 17990,
        "category": "Аудио",
        "description": "Компактная колонка с мощным звуком, Bluetooth и хорошей автономностью.",
        "photo": "",
        "is_hit": 1,
        "is_new": 0,
    },
    {
        "title": "Рюкзак Urban Move",
        "price": 22990,
        "category": "Аксессуары",
        "description": "Городской рюкзак с защитой от влаги и отдельным карманом для ноутбука.",
        "photo": "",
        "is_hit": 0,
        "is_new": 1,
    },
    {
        "title": "Power Bank VoltMax 20000",
        "price": 19990,
        "category": "Гаджеты",
        "description": "Ёмкий внешний аккумулятор с быстрой зарядкой и стильным корпусом.",
        "photo": "",
        "is_hit": 0,
        "is_new": 1,
    },
    {
        "title": "Термокружка Steel Heat",
        "price": 8990,
        "category": "Аксессуары",
        "description": "Держит тепло до 6 часов. Удобный формат для города, авто и офиса.",
        "photo": "",
        "is_hit": 0,
        "is_new": 1,
    },
]


# =========================
# STATES
# =========================
class SearchState(StatesGroup):
    waiting_query = State()


class CheckoutState(StatesGroup):
    waiting_name = State()
    waiting_phone = State()
    waiting_address = State()
    waiting_comment = State()


# =========================
# DATABASE
# =========================
def db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def init_db() -> None:
    with closing(db()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                price INTEGER NOT NULL,
                category TEXT NOT NULL,
                description TEXT NOT NULL,
                photo TEXT DEFAULT '',
                is_hit INTEGER DEFAULT 0,
                is_new INTEGER DEFAULT 0,
                active INTEGER DEFAULT 1
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS carts (
                user_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (user_id, product_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                customer_name TEXT NOT NULL,
                phone TEXT NOT NULL,
                address TEXT NOT NULL,
                comment TEXT NOT NULL,
                total INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS order_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                price INTEGER NOT NULL,
                quantity INTEGER NOT NULL
            )
            """
        )

        count = cur.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        if count == 0:
            cur.executemany(
                """
                INSERT INTO products (title, price, category, description, photo, is_hit, is_new)
                VALUES (:title, :price, :category, :description, :photo, :is_hit, :is_new)
                """,
                DEMO_PRODUCTS,
            )

        conn.commit()


# =========================
# HELPERS
# =========================
def money(value: int) -> str:
    return f"{value:,}".replace(",", " ") + f" {CURRENCY}"


def short_text(text: str, limit: int = 18) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def escape_text(value: object) -> str:
    return escape(str(value))


def get_categories() -> List[str]:
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT DISTINCT category FROM products WHERE active = 1 ORDER BY category"
        ).fetchall()
    return [row[0] for row in rows]


def get_products(
    section: Optional[str] = None,
    category: Optional[str] = None,
    search: Optional[str] = None,
):
    with closing(db()) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        query = "SELECT * FROM products WHERE active = 1"
        params = []

        if section == "hits":
            query += " AND is_hit = 1"
        if section == "new":
            query += " AND is_new = 1"
        if category:
            query += " AND category = ?"
            params.append(category)
        if search:
            query += " AND (LOWER(title) LIKE ? OR LOWER(description) LIKE ?)"
            like = f"%{search.lower()}%"
            params.extend([like, like])

        query += " ORDER BY id ASC"
        return cur.execute(query, params).fetchall()


def get_product(product_id: int):
    with closing(db()) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM products WHERE id = ? AND active = 1",
            (product_id,),
        ).fetchone()
    return row


def cart_add(user_id: int, product_id: int, qty: int = 1) -> None:
    with closing(db()) as conn:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT quantity FROM carts WHERE user_id = ? AND product_id = ?",
            (user_id, product_id),
        ).fetchone()
        if row:
            cur.execute(
                "UPDATE carts SET quantity = quantity + ? WHERE user_id = ? AND product_id = ?",
                (qty, user_id, product_id),
            )
        else:
            cur.execute(
                "INSERT INTO carts (user_id, product_id, quantity) VALUES (?, ?, ?)",
                (user_id, product_id, qty),
            )
        conn.commit()


def cart_change(user_id: int, product_id: int, delta: int) -> None:
    with closing(db()) as conn:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT quantity FROM carts WHERE user_id = ? AND product_id = ?",
            (user_id, product_id),
        ).fetchone()
        if not row:
            return

        new_qty = row[0] + delta
        if new_qty <= 0:
            cur.execute(
                "DELETE FROM carts WHERE user_id = ? AND product_id = ?",
                (user_id, product_id),
            )
        else:
            cur.execute(
                "UPDATE carts SET quantity = ? WHERE user_id = ? AND product_id = ?",
                (new_qty, user_id, product_id),
            )
        conn.commit()


def cart_remove(user_id: int, product_id: int) -> None:
    with closing(db()) as conn:
        conn.execute(
            "DELETE FROM carts WHERE user_id = ? AND product_id = ?",
            (user_id, product_id),
        )
        conn.commit()


def cart_clear(user_id: int) -> None:
    with closing(db()) as conn:
        conn.execute("DELETE FROM carts WHERE user_id = ?", (user_id,))
        conn.commit()


def cart_items(user_id: int):
    with closing(db()) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT p.id, p.title, p.price, p.category, p.description, p.photo,
                   p.is_hit, p.is_new, c.quantity
            FROM carts c
            JOIN products p ON p.id = c.product_id
            WHERE c.user_id = ? AND p.active = 1
            ORDER BY p.id ASC
            """,
            (user_id,),
        ).fetchall()
    return rows


def cart_total(user_id: int) -> int:
    return sum(int(item["price"]) * int(item["quantity"]) for item in cart_items(user_id))


def cart_count(user_id: int) -> int:
    return sum(int(item["quantity"]) for item in cart_items(user_id))


def create_order(user_id: int, name: str, phone: str, address: str, comment: str):
    items = cart_items(user_id)
    total = sum(int(item["price"]) * int(item["quantity"]) for item in items)
    created_at = datetime.now().strftime("%d.%m.%Y %H:%M")

    with closing(db()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO orders (user_id, customer_name, phone, address, comment, total, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, name, phone, address, comment, total, created_at),
        )
        order_id = cur.lastrowid
        cur.executemany(
            """
            INSERT INTO order_items (order_id, product_id, title, price, quantity)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    order_id,
                    int(item["id"]),
                    str(item["title"]),
                    int(item["price"]),
                    int(item["quantity"]),
                )
                for item in items
            ],
        )
        conn.commit()

    cart_clear(user_id)
    return {
        "order_id": order_id,
        "items": items,
        "total": total,
        "created_at": created_at,
    }


def get_stats():
    with closing(db()) as conn:
        cur = conn.cursor()
        products = cur.execute("SELECT COUNT(*) FROM products WHERE active = 1").fetchone()[0]
        orders = cur.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        revenue = cur.execute("SELECT COALESCE(SUM(total), 0) FROM orders").fetchone()[0]
    return {"products": products, "orders": orders, "revenue": revenue}


def get_recent_orders(limit: int = 5):
    with closing(db()) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM orders ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return rows


def manager_url() -> str:
    return f"https://t.me/{MANAGER_USERNAME}" if MANAGER_USERNAME else "https://t.me"


def product_caption(product: sqlite3.Row) -> str:
    marks = []
    if int(product["is_hit"]):
        marks.append("ХИТ")
    if int(product["is_new"]):
        marks.append("НОВИНКА")
    badge = f" [{' • '.join(marks)}]" if marks else ""

    return (
        f"<b>{escape_text(product['title'])}</b>{badge}

"
        f"💵 Цена: <b>{money(int(product['price']))}</b>
"
        f"📂 Категория: {escape_text(product['category'])}

"
        f"{escape_text(product['description'])}"
    )


def products_text(title: str, items) -> str:
    if not items:
        return f"<b>{escape_text(title)}</b>

Пока в этом разделе ничего нет."

    lines = [f"<b>{escape_text(title)}</b>", ""]
    for item in items:
        marks = []
        if int(item["is_hit"]):
            marks.append("Хит")
        if int(item["is_new"]):
            marks.append("Новинка")
        suffix = f" • {', '.join(marks)}" if marks else ""
        lines.append(f"• {escape_text(item['title'])} — {money(int(item['price']))}{suffix}")

    lines.append("")
    lines.append("Нажмите на товар ниже, чтобы открыть карточку.")
    return "
".join(lines)


def cart_text(user_id: int) -> str:
    items = cart_items(user_id)
    if not items:
        return "🧺 <b>Корзина пуста</b>

Добавьте товары из каталога, а потом оформите заявку."

    lines = ["🧺 <b>Ваша корзина</b>", ""]
    for idx, item in enumerate(items, start=1):
        lines.append(
            f"{idx}. {escape_text(item['title'])} — {money(int(item['price']))} × {int(item['quantity'])}"
        )

    lines.append("")
    lines.append(f"Итого: <b>{money(cart_total(user_id))}</b>")
    lines.append(f"Позиций: <b>{cart_count(user_id)}</b>")
    lines.append("")
    lines.append("Нажмите <b>✅ Оформить</b>, чтобы отправить заказ админу.")
    return "
".join(lines)


def order_items_text(items) -> str:
    lines = []
    for idx, item in enumerate(items, start=1):
        qty = int(item["quantity"])
        price = int(item["price"])
        lines.append(
            f"{idx}. {escape_text(item['title'])} — {money(price)} × {qty} = {money(price * qty)}"
        )
    return "
".join(lines)


def user_tag(user) -> str:
    full_name = escape_text(user.full_name)
    if user.username:
        return f"{full_name} (@{escape_text(user.username)}) | ID: <code>{user.id}</code>"
    return f"{full_name} | ID: <code>{user.id}</code>"


# =========================
# REPLY KEYBOARDS
# =========================
def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛍 Коллекция"), KeyboardButton(text="🔥 Хиты")],
            [KeyboardButton(text="🆕 Новинки"), KeyboardButton(text="🔎 Поиск")],
            [KeyboardButton(text="🧺 Корзина"), KeyboardButton(text="💬 Менеджер")],
            [KeyboardButton(text="🚚 Доставка"), KeyboardButton(text="⭐ Отзывы")],
            [KeyboardButton(text="ℹ️ О бутике")],
            [KeyboardButton(text="⚙️ Управление")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите раздел...",
    )


def cancel_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True,
        input_field_placeholder="Можно отменить действие...",
    )


def phone_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Отправить номер", request_contact=True)],
            [KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Введите номер или отправьте контакт...",
    )


# =========================
# INLINE KEYBOARDS
# =========================
def categories_kb() -> InlineKeyboardMarkup:
    rows = []
    for category in get_categories():
        rows.append([InlineKeyboardButton(text=f"📂 {category}", callback_data=f"cat:{category}")])

    rows.append(
        [
            InlineKeyboardButton(text="🔥 Хиты", callback_data="section:hits"),
            InlineKeyboardButton(text="🆕 Новинки", callback_data="section:new"),
        ]
    )
    rows.append([InlineKeyboardButton(text="🧺 Корзина", callback_data="cart:open")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def products_kb(items) -> InlineKeyboardMarkup:
    rows = []
    for item in items:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{item['title']} • {money(int(item['price']))}",
                    callback_data=f"product:{int(item['id'])}",
                )
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(text="🛍 Каталог", callback_data="catalog:open"),
            InlineKeyboardButton(text="🧺 Корзина", callback_data="cart:open"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def product_kb(product_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🛒 В корзину", callback_data=f"add:{product_id}"),
                InlineKeyboardButton(text="🧺 Корзина", callback_data="cart:open"),
            ],
            [
                InlineKeyboardButton(text="🛍 Каталог", callback_data="catalog:open"),
                InlineKeyboardButton(text="💬 Менеджер", url=manager_url()),
            ],
        ]
    )


def cart_kb(items) -> InlineKeyboardMarkup:
    rows = []
    for item in items:
        product_id = int(item["id"])
        rows.append(
            [
                InlineKeyboardButton(text="➖", callback_data=f"cart:minus:{product_id}"),
                InlineKeyboardButton(
                    text=f"{short_text(str(item['title']))} × {int(item['quantity'])}",
                    callback_data="noop",
                ),
                InlineKeyboardButton(text="➕", callback_data=f"cart:plus:{product_id}"),
            ]
        )
        rows.append([InlineKeyboardButton(text="🗑 Удалить", callback_data=f"cart:del:{product_id}")])

    rows.append(
        [
            InlineKeyboardButton(text="✅ Оформить", callback_data="checkout:start"),
            InlineKeyboardButton(text="🧹 Очистить", callback_data="cart:clear"),
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(text="🛍 Каталог", callback_data="catalog:open"),
            InlineKeyboardButton(text="💬 Менеджер", url=manager_url()),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats"),
                InlineKeyboardButton(text="🧾 Последние заказы", callback_data="admin:orders"),
            ]
        ]
    )


# =========================
# SEND HELPERS
# =========================
async def send_product_message(target: Message, product: sqlite3.Row) -> None:
    caption = product_caption(product)
    photo = str(product["photo"]).strip()

    if photo.startswith("http://") or photo.startswith("https://"):
        await target.answer_photo(
            photo=photo,
            caption=caption,
            reply_markup=product_kb(int(product["id"])),
        )
    else:
        await target.answer(
            caption,
            reply_markup=product_kb(int(product["id"])),
        )


# =========================
# COMMAND HANDLERS
# =========================
async def start_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer(
        f"👋 Добро пожаловать в <b>{SHOP_NAME}</b>

{SHOP_TAGLINE}",
        reply_markup=main_menu(),
    )


async def menu_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Главное меню", reply_markup=main_menu())


async def help_handler(message: Message) -> None:
    await message.answer(
        "/start — открыть магазин
/menu — главное меню
/help — помощь",
        reply_markup=main_menu(),
    )


async def cancel_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Действие отменено.", reply_markup=main_menu())


# =========================
# SEARCH FLOW
# =========================
async def search_start(message: Message, state: FSMContext) -> None:
    await state.set_state(SearchState.waiting_query)
    await message.answer(
        "🔎 Напишите название товара или ключевое слово для поиска.",
        reply_markup=cancel_menu(),
    )


async def search_input(message: Message, state: FSMContext) -> None:
    query = (message.text or "").strip()
    items = get_products(search=query)
    await state.clear()

    if not items:
        await message.answer("Ничего не найдено. Попробуйте другой запрос.", reply_markup=main_menu())
        return

    await message.answer(
        products_text("🔎 Результаты поиска", items),
        reply_markup=main_menu(),
    )
    await message.answer("Открыть товары:", reply_markup=products_kb(items))


# =========================
# CHECKOUT FLOW
# =========================
async def checkout_start_from_message(message: Message, state: FSMContext) -> None:
    if not cart_items(message.from_user.id):
        await message.answer("Сначала добавьте товары в корзину.", reply_markup=main_menu())
        return

    await state.set_state(CheckoutState.waiting_name)
    await message.answer(
        "✅ <b>Оформление заявки</b>

Введите ваше имя:",
        reply_markup=cancel_menu(),
    )


async def checkout_name(message: Message, state: FSMContext) -> None:
    name = (message.text or "").strip()
    if len(name) < 2:
        await message.answer("Введите имя минимум из 2 символов.")
        return

    await state.update_data(customer_name=name)
    await state.set_state(CheckoutState.waiting_phone)
    await message.answer(
        "Введите номер телефона или нажмите кнопку ниже.",
        reply_markup=phone_menu(),
    )


async def checkout_phone_text(message: Message, state: FSMContext) -> None:
    phone = (message.text or "").strip()
    if len(phone) < 6:
        await message.answer("Введите корректный номер телефона.")
        return

    await state.update_data(customer_phone=phone)
    await state.set_state(CheckoutState.waiting_address)
    await message.answer("Введите город и адрес доставки:", reply_markup=cancel_menu())


async def checkout_phone_contact(message: Message, state: FSMContext) -> None:
    if not message.contact:
        return

    await state.update_data(customer_phone=message.contact.phone_number)
    await state.set_state(CheckoutState.waiting_address)
    await message.answer("Введите город и адрес доставки:", reply_markup=cancel_menu())


async def checkout_address(message: Message, state: FSMContext) -> None:
    address = (message.text or "").strip()
    if len(address) < 5:
        await message.answer("Введите более полный адрес.")
        return

    await state.update_data(customer_address=address)
    await state.set_state(CheckoutState.waiting_comment)
    await message.answer(
        "Напишите комментарий к заказу или отправьте <b>нет</b>.",
        reply_markup=cancel_menu(),
    )


async def checkout_comment(message: Message, state: FSMContext, bot: Bot) -> None:
    comment = (message.text or "").strip()
    if comment.lower() == "нет":
        comment = "Без комментария"

    data = await state.get_data()
    order = create_order(
        user_id=message.from_user.id,
        name=str(data.get("customer_name", "-")),
        phone=str(data.get("customer_phone", "-")),
        address=str(data.get("customer_address", "-")),
        comment=comment,
    )
    await state.clear()

    admin_text = (
        "🛍 <b>Новая заявка</b>

"
        f"🧾 Заказ: <b>#{order['order_id']}</b>
"
        f"⏰ Время: {escape_text(order['created_at'])}
"
        f"👤 Клиент: {user_tag(message.from_user)}
"
        f"Имя: {escape_text(data.get('customer_name', '-'))}
"
        f"Телефон: {escape_text(data.get('customer_phone', '-'))}
"
        f"Адрес: {escape_text(data.get('customer_address', '-'))}
"
        f"Комментарий: {escape_text(comment)}

"
        f"<b>Товары:</b>
{order_items_text(order['items'])}

"
        f"💰 Итого: <b>{money(int(order['total']))}</b>"
    )

    try:
        await bot.send_message(ADMIN_ID, admin_text)
    except Exception as exc:
        logging.exception("Cannot send order to admin: %s", exc)
        await message.answer(
            "Заявка сохранена, но не отправилась админу. Пусть админ сначала напишет боту /start.",
            reply_markup=main_menu(),
        )
        return

    await message.answer(
        f"🎉 Заявка <b>#{order['order_id']}</b> отправлена.",
        reply_markup=main_menu(),
    )


# =========================
# TEXT MENU HANDLER
# =========================
async def text_menu(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()

    if text == "🛍 Коллекция":
        await state.clear()
        await message.answer("Категории каталога:", reply_markup=categories_kb())
        return

    if text == "🔥 Хиты":
        await state.clear()
        items = get_products(section="hits")
        await message.answer(products_text("🔥 Хиты бутика", items), reply_markup=main_menu())
        await message.answer("Открыть товары:", reply_markup=products_kb(items))
        return

    if text == "🆕 Новинки":
        await state.clear()
        items = get_products(section="new")
        await message.answer(products_text("🆕 Новинки", items), reply_markup=main_menu())
        await message.answer("Открыть товары:", reply_markup=products_kb(items))
        return

    if text == "🔎 Поиск":
        await search_start(message, state)
        return

    if text == "🧺 Корзина":
        await state.clear()
        items = cart_items(message.from_user.id)
        await message.answer(cart_text(message.from_user.id), reply_markup=main_menu())
        if items:
            await message.answer("Управление корзиной:", reply_markup=cart_kb(items))
        return

    if text == "✅ Оформить заявку":
        await checkout_start_from_message(message, state)
        return

    if text == "💬 Менеджер":
        await state.clear()
        await message.answer(
            f"💬 <b>Менеджер</b>

"
            f"Имя: {escape_text(MANAGER_NAME)}
"
            f"Username: @{escape_text(MANAGER_USERNAME)}
"
            f"Телефон: {escape_text(SHOP_PHONE)}",
            reply_markup=main_menu(),
        )
        await message.answer(
            "Быстрая связь:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="Написать менеджеру", url=manager_url())]]
            ),
        )
        return

    if text == "🚚 Доставка":
        await state.clear()
        await message.answer(DELIVERY_TEXT, reply_markup=main_menu())
        return

    if text == "⭐ Отзывы":
        await state.clear()
        await message.answer(REVIEWS_TEXT, reply_markup=main_menu())
        return

    if text == "ℹ️ О бутике":
        await state.clear()
        await message.answer(ABOUT_TEXT, reply_markup=main_menu())
        return

    if text == "⚙️ Управление":
        await state.clear()
        if message.from_user.id != ADMIN_ID:
            await message.answer("⛔ Этот раздел доступен только админу.", reply_markup=main_menu())
            return

        stats = get_stats()
        await message.answer(
            "⚙️ <b>Панель управления</b>

"
            f"Товаров: <b>{stats['products']}</b>
"
            f"Заказов: <b>{stats['orders']}</b>
"
            f"Выручка: <b>{money(stats['revenue'])}</b>",
            reply_markup=main_menu(),
        )
        await message.answer("Админ-инструменты:", reply_markup=admin_kb())
        return

    if text.isdigit():
        product = get_product(int(text))
        if product:
            await state.clear()
            await send_product_message(message, product)
            return

    await state.clear()
    await message.answer("Выберите нужный раздел через меню.", reply_markup=main_menu())


# =========================
# CALLBACK HANDLERS
# =========================
async def callback_catalog(callback: CallbackQuery) -> None:
    await callback.answer()
    if callback.message:
        await callback.message.answer("Категории каталога:", reply_markup=categories_kb())


async def callback_section(callback: CallbackQuery) -> None:
    await callback.answer()
    section = callback.data.split(":", 1)[1]
    title = "🔥 Хиты бутика" if section == "hits" else "🆕 Новинки"
    items = get_products(section=section)
    if callback.message:
        await callback.message.answer(products_text(title, items))
        await callback.message.answer("Открыть товары:", reply_markup=products_kb(items))


async def callback_category(callback: CallbackQuery) -> None:
    await callback.answer()
    category = callback.data.split(":", 1)[1]
    items = get_products(category=category)
    if callback.message:
        await callback.message.answer(products_text(f"📂 {category}", items))
        await callback.message.answer("Открыть товары:", reply_markup=products_kb(items))


async def callback_product(callback: CallbackQuery) -> None:
    await callback.answer()
    product_id = int(callback.data.split(":", 1)[1])
    product = get_product(product_id)
    if not product:
        if callback.message:
            await callback.message.answer("Товар не найден.")
        return

    if callback.message:
        await send_product_message(callback.message, product)


async def callback_add(callback: CallbackQuery) -> None:
    await callback.answer("Товар добавлен в корзину")
    product_id = int(callback.data.split(":", 1)[1])
    product = get_product(product_id)
    if not product:
        return

    cart_add(callback.from_user.id, product_id, 1)
    if callback.message:
        await callback.message.answer(
            f"✅ <b>{escape_text(product['title'])}</b> добавлен в корзину.
"
            f"Сейчас в корзине: <b>{cart_count(callback.from_user.id)}</b> шт."
        )


async def callback_cart_open(callback: CallbackQuery) -> None:
    await callback.answer()
    items = cart_items(callback.from_user.id)
    if callback.message:
        if not items:
            await callback.message.answer(cart_text(callback.from_user.id))
        else:
            await callback.message.answer(cart_text(callback.from_user.id), reply_markup=cart_kb(items))


async def callback_cart_edit(callback: CallbackQuery) -> None:
    await callback.answer()
    _, action, product_id_str = callback.data.split(":")
    product_id = int(product_id_str)

    if action == "plus":
        cart_change(callback.from_user.id, product_id, 1)
    elif action == "minus":
        cart_change(callback.from_user.id, product_id, -1)
    elif action == "del":
        cart_remove(callback.from_user.id, product_id)

    items = cart_items(callback.from_user.id)
    if callback.message:
        if not items:
            await callback.message.edit_text(cart_text(callback.from_user.id))
        else:
            await callback.message.edit_text(
                cart_text(callback.from_user.id),
                reply_markup=cart_kb(items),
            )


async def callback_cart_clear(callback: CallbackQuery) -> None:
    await callback.answer("Корзина очищена")
    cart_clear(callback.from_user.id)
    if callback.message:
        await callback.message.edit_text(cart_text(callback.from_user.id))


async def callback_checkout_start(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    if not cart_items(callback.from_user.id):
        if callback.message:
            await callback.message.answer("Корзина пуста.")
        return

    await state.set_state(CheckoutState.waiting_name)
    if callback.message:
        await callback.message.answer(
            "✅ <b>Оформление заявки</b>

Введите ваше имя:",
            reply_markup=cancel_menu(),
        )


async def callback_admin_stats(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа", show_alert=True)
        return

    await callback.answer()
    stats = get_stats()
    if callback.message:
        await callback.message.answer(
            "📊 <b>Статистика магазина</b>

"
            f"Товаров: <b>{stats['products']}</b>
"
            f"Заказов: <b>{stats['orders']}</b>
"
            f"Выручка: <b>{money(stats['revenue'])}</b>"
        )


async def callback_admin_orders(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа", show_alert=True)
        return

    await callback.answer()
    orders = get_recent_orders()
    if not callback.message:
        return

    if not orders:
        await callback.message.answer("Заказов пока нет.")
        return

    lines = ["🧾 <b>Последние заказы</b>", ""]
    for order in orders:
        lines.append(
            f"#{order['id']} • {escape_text(order['created_at'])}
"
            f"{escape_text(order['customer_name'])} • {money(int(order['total']))}"
        )
        lines.append("")

    await callback.message.answer("
".join(lines))


async def noop_handler(callback: CallbackQuery) -> None:
    await callback.answer()


# =========================
# MAIN
# =========================
async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    init_db()

    if BOT_TOKEN == "PASTE_BOT_TOKEN_HERE":
        raise ValueError("Укажите BOT_TOKEN в переменных окружения или прямо в коде.")

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())

    dp.message.register(start_handler, CommandStart())
    dp.message.register(menu_handler, Command("menu"))
    dp.message.register(help_handler, Command("help"))

    dp.message.register(cancel_handler, F.text == "❌ Отмена")

    dp.message.register(search_input, SearchState.waiting_query)

    dp.message.register(checkout_name, CheckoutState.waiting_name)
    dp.message.register(checkout_phone_contact, CheckoutState.waiting_phone, F.contact)
    dp.message.register(checkout_phone_text, CheckoutState.waiting_phone, F.text)
    dp.message.register(checkout_address, CheckoutState.waiting_address)
    dp.message.register(checkout_comment, CheckoutState.waiting_comment)

    dp.callback_query.register(noop_handler, F.data == "noop")
    dp.callback_query.register(callback_catalog, F.data == "catalog:open")
    dp.callback_query.register(callback_section, F.data.startswith("section:"))
    dp.callback_query.register(callback_category, F.data.startswith("cat:"))
    dp.callback_query.register(callback_product, F.data.startswith("product:"))
    dp.callback_query.register(callback_add, F.data.startswith("add:"))
    dp.callback_query.register(callback_cart_open, F.data == "cart:open")
    dp.callback_query.register(callback_cart_edit, F.data.startswith("cart:plus:"))
    dp.callback_query.register(callback_cart_edit, F.data.startswith("cart:minus:"))
    dp.callback_query.register(callback_cart_edit, F.data.startswith("cart:del:"))
    dp.callback_query.register(callback_cart_clear, F.data == "cart:clear")
    dp.callback_query.register(callback_checkout_start, F.data == "checkout:start")
    dp.callback_query.register(callback_admin_stats, F.data == "admin:stats")
    dp.callback_query.register(callback_admin_orders, F.data == "admin:orders")

    dp.message.register(text_menu, F.text)

    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped")
