
import asyncio
import csv
import logging
import os
import re
import sqlite3
import tempfile
import time
from contextlib import closing
from datetime import datetime, timedelta
from html import escape
from pathlib import Path
from typing import Iterable, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "6954213997").split(",")
    if x.strip().isdigit()
}
MAIN_ADMIN_ID = 6954213997
ADMIN_IDS.add(MAIN_ADMIN_ID)

SHOP_NAME = os.getenv("SHOP_NAME", "ShopBron")
SHOP_TAGLINE = os.getenv("SHOP_TAGLINE", "Премиальный магазин прямо в Telegram")
SHOP_PHONE = os.getenv("SHOP_PHONE", "+7 700 000 00 00")
MANAGER_NAME = os.getenv("MANAGER_NAME", "Персональный менеджер")
MANAGER_USERNAME = os.getenv("MANAGER_USERNAME", "shopbron_manager").replace("@", "").strip()
CURRENCY = os.getenv("CURRENCY", "₸")
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = os.getenv("DB_PATH", str(BASE_DIR / "shop_store_v2.db"))
LOG_PATH = os.getenv("LOG_PATH", str(BASE_DIR / "shop_store.log"))
RESERVATION_HOURS = int(os.getenv("RESERVATION_HOURS", "24"))
SPAM_INTERVAL = float(os.getenv("SPAM_INTERVAL", "0.35"))
SHOP_CITY = os.getenv("SHOP_CITY", "Ваш город")

DELIVERY_METHODS = {
    "pickup": "Самовывоз",
    "courier_city": "Курьер по городу",
    "courier_kz": "Доставка по Казахстану",
}

PAYMENT_METHODS = {
    "cash": "Наличными",
    "card": "Картой",
    "transfer": "Переводом",
}

ORDER_STATUSES = {
    "new": "Новая",
    "confirmed": "Подтверждена",
    "cancelled": "Отменена",
    "issued": "Выдана",
}

ENTITY_LABEL = {
    "order": "заказ",
    "reservation": "бронь",
}

PHONE_RE = re.compile(r"^\+?[0-9 ()\-]{6,20}$")
LAST_ACTION_TS: dict[tuple[int, str], float] = {}

DELIVERY_TEXT = f"""🚚 <b>Доставка</b>

• Самовывоз из магазина
• По городу {SHOP_CITY}: в день заказа или на следующий день
• По Казахстану: 2–5 рабочих дней
• После оформления менеджер свяжется с вами
• При брони товар резервируется на {RESERVATION_HOURS} часа(ов)"""

ABOUT_TEXT = f"""ℹ️ <b>{SHOP_NAME}</b>

{SHOP_TAGLINE}

📞 Телефон: {SHOP_PHONE}
💬 Менеджер: @{MANAGER_USERNAME if MANAGER_USERNAME else MAIN_ADMIN_ID}
👤 Главный админ: <code>{MAIN_ADMIN_ID}</code>"""

DEMO_PRODUCTS = [
    {
        "title": "Смарт-часы FitTime Pro",
        "price": 31990,
        "category": "Гаджеты",
        "description": "Стильные смарт-часы с уведомлениями, шагомером и премиальным дизайном.",
        "photo": "",
        "is_hit": 1,
        "is_new": 0,
        "stock": 8,
    },
    {
        "title": "Наушники AirSound X1",
        "price": 24990,
        "category": "Аудио",
        "description": "Беспроводные наушники с глубоким звуком и шумоподавлением.",
        "photo": "",
        "is_hit": 1,
        "is_new": 0,
        "stock": 14,
    },
    {
        "title": "Портативная колонка BeatBox Mini",
        "price": 17990,
        "category": "Аудио",
        "description": "Компактная колонка с мощным звуком и хорошей автономностью.",
        "photo": "",
        "is_hit": 1,
        "is_new": 0,
        "stock": 5,
    },
    {
        "title": "Рюкзак Urban Move",
        "price": 22990,
        "category": "Аксессуары",
        "description": "Городской рюкзак с защитой от влаги и карманом для ноутбука.",
        "photo": "",
        "is_hit": 0,
        "is_new": 1,
        "stock": 9,
    },
    {
        "title": "Power Bank VoltMax 20000",
        "price": 19990,
        "category": "Гаджеты",
        "description": "Ёмкий внешний аккумулятор с быстрой зарядкой.",
        "photo": "",
        "is_hit": 0,
        "is_new": 1,
        "stock": 12,
    },
    {
        "title": "Термокружка Steel Heat",
        "price": 8990,
        "category": "Аксессуары",
        "description": "Держит тепло до 6 часов. Отличный вариант для города и авто.",
        "photo": "",
        "is_hit": 0,
        "is_new": 1,
        "stock": 18,
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
    waiting_delivery = State()
    waiting_payment = State()
    waiting_address = State()
    waiting_comment = State()


class AdminProductAddState(StatesGroup):
    waiting_title = State()
    waiting_price = State()
    waiting_category = State()
    waiting_description = State()
    waiting_photo = State()
    waiting_stock = State()
    waiting_flags = State()


class AdminProductEditState(StatesGroup):
    waiting_value = State()


class AdminOrderSearchState(StatesGroup):
    waiting_query = State()


# =========================
# DATABASE
# =========================
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def migrate_legacy_products(conn: sqlite3.Connection) -> None:
    """Backfill stock/timestamps for databases created by the old bot version."""
    title_to_stock = {item["title"]: int(item["stock"]) for item in DEMO_PRODUCTS}
    now = now_str()

    rows = conn.execute(
        "SELECT id, title, stock, created_at, updated_at FROM products"
    ).fetchall()

    for row in rows:
        updates: list[str] = []
        params: list[object] = []

        current_stock = int(row["stock"] or 0)
        if current_stock <= 0 and row["title"] in title_to_stock:
            updates.append("stock = ?")
            params.append(title_to_stock[row["title"]])

        if not str(row["created_at"] or "").strip():
            updates.append("created_at = ?")
            params.append(now)

        if not str(row["updated_at"] or "").strip():
            updates.append("updated_at = ?")
            params.append(now)

        if updates:
            params.append(int(row["id"]))
            conn.execute(
                f"UPDATE products SET {', '.join(updates)} WHERE id = ?",
                params,
            )


def init_db() -> None:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    with closing(db()) as conn:
        conn.execute("PRAGMA foreign_keys = ON")

        conn.execute(
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
                stock INTEGER NOT NULL DEFAULT 0,
                active INTEGER DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS carts (
                user_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (user_id, product_id)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS favorites (
                user_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (user_id, product_id)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS customers (
                user_id INTEGER PRIMARY KEY,
                full_name TEXT DEFAULT '',
                username TEXT DEFAULT '',
                phone TEXT DEFAULT '',
                updated_at TEXT NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                type TEXT NOT NULL DEFAULT 'order',
                status TEXT NOT NULL DEFAULT 'new',
                customer_name TEXT NOT NULL DEFAULT '',
                phone TEXT NOT NULL DEFAULT '',
                delivery_method TEXT NOT NULL DEFAULT '',
                payment_method TEXT NOT NULL DEFAULT '',
                address TEXT NOT NULL DEFAULT '',
                comment TEXT NOT NULL DEFAULT '',
                total INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                expires_at TEXT DEFAULT ''
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS order_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                price INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
            )
            """
        )

        ensure_column(conn, "products", "stock", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "products", "created_at", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "products", "updated_at", "TEXT NOT NULL DEFAULT ''")

        ensure_column(conn, "orders", "type", "TEXT NOT NULL DEFAULT 'order'")
        ensure_column(conn, "orders", "status", "TEXT NOT NULL DEFAULT 'new'")
        ensure_column(conn, "orders", "delivery_method", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "orders", "payment_method", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "orders", "updated_at", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "orders", "expires_at", "TEXT DEFAULT ''")

        conn.execute("CREATE INDEX IF NOT EXISTS idx_products_active ON products(active, category)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status, type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id)")

        migrate_legacy_products(conn)

        now = now_str()
        count = conn.execute("SELECT COUNT(*) AS c FROM products").fetchone()["c"]
        if count == 0:
            conn.executemany(
                """
                INSERT INTO products (
                    title, price, category, description, photo, is_hit, is_new, stock, active, created_at, updated_at
                )
                VALUES (:title, :price, :category, :description, :photo, :is_hit, :is_new, :stock, 1, :created_at, :updated_at)
                """,
                [{**item, "created_at": now, "updated_at": now} for item in DEMO_PRODUCTS],
            )

        conn.commit()


# =========================
# HELPERS
# =========================
def configure_logging() -> None:
    Path(LOG_PATH).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def now_dt() -> datetime:
    return datetime.now()


def now_str() -> str:
    return now_dt().strftime("%Y-%m-%d %H:%M:%S")


def human_dt(value: str) -> str:
    if not value:
        return "-"
    try:
        dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d.%m.%Y %H:%M")
    except ValueError:
        return value


def money(value: int) -> str:
    return f"{value:,}".replace(",", " ") + f" {CURRENCY}"


def short_text(text: str, limit: int = 22) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def escape_text(value: object) -> str:
    return escape(str(value))


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def manager_url() -> str:
    if MANAGER_USERNAME:
        return f"https://t.me/{MANAGER_USERNAME}"
    return f"tg://user?id={MAIN_ADMIN_ID}"


def manager_display() -> str:
    return f"@{MANAGER_USERNAME}" if MANAGER_USERNAME else f"ID {MAIN_ADMIN_ID}"


def admin_recipient_ids() -> list[int]:
    ids = {MAIN_ADMIN_ID}
    ids.update({admin_id for admin_id in ADMIN_IDS if isinstance(admin_id, int) and admin_id > 0})
    return sorted(ids)


def throttle_ok(user_id: int, key: str) -> bool:
    now = time.time()
    full_key = (user_id, key)
    last = LAST_ACTION_TS.get(full_key, 0.0)
    if now - last < SPAM_INTERVAL:
        return False
    LAST_ACTION_TS[full_key] = now
    return True


def parse_bool_flag(value: str) -> bool:
    return value.strip().lower() in {"1", "да", "yes", "y", "true", "хит", "новинка", "on"}


def normalize_phone(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


def validate_phone(value: str) -> bool:
    return bool(PHONE_RE.match(normalize_phone(value)))


def status_label(status: str) -> str:
    return ORDER_STATUSES.get(status, status)


def order_type_label(order_type: str) -> str:
    return "Заказ" if order_type == "order" else "Бронь"


def available_stock(product_id: int) -> int:
    with closing(db()) as conn:
        row = conn.execute(
            "SELECT stock FROM products WHERE id = ? AND active = 1",
            (product_id,),
        ).fetchone()
    return int(row["stock"]) if row else 0


def get_categories() -> list[str]:
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT DISTINCT category FROM products WHERE active = 1 ORDER BY category"
        ).fetchall()
    return [row["category"] for row in rows]


def get_products(
    *,
    section: Optional[str] = None,
    category: Optional[str] = None,
    search: Optional[str] = None,
    only_available: bool = False,
):
    with closing(db()) as conn:
        query = "SELECT * FROM products WHERE active = 1"
        params: list[object] = []

        if section == "hits":
            query += " AND is_hit = 1"
        if section == "new":
            query += " AND is_new = 1"
        if category:
            query += " AND category = ?"
            params.append(category)
        if search:
            like = f"%{search.lower()}%"
            query += " AND (LOWER(title) LIKE ? OR LOWER(description) LIKE ? OR LOWER(category) LIKE ?)"
            params.extend([like, like, like])
        if only_available:
            query += " AND stock > 0"

        query += " ORDER BY id DESC"
        return conn.execute(query, params).fetchall()


def get_product(product_id: int):
    with closing(db()) as conn:
        return conn.execute(
            "SELECT * FROM products WHERE id = ? AND active = 1",
            (product_id,),
        ).fetchone()


def admin_get_product(product_id: int):
    with closing(db()) as conn:
        return conn.execute(
            "SELECT * FROM products WHERE id = ?",
            (product_id,),
        ).fetchone()


def upsert_customer(user_id: int, full_name: str, username: str, phone: str = "") -> None:
    with closing(db()) as conn:
        conn.execute(
            """
            INSERT INTO customers (user_id, full_name, username, phone, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                full_name=excluded.full_name,
                username=excluded.username,
                phone=CASE WHEN excluded.phone <> '' THEN excluded.phone ELSE customers.phone END,
                updated_at=excluded.updated_at
            """,
            (user_id, full_name, username, phone, now_str()),
        )
        conn.commit()


def get_customer_phone(user_id: int) -> str:
    with closing(db()) as conn:
        row = conn.execute("SELECT phone FROM customers WHERE user_id = ?", (user_id,)).fetchone()
    return row["phone"] if row and row["phone"] else ""


def is_favorite(user_id: int, product_id: int) -> bool:
    with closing(db()) as conn:
        row = conn.execute(
            "SELECT 1 FROM favorites WHERE user_id = ? AND product_id = ?",
            (user_id, product_id),
        ).fetchone()
    return row is not None


def toggle_favorite(user_id: int, product_id: int) -> bool:
    with closing(db()) as conn:
        row = conn.execute(
            "SELECT 1 FROM favorites WHERE user_id = ? AND product_id = ?",
            (user_id, product_id),
        ).fetchone()
        if row:
            conn.execute(
                "DELETE FROM favorites WHERE user_id = ? AND product_id = ?",
                (user_id, product_id),
            )
            conn.commit()
            return False

        conn.execute(
            "INSERT INTO favorites (user_id, product_id, created_at) VALUES (?, ?, ?)",
            (user_id, product_id, now_str()),
        )
        conn.commit()
        return True


def favorite_items(user_id: int):
    with closing(db()) as conn:
        return conn.execute(
            """
            SELECT p.*
            FROM favorites f
            JOIN products p ON p.id = f.product_id
            WHERE f.user_id = ? AND p.active = 1
            ORDER BY f.created_at DESC
            """,
            (user_id,),
        ).fetchall()


def cart_add(user_id: int, product_id: int, qty: int = 1) -> tuple[bool, str]:
    product = get_product(product_id)
    if not product:
        return False, "Товар не найден."

    with closing(db()) as conn:
        row = conn.execute(
            "SELECT quantity FROM carts WHERE user_id = ? AND product_id = ?",
            (user_id, product_id),
        ).fetchone()
        current_qty = int(row["quantity"]) if row else 0
        new_qty = current_qty + qty

        if new_qty <= 0:
            conn.execute(
                "DELETE FROM carts WHERE user_id = ? AND product_id = ?",
                (user_id, product_id),
            )
            conn.commit()
            return True, "Товар удалён из корзины."

        if new_qty > int(product["stock"]):
            return False, f"Недостаточно остатка. В наличии: {int(product['stock'])} шт."

        if row:
            conn.execute(
                "UPDATE carts SET quantity = ? WHERE user_id = ? AND product_id = ?",
                (new_qty, user_id, product_id),
            )
        else:
            conn.execute(
                "INSERT INTO carts (user_id, product_id, quantity) VALUES (?, ?, ?)",
                (user_id, product_id, qty),
            )
        conn.commit()
    return True, "Корзина обновлена."


def cart_change(user_id: int, product_id: int, delta: int) -> tuple[bool, str]:
    return cart_add(user_id, product_id, delta)


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
        return conn.execute(
            """
            SELECT p.id, p.title, p.price, p.category, p.description, p.photo,
                   p.is_hit, p.is_new, p.stock, c.quantity
            FROM carts c
            JOIN products p ON p.id = c.product_id
            WHERE c.user_id = ? AND p.active = 1
            ORDER BY p.id DESC
            """,
            (user_id,),
        ).fetchall()


def cart_total(user_id: int) -> int:
    return sum(int(item["price"]) * int(item["quantity"]) for item in cart_items(user_id))


def cart_count(user_id: int) -> int:
    return sum(int(item["quantity"]) for item in cart_items(user_id))


def validate_cart_stock(user_id: int) -> tuple[bool, str]:
    items = cart_items(user_id)
    if not items:
        return False, "Корзина пуста."
    for item in items:
        if int(item["stock"]) < int(item["quantity"]):
            return (
                False,
                f"Товар «{item['title']}» недоступен в нужном количестве. Остаток: {int(item['stock'])} шт.",
            )
    return True, ""


def change_stock(product_id: int, delta: int) -> None:
    with closing(db()) as conn:
        conn.execute(
            "UPDATE products SET stock = stock + ?, updated_at = ? WHERE id = ?",
            (delta, now_str(), product_id),
        )
        conn.commit()


def create_sale(
    *,
    sale_type: str,
    user_id: int,
    customer_name: str,
    phone: str,
    delivery_method: str,
    payment_method: str,
    address: str,
    comment: str,
) -> dict:
    items = cart_items(user_id)
    if not items:
        raise ValueError("Корзина пуста.")

    ok, error = validate_cart_stock(user_id)
    if not ok:
        raise ValueError(error)

    total = sum(int(item["price"]) * int(item["quantity"]) for item in items)
    created = now_str()
    expires_at = ""
    if sale_type == "reservation":
        expires_at = (now_dt() + timedelta(hours=RESERVATION_HOURS)).strftime("%Y-%m-%d %H:%M:%S")

    with closing(db()) as conn:
        conn.execute("BEGIN")
        try:
            for item in items:
                current_stock_row = conn.execute(
                    "SELECT stock FROM products WHERE id = ?",
                    (int(item["id"]),),
                ).fetchone()
                current_stock = int(current_stock_row["stock"]) if current_stock_row else 0
                if current_stock < int(item["quantity"]):
                    raise ValueError(
                        f"Товар «{item['title']}» недоступен в нужном количестве. Остаток: {current_stock} шт."
                    )

            cur = conn.execute(
                """
                INSERT INTO orders (
                    user_id, type, status, customer_name, phone, delivery_method,
                    payment_method, address, comment, total, created_at, updated_at, expires_at
                )
                VALUES (?, ?, 'new', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    sale_type,
                    customer_name,
                    phone,
                    delivery_method,
                    payment_method,
                    address,
                    comment,
                    total,
                    created,
                    created,
                    expires_at,
                ),
            )
            sale_id = cur.lastrowid

            for item in items:
                conn.execute(
                    """
                    INSERT INTO order_items (order_id, product_id, title, price, quantity)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        sale_id,
                        int(item["id"]),
                        str(item["title"]),
                        int(item["price"]),
                        int(item["quantity"]),
                    ),
                )
                conn.execute(
                    "UPDATE products SET stock = stock - ?, updated_at = ? WHERE id = ?",
                    (int(item["quantity"]), created, int(item["id"])),
                )

            conn.commit()
        except Exception:
            conn.rollback()
            raise

    cart_clear(user_id)

    return {
        "id": sale_id,
        "type": sale_type,
        "status": "new",
        "customer_name": customer_name,
        "phone": phone,
        "delivery_method": delivery_method,
        "payment_method": payment_method,
        "address": address,
        "comment": comment,
        "total": total,
        "created_at": created,
        "updated_at": created,
        "expires_at": expires_at,
        "items": items,
    }


def get_order(order_id: int):
    with closing(db()) as conn:
        return conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()


def get_order_items(order_id: int):
    with closing(db()) as conn:
        return conn.execute(
            "SELECT * FROM order_items WHERE order_id = ? ORDER BY id ASC",
            (order_id,),
        ).fetchall()


def get_user_orders(user_id: int, limit: int = 20):
    with closing(db()) as conn:
        return conn.execute(
            "SELECT * FROM orders WHERE user_id = ? ORDER BY id DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()


def get_recent_sales(limit: int = 10, sale_type: Optional[str] = None):
    with closing(db()) as conn:
        query = "SELECT * FROM orders"
        params: list[object] = []
        if sale_type:
            query += " WHERE type = ?"
            params.append(sale_type)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        return conn.execute(query, params).fetchall()


def admin_search_sales(query_text: str, limit: int = 20):
    like = f"%{query_text.lower()}%"
    with closing(db()) as conn:
        return conn.execute(
            """
            SELECT *
            FROM orders
            WHERE LOWER(customer_name) LIKE ?
               OR LOWER(phone) LIKE ?
               OR CAST(id AS TEXT) = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (like, like, query_text.strip(), limit),
        ).fetchall()


def restore_stock_for_sale(conn: sqlite3.Connection, order_id: int) -> None:
    items = conn.execute(
        "SELECT product_id, quantity FROM order_items WHERE order_id = ?",
        (order_id,),
    ).fetchall()
    for item in items:
        conn.execute(
            "UPDATE products SET stock = stock + ?, updated_at = ? WHERE id = ?",
            (int(item["quantity"]), now_str(), int(item["product_id"])),
        )


def update_sale_status(order_id: int, new_status: str) -> tuple[bool, str]:
    with closing(db()) as conn:
        row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        if not row:
            return False, "Запись не найдена."

        old_status = row["status"]
        if old_status == new_status:
            return True, "Статус уже установлен."

        if old_status == "cancelled":
            return False, "Отменённую запись повторно менять нельзя."

        conn.execute("BEGIN")
        try:
            if new_status == "cancelled":
                restore_stock_for_sale(conn, order_id)
            conn.execute(
                "UPDATE orders SET status = ?, updated_at = ? WHERE id = ?",
                (new_status, now_str(), order_id),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return True, "Статус обновлён."


def get_stats() -> dict[str, int]:
    with closing(db()) as conn:
        products = conn.execute("SELECT COUNT(*) AS c FROM products WHERE active = 1").fetchone()["c"]
        orders = conn.execute(
            "SELECT COUNT(*) AS c FROM orders WHERE type = 'order'"
        ).fetchone()["c"]
        reservations = conn.execute(
            "SELECT COUNT(*) AS c FROM orders WHERE type = 'reservation'"
        ).fetchone()["c"]
        revenue = conn.execute(
            "SELECT COALESCE(SUM(total), 0) AS s FROM orders WHERE type = 'order' AND status IN ('new', 'confirmed', 'issued')"
        ).fetchone()["s"]
        total_stock = conn.execute(
            "SELECT COALESCE(SUM(stock), 0) AS s FROM products WHERE active = 1"
        ).fetchone()["s"]
    return {
        "products": int(products),
        "orders": int(orders),
        "reservations": int(reservations),
        "revenue": int(revenue or 0),
        "stock": int(total_stock or 0),
    }


def add_product(data: dict[str, object]) -> int:
    created = now_str()
    with closing(db()) as conn:
        cur = conn.execute(
            """
            INSERT INTO products (
                title, price, category, description, photo,
                is_hit, is_new, stock, active, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (
                str(data["title"]),
                int(data["price"]),
                str(data["category"]),
                str(data["description"]),
                str(data["photo"]),
                int(data["is_hit"]),
                int(data["is_new"]),
                int(data["stock"]),
                created,
                created,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def update_product_field(product_id: int, field: str, value: object) -> None:
    allowed = {
        "title",
        "price",
        "category",
        "description",
        "photo",
        "stock",
        "is_hit",
        "is_new",
        "active",
    }
    if field not in allowed:
        raise ValueError("Недопустимое поле.")

    with closing(db()) as conn:
        conn.execute(
            f"UPDATE products SET {field} = ?, updated_at = ? WHERE id = ?",
            (value, now_str(), product_id),
        )
        conn.commit()


def product_caption(product: sqlite3.Row, *, favorite: bool = False) -> str:
    marks: list[str] = []
    if int(product["is_hit"]):
        marks.append("ХИТ")
    if int(product["is_new"]):
        marks.append("НОВИНКА")
    if int(product["stock"]) <= 0:
        marks.append("НЕТ В НАЛИЧИИ")
    badge = f" [{' • '.join(marks)}]" if marks else ""
    fav = "\n❤️ В избранном" if favorite else ""

    return (
        f"<b>{escape_text(product['title'])}</b>{badge}\n\n"
        f"💵 Цена: <b>{money(int(product['price']))}</b>\n"
        f"📦 Остаток: <b>{int(product['stock'])} шт.</b>\n"
        f"📂 Категория: {escape_text(product['category'])}{fav}\n\n"
        f"{escape_text(product['description'])}"
    )


def products_text(title: str, items) -> str:
    if not items:
        return f"<b>{escape_text(title)}</b>\n\nПока в этом разделе ничего нет."

    lines = [f"<b>{escape_text(title)}</b>", ""]
    for item in items:
        labels: list[str] = []
        if int(item["is_hit"]):
            labels.append("Хит")
        if int(item["is_new"]):
            labels.append("Новинка")
        if int(item["stock"]) <= 0:
            labels.append("Нет в наличии")
        suffix = f" • {', '.join(labels)}" if labels else ""
        lines.append(
            f"• #{int(item['id'])} {escape_text(item['title'])} — {money(int(item['price']))} • Остаток: {int(item['stock'])} шт.{suffix}"
        )

    lines.append("")
    lines.append("Нажмите на товар ниже, чтобы открыть карточку.")
    return "\n".join(lines)


def cart_text(user_id: int) -> str:
    items = cart_items(user_id)
    if not items:
        return "🧺 <b>Корзина пуста</b>\n\nДобавьте товары из каталога."

    lines = ["🧺 <b>Ваша корзина</b>", ""]
    for idx, item in enumerate(items, start=1):
        lines.append(
            f"{idx}. {escape_text(item['title'])} — {money(int(item['price']))} × {int(item['quantity'])} "
            f"(остаток: {int(item['stock'])})"
        )

    lines.append("")
    lines.append(f"Итого: <b>{money(cart_total(user_id))}</b>")
    lines.append(f"Позиций: <b>{cart_count(user_id)}</b>")
    lines.append("")
    lines.append("Можно оформить заказ или бронь на 24 часа.")
    return "\n".join(lines)


def order_items_text(items: Iterable[sqlite3.Row]) -> str:
    lines = []
    for idx, item in enumerate(items, start=1):
        qty = int(item["quantity"])
        price = int(item["price"])
        lines.append(
            f"{idx}. {escape_text(item['title'])} — {money(price)} × {qty} = {money(price * qty)}"
        )
    return "\n".join(lines)


def order_text(order: sqlite3.Row, items: Optional[list[sqlite3.Row]] = None) -> str:
    if items is None:
        items = list(get_order_items(int(order["id"])))

    extra = ""
    if order["type"] == "reservation" and order["expires_at"]:
        extra = f"\n⏳ До: <b>{human_dt(order['expires_at'])}</b>"

    delivery = DELIVERY_METHODS.get(order["delivery_method"], order["delivery_method"] or "-")
    payment = PAYMENT_METHODS.get(order["payment_method"], order["payment_method"] or "-")

    return (
        f"📦 <b>{order_type_label(order['type'])} #{int(order['id'])}</b>\n\n"
        f"Статус: <b>{status_label(order['status'])}</b>\n"
        f"Создано: {human_dt(order['created_at'])}\n"
        f"Обновлено: {human_dt(order['updated_at'])}{extra}\n"
        f"Имя: {escape_text(order['customer_name'])}\n"
        f"Телефон: {escape_text(order['phone'])}\n"
        f"Доставка: {escape_text(delivery)}\n"
        f"Оплата: {escape_text(payment)}\n"
        f"Адрес: {escape_text(order['address'] or '-')}\n"
        f"Комментарий: {escape_text(order['comment'] or '-')}\n\n"
        f"<b>Товары:</b>\n{order_items_text(items)}\n\n"
        f"💰 Итого: <b>{money(int(order['total']))}</b>"
    )


def product_admin_text(product: sqlite3.Row) -> str:
    return (
        f"🛠 <b>Товар #{int(product['id'])}</b>\n\n"
        f"Название: {escape_text(product['title'])}\n"
        f"Цена: {money(int(product['price']))}\n"
        f"Категория: {escape_text(product['category'])}\n"
        f"Остаток: {int(product['stock'])} шт.\n"
        f"Хит: {'Да' if int(product['is_hit']) else 'Нет'}\n"
        f"Новинка: {'Да' if int(product['is_new']) else 'Нет'}\n"
        f"Активен: {'Да' if int(product['active']) else 'Нет'}\n"
        f"Фото: {'Есть' if str(product['photo']).strip() else 'Нет'}\n\n"
        f"{escape_text(product['description'])}"
    )


async def safe_send_user_message(bot: Bot, user_id: int, text: str, **kwargs) -> None:
    try:
        await bot.send_message(user_id, text, **kwargs)
    except Exception as exc:
        logging.warning("Cannot notify user %s: %s", user_id, exc)


# =========================
# KEYBOARDS
# =========================
def main_menu(user_id: int) -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(text="🛍 Каталог"), KeyboardButton(text="🔥 Хиты")],
        [KeyboardButton(text="🆕 Новинки"), KeyboardButton(text="🔎 Поиск")],
        [KeyboardButton(text="❤️ Избранное"), KeyboardButton(text="🧺 Корзина")],
        [KeyboardButton(text="📦 Мои заказы и брони"), KeyboardButton(text="💬 Менеджер")],
        [KeyboardButton(text="🚚 Доставка"), KeyboardButton(text="ℹ️ О магазине")],
    ]
    if is_admin(user_id):
        keyboard.append([KeyboardButton(text="⚙️ Админ-панель")])
    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        input_field_placeholder="Выберите раздел…",
    )


def cancel_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True,
    )


def phone_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Отправить номер", request_contact=True)],
            [KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
    )


def categories_kb() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=f"📂 {cat}", callback_data=f"cat:{cat}")] for cat in get_categories()]
    rows.append(
        [
            InlineKeyboardButton(text="🔥 Хиты", callback_data="section:hits"),
            InlineKeyboardButton(text="🆕 Новинки", callback_data="section:new"),
        ]
    )
    rows.append([InlineKeyboardButton(text="🧺 Корзина", callback_data="cart:open")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def products_kb(items) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=f"{short_text(str(item['title']))} • {money(int(item['price']))}",
                callback_data=f"product:{int(item['id'])}",
            )
        ]
        for item in items
    ]
    rows.append(
        [
            InlineKeyboardButton(text="🛍 Каталог", callback_data="catalog:open"),
            InlineKeyboardButton(text="🧺 Корзина", callback_data="cart:open"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def product_kb(product_id: int, user_id: int) -> InlineKeyboardMarkup:
    fav_label = "💔 Убрать из избранного" if is_favorite(user_id, product_id) else "❤️ В избранное"
    product = get_product(product_id)
    stock = int(product["stock"]) if product else 0

    rows = []
    if stock > 0:
        rows.append(
            [
                InlineKeyboardButton(text="🛒 В корзину", callback_data=f"add:{product_id}"),
                InlineKeyboardButton(text="🧺 Корзина", callback_data="cart:open"),
            ]
        )
    else:
        rows.append([InlineKeyboardButton(text="🚫 Нет в наличии", callback_data="noop")])

    rows.append([InlineKeyboardButton(text=fav_label, callback_data=f"fav:{product_id}")])
    rows.append(
        [
            InlineKeyboardButton(text="🛍 Каталог", callback_data="catalog:open"),
            InlineKeyboardButton(text="💬 Менеджер", url=manager_url()),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def cart_kb(items) -> InlineKeyboardMarkup:
    rows = []
    for item in items:
        pid = int(item["id"])
        rows.append(
            [
                InlineKeyboardButton(text="➖", callback_data=f"cart:minus:{pid}"),
                InlineKeyboardButton(text=f"{short_text(str(item['title']))} × {int(item['quantity'])}", callback_data="noop"),
                InlineKeyboardButton(text="➕", callback_data=f"cart:plus:{pid}"),
            ]
        )
        rows.append([InlineKeyboardButton(text="🗑 Удалить", callback_data=f"cart:del:{pid}")])

    rows.append(
        [
            InlineKeyboardButton(text="✅ Оформить заказ", callback_data="checkout:order"),
            InlineKeyboardButton(text="📌 Оформить бронь", callback_data="checkout:reservation"),
        ]
    )
    rows.append([InlineKeyboardButton(text="🧹 Очистить", callback_data="cart:clear")])
    rows.append([InlineKeyboardButton(text="🛍 Каталог", callback_data="catalog:open")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def history_kb(items) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"{order_type_label(item['type'])} #{int(item['id'])} • {status_label(item['status'])}", callback_data=f"sale:view:{int(item['id'])}")]
        for item in items
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else InlineKeyboardMarkup(inline_keyboard=[])


def checkout_delivery_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=label, callback_data=f"delivery:{key}")]
            for key, label in DELIVERY_METHODS.items()
        ]
    )


def checkout_payment_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=label, callback_data=f"payment:{key}")]
            for key, label in PAYMENT_METHODS.items()
        ]
    )


def admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats"),
                InlineKeyboardButton(text="📦 Заказы", callback_data="admin:list:order"),
            ],
            [
                InlineKeyboardButton(text="📌 Брони", callback_data="admin:list:reservation"),
                InlineKeyboardButton(text="🔎 Поиск по заказам", callback_data="admin:search"),
            ],
            [
                InlineKeyboardButton(text="🛠 Товары", callback_data="admin:products"),
                InlineKeyboardButton(text="➕ Добавить товар", callback_data="admin:add_product"),
            ],
            [
                InlineKeyboardButton(text="📤 Экспорт CSV", callback_data="admin:export"),
                InlineKeyboardButton(text="💾 Бэкап БД", callback_data="admin:backup"),
            ],
        ]
    )


def admin_sale_actions_kb(order_id: int, status: str) -> InlineKeyboardMarkup:
    rows = []
    if status not in {"cancelled", "issued"}:
        rows.append(
            [
                InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin:status:{order_id}:confirmed"),
                InlineKeyboardButton(text="🚫 Отменить", callback_data=f"admin:status:{order_id}:cancelled"),
            ]
        )
        rows.append([InlineKeyboardButton(text="📦 Выдано/Закрыто", callback_data=f"admin:status:{order_id}:issued")])
    rows.append([InlineKeyboardButton(text="⬅️ К списку", callback_data="admin:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_products_kb(items) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"#{int(item['id'])} {short_text(str(item['title']))}", callback_data=f"admin:product:{int(item['id'])}")]
        for item in items
    ]
    rows.append([InlineKeyboardButton(text="➕ Добавить товар", callback_data="admin:add_product")])
    rows.append([InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_product_actions_kb(product_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✏️ Название", callback_data=f"admin:edit:{product_id}:title"),
                InlineKeyboardButton(text="💵 Цена", callback_data=f"admin:edit:{product_id}:price"),
            ],
            [
                InlineKeyboardButton(text="📂 Категория", callback_data=f"admin:edit:{product_id}:category"),
                InlineKeyboardButton(text="📝 Описание", callback_data=f"admin:edit:{product_id}:description"),
            ],
            [
                InlineKeyboardButton(text="🖼 Фото", callback_data=f"admin:edit:{product_id}:photo"),
                InlineKeyboardButton(text="📦 Остаток", callback_data=f"admin:edit:{product_id}:stock"),
            ],
            [
                InlineKeyboardButton(text="🔥 Хит", callback_data=f"admin:toggle:{product_id}:is_hit"),
                InlineKeyboardButton(text="🆕 Новинка", callback_data=f"admin:toggle:{product_id}:is_new"),
            ],
            [
                InlineKeyboardButton(text="👁 Активность", callback_data=f"admin:toggle:{product_id}:active"),
            ],
            [
                InlineKeyboardButton(text="⬅️ К товарам", callback_data="admin:products"),
            ],
        ]
    )


# =========================
# SEND HELPERS
# =========================
async def send_product_message(target: Message, product: sqlite3.Row, user_id: int) -> None:
    caption = product_caption(product, favorite=is_favorite(user_id, int(product["id"])))
    photo = str(product["photo"]).strip()

    if photo:
        try:
            await target.answer_photo(
                photo=photo,
                caption=caption,
                reply_markup=product_kb(int(product["id"]), user_id),
            )
            return
        except Exception as exc:
            logging.warning("Cannot send product photo %s: %s", product["id"], exc)

    await target.answer(
        caption,
        reply_markup=product_kb(int(product["id"]), user_id),
    )


# =========================
# BACKGROUND JOBS
# =========================
async def expire_reservations_job(bot: Bot) -> None:
    while True:
        try:
            expired = []
            with closing(db()) as conn:
                rows = conn.execute(
                    """
                    SELECT * FROM orders
                    WHERE type = 'reservation'
                      AND status = 'new'
                      AND expires_at <> ''
                      AND expires_at <= ?
                    ORDER BY id ASC
                    """,
                    (now_str(),),
                ).fetchall()

                if rows:
                    conn.execute("BEGIN")
                    try:
                        for row in rows:
                            restore_stock_for_sale(conn, int(row["id"]))
                            conn.execute(
                                "UPDATE orders SET status = 'cancelled', updated_at = ? WHERE id = ?",
                                (now_str(), int(row["id"])),
                            )
                            expired.append(dict(row))
                        conn.commit()
                    except Exception:
                        conn.rollback()
                        raise

            for row in expired:
                text = (
                    f"⌛ <b>Бронь #{row['id']}</b> автоматически отменена, потому что истёк срок {RESERVATION_HOURS} часа(ов)."
                )
                await safe_send_user_message(bot, int(row["user_id"]), text)
                for admin_id in admin_recipient_ids():
                    if admin_id != int(row["user_id"]):
                        await safe_send_user_message(bot, admin_id, f"⌛ Истекла бронь #{row['id']}. Остаток возвращён на склад.")
        except Exception as exc:
            logging.exception("Expire reservations job failed: %s", exc)

        await asyncio.sleep(60)


# =========================
# USER HANDLERS
# =========================
async def start_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    upsert_customer(
        message.from_user.id,
        message.from_user.full_name,
        message.from_user.username or "",
    )
    await message.answer(
        f"👋 Добро пожаловать в <b>{SHOP_NAME}</b>\n\n{SHOP_TAGLINE}",
        reply_markup=main_menu(message.from_user.id),
    )


async def menu_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Главное меню", reply_markup=main_menu(message.from_user.id))


async def help_handler(message: Message) -> None:
    await message.answer(
        "/start — открыть магазин\n"
        "/menu — главное меню\n"
        "/help — помощь",
        reply_markup=main_menu(message.from_user.id),
    )


async def cancel_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Действие отменено.", reply_markup=main_menu(message.from_user.id))


async def search_start(message: Message, state: FSMContext) -> None:
    await state.set_state(SearchState.waiting_query)
    await message.answer(
        "🔎 Напишите название товара, категорию или ключевое слово.",
        reply_markup=cancel_menu(),
    )


async def search_input(message: Message, state: FSMContext) -> None:
    query = (message.text or "").strip()
    items = get_products(search=query)
    await state.clear()

    if not items:
        await message.answer("Ничего не найдено. Попробуйте другой запрос.", reply_markup=main_menu(message.from_user.id))
        return

    await message.answer(products_text("🔎 Результаты поиска", items), reply_markup=main_menu(message.from_user.id))
    await message.answer("Открыть товары:", reply_markup=products_kb(items))


async def favorites_open(message: Message) -> None:
    items = favorite_items(message.from_user.id)
    if not items:
        await message.answer("❤️ Избранное пока пусто.", reply_markup=main_menu(message.from_user.id))
        return
    await message.answer(products_text("❤️ Избранное", items), reply_markup=main_menu(message.from_user.id))
    await message.answer("Открыть товары:", reply_markup=products_kb(items))


async def history_open(message: Message) -> None:
    items = get_user_orders(message.from_user.id)
    if not items:
        await message.answer("У вас пока нет заказов и броней.", reply_markup=main_menu(message.from_user.id))
        return

    lines = ["📦 <b>Мои заказы и брони</b>", ""]
    for item in items:
        line = f"{order_type_label(item['type'])} #{int(item['id'])} • {status_label(item['status'])} • {money(int(item['total']))}"
        if item["type"] == "reservation" and item["expires_at"] and item["status"] == "new":
            line += f" • до {human_dt(item['expires_at'])}"
        lines.append(line)

    await message.answer("\n".join(lines), reply_markup=main_menu(message.from_user.id))
    await message.answer("Открыть карточку:", reply_markup=history_kb(items))


async def checkout_start(message: Message, state: FSMContext, sale_type: str, user_id: Optional[int] = None) -> None:
    actual_user_id = int(user_id or (message.from_user.id if message.from_user else 0))
    ok, error = validate_cart_stock(actual_user_id)
    if not ok:
        await message.answer(error, reply_markup=main_menu(actual_user_id))
        return

    await state.clear()
    await state.update_data(sale_type=sale_type)
    await state.set_state(CheckoutState.waiting_name)
    await message.answer(
        f"✅ <b>{'Оформление заказа' if sale_type == 'order' else 'Оформление брони'}</b>\n\nВведите ваше имя:",
        reply_markup=cancel_menu(),
    )


async def checkout_name(message: Message, state: FSMContext) -> None:
    name = (message.text or "").strip()
    if len(name) < 2:
        await message.answer("Введите имя минимум из 2 символов.")
        return

    await state.update_data(customer_name=name)
    await state.set_state(CheckoutState.waiting_phone)

    saved_phone = get_customer_phone(message.from_user.id)
    text = "Введите номер телефона или нажмите кнопку ниже."
    if saved_phone:
        text += f"\n\nСохранённый номер: <code>{escape_text(saved_phone)}</code>"

    await message.answer(text, reply_markup=phone_menu())


async def checkout_phone_contact(message: Message, state: FSMContext) -> None:
    if not message.contact:
        return
    phone = normalize_phone(message.contact.phone_number)
    if not validate_phone(phone):
        await message.answer("Введите корректный номер телефона.")
        return

    upsert_customer(
        message.from_user.id,
        message.from_user.full_name,
        message.from_user.username or "",
        phone,
    )
    await state.update_data(customer_phone=phone)
    data = await state.get_data()
    if data.get("sale_type") == "reservation":
        await state.update_data(delivery_method="pickup", payment_method="pay_later", customer_address="Самовывоз")
        await state.set_state(CheckoutState.waiting_comment)
        await message.answer(
            "Напишите комментарий к брони или отправьте <b>нет</b>.",
            reply_markup=cancel_menu(),
        )
        return

    await state.set_state(CheckoutState.waiting_delivery)
    await message.answer("Выберите способ доставки:", reply_markup=checkout_delivery_kb())


async def checkout_phone_text(message: Message, state: FSMContext) -> None:
    phone = normalize_phone((message.text or "").strip())
    if not validate_phone(phone):
        await message.answer("Введите корректный номер телефона.")
        return

    upsert_customer(
        message.from_user.id,
        message.from_user.full_name,
        message.from_user.username or "",
        phone,
    )
    await state.update_data(customer_phone=phone)
    data = await state.get_data()
    if data.get("sale_type") == "reservation":
        await state.update_data(delivery_method="pickup", payment_method="pay_later", customer_address="Самовывоз")
        await state.set_state(CheckoutState.waiting_comment)
        await message.answer(
            "Напишите комментарий к брони или отправьте <b>нет</b>.",
            reply_markup=cancel_menu(),
        )
        return

    await state.set_state(CheckoutState.waiting_delivery)
    await message.answer("Выберите способ доставки:", reply_markup=checkout_delivery_kb())


async def checkout_delivery_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    method = callback.data.split(":", 1)[1]
    if method not in DELIVERY_METHODS:
        return
    await state.update_data(delivery_method=method)
    await state.set_state(CheckoutState.waiting_payment)
    if callback.message:
        await callback.message.answer("Выберите способ оплаты:", reply_markup=checkout_payment_kb())


async def checkout_payment_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    method = callback.data.split(":", 1)[1]
    if method not in PAYMENT_METHODS:
        return
    await state.update_data(payment_method=method)
    await state.set_state(CheckoutState.waiting_address)
    if callback.message:
        await callback.message.answer(
            "Введите город и адрес доставки.\nДля самовывоза можно написать: <b>Самовывоз</b>.",
            reply_markup=cancel_menu(),
        )


async def checkout_address(message: Message, state: FSMContext) -> None:
    address = (message.text or "").strip()
    if len(address) < 4:
        await message.answer("Введите более полный адрес.")
        return

    await state.update_data(customer_address=address)
    await state.set_state(CheckoutState.waiting_comment)
    await message.answer("Напишите комментарий к заказу или отправьте <b>нет</b>.", reply_markup=cancel_menu())


async def checkout_comment(message: Message, state: FSMContext, bot: Bot) -> None:
    comment = (message.text or "").strip()
    if comment.lower() == "нет":
        comment = "Без комментария"

    data = await state.get_data()
    sale_type = str(data.get("sale_type", "order"))

    try:
        sale = create_sale(
            sale_type=sale_type,
            user_id=message.from_user.id,
            customer_name=str(data.get("customer_name", "-")),
            phone=str(data.get("customer_phone", "-")),
            delivery_method=str(data.get("delivery_method", "pickup")),
            payment_method=str(data.get("payment_method", "pay_later")),
            address=str(data.get("customer_address", "Самовывоз")),
            comment=comment,
        )
    except ValueError as exc:
        await state.clear()
        await message.answer(str(exc), reply_markup=main_menu(message.from_user.id))
        return

    await state.clear()

    order_label = order_type_label(sale_type)
    expires = ""
    if sale_type == "reservation" and sale["expires_at"]:
        expires = f"\n⏳ Действует до: <b>{human_dt(str(sale['expires_at']))}</b>"

    user_text = (
        f"🎉 <b>{order_label} #{sale['id']}</b> создана.\n"
        f"Статус: <b>{status_label(sale['status'])}</b>{expires}\n"
        f"Сумма: <b>{money(int(sale['total']))}</b>\n\n"
        f"Менеджер свяжется с вами после проверки."
    )
    await message.answer(user_text, reply_markup=main_menu(message.from_user.id))

    admin_text = (
        f"🆕 <b>Новая {ENTITY_LABEL[sale_type]}</b>\n\n"
        f"{order_text(sqlite3.Row(dict, sale) if False else get_order(int(sale['id'])))}\n\n"
        f"Клиент TG ID: <code>{message.from_user.id}</code>\n"
        f"Username: @{escape_text(message.from_user.username or '-')}"
    )

    order_id = int(sale["id"])
    admin_markup = admin_sale_actions_kb(order_id, "new")
    for admin_id in admin_recipient_ids():
        try:
            await bot.send_message(admin_id, admin_text, reply_markup=admin_markup)
        except Exception as exc:
            logging.warning("Cannot notify admin %s: %s", admin_id, exc)


async def text_menu(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()

    if text == "🛍 Каталог":
        await state.clear()
        await message.answer("Категории каталога:", reply_markup=categories_kb())
        return

    if text == "🔥 Хиты":
        await state.clear()
        items = get_products(section="hits")
        await message.answer(products_text("🔥 Хиты", items), reply_markup=main_menu(message.from_user.id))
        await message.answer("Открыть товары:", reply_markup=products_kb(items))
        return

    if text == "🆕 Новинки":
        await state.clear()
        items = get_products(section="new")
        await message.answer(products_text("🆕 Новинки", items), reply_markup=main_menu(message.from_user.id))
        await message.answer("Открыть товары:", reply_markup=products_kb(items))
        return

    if text == "🔎 Поиск":
        await search_start(message, state)
        return

    if text == "❤️ Избранное":
        await state.clear()
        await favorites_open(message)
        return

    if text == "🧺 Корзина":
        await state.clear()
        items = cart_items(message.from_user.id)
        await message.answer(cart_text(message.from_user.id), reply_markup=main_menu(message.from_user.id))
        if items:
            await message.answer("Управление корзиной:", reply_markup=cart_kb(items))
        return

    if text == "📦 Мои заказы и брони":
        await state.clear()
        await history_open(message)
        return

    if text == "💬 Менеджер":
        await state.clear()
        await message.answer(
            f"💬 <b>Менеджер</b>\n\n"
            f"Имя: {escape_text(MANAGER_NAME)}\n"
            f"Контакт: {escape_text(manager_display())}\n"
            f"Телефон: {escape_text(SHOP_PHONE)}",
            reply_markup=main_menu(message.from_user.id),
        )
        return

    if text == "🚚 Доставка":
        await state.clear()
        await message.answer(DELIVERY_TEXT, reply_markup=main_menu(message.from_user.id))
        return

    if text == "ℹ️ О магазине":
        await state.clear()
        await message.answer(ABOUT_TEXT, reply_markup=main_menu(message.from_user.id))
        return

    if text == "⚙️ Админ-панель":
        await state.clear()
        if not is_admin(message.from_user.id):
            await message.answer("⛔ Этот раздел доступен только админу.", reply_markup=main_menu(message.from_user.id))
            return
        stats = get_stats()
        await message.answer(
            "⚙️ <b>Админ-панель</b>\n\n"
            f"Товаров: <b>{stats['products']}</b>\n"
            f"Заказов: <b>{stats['orders']}</b>\n"
            f"Броней: <b>{stats['reservations']}</b>\n"
            f"Остаток на складе: <b>{stats['stock']} шт.</b>\n"
            f"Оборот: <b>{money(stats['revenue'])}</b>\n"
            f"Админ ID: <code>{MAIN_ADMIN_ID}</code>",
            reply_markup=main_menu(message.from_user.id),
        )
        await message.answer("Инструменты:", reply_markup=admin_kb())
        return

    if text.isdigit():
        product = get_product(int(text))
        if product:
            await state.clear()
            await send_product_message(message, product, message.from_user.id)
            return

    await state.clear()
    await message.answer("Выберите нужный раздел через меню.", reply_markup=main_menu(message.from_user.id))


# =========================
# CALLBACKS: CATALOG AND CART
# =========================
async def callback_catalog(callback: CallbackQuery) -> None:
    await callback.answer()
    if callback.message:
        await callback.message.answer("Категории каталога:", reply_markup=categories_kb())


async def callback_section(callback: CallbackQuery) -> None:
    await callback.answer()
    section = callback.data.split(":", 1)[1]
    title = "🔥 Хиты" if section == "hits" else "🆕 Новинки"
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
        await send_product_message(callback.message, product, callback.from_user.id)


async def callback_add(callback: CallbackQuery) -> None:
    if not throttle_ok(callback.from_user.id, "cart_add"):
        await callback.answer("Слишком быстро", show_alert=False)
        return

    product_id = int(callback.data.split(":", 1)[1])
    ok, text = cart_add(callback.from_user.id, product_id, 1)
    await callback.answer("Добавлено" if ok else "Ошибка", show_alert=not ok)
    if callback.message:
        product = get_product(product_id)
        if product:
            count = cart_count(callback.from_user.id)
            await callback.message.answer(
                ("✅ " if ok else "⚠️ ") + escape_text(text) + (f"\nСейчас в корзине: <b>{count}</b> шт." if ok else "")
            )
            if ok:
                items = cart_items(callback.from_user.id)
                await callback.message.answer(
                    cart_text(callback.from_user.id),
                    reply_markup=cart_kb(items) if items else None,
                )


async def callback_favorite(callback: CallbackQuery) -> None:
    await callback.answer()
    product_id = int(callback.data.split(":", 1)[1])
    added = toggle_favorite(callback.from_user.id, product_id)
    if callback.message:
        product = get_product(product_id)
        if product:
            await callback.message.answer(
                f"{'❤️ Добавлено в избранное.' if added else '💔 Удалено из избранного.'}",
                reply_markup=product_kb(product_id, callback.from_user.id),
            )


async def callback_cart_open(callback: CallbackQuery) -> None:
    await callback.answer()
    items = cart_items(callback.from_user.id)
    if callback.message:
        await callback.message.answer(cart_text(callback.from_user.id))
        if items:
            await callback.message.answer("Управление корзиной:", reply_markup=cart_kb(items))


async def callback_cart_edit(callback: CallbackQuery) -> None:
    if not throttle_ok(callback.from_user.id, "cart_edit"):
        await callback.answer("Слишком быстро", show_alert=False)
        return

    await callback.answer()
    _, action, product_id_str = callback.data.split(":")
    product_id = int(product_id_str)

    if action == "plus":
        ok, text = cart_change(callback.from_user.id, product_id, 1)
    elif action == "minus":
        ok, text = cart_change(callback.from_user.id, product_id, -1)
    else:
        cart_remove(callback.from_user.id, product_id)
        ok, text = True, "Удалено."

    items = cart_items(callback.from_user.id)
    if callback.message:
        if not items:
            await callback.message.edit_text(cart_text(callback.from_user.id))
        else:
            await callback.message.edit_text(cart_text(callback.from_user.id), reply_markup=cart_kb(items))
        if text and action != "del":
            await callback.message.answer(text)


async def callback_cart_clear(callback: CallbackQuery) -> None:
    await callback.answer("Корзина очищена")
    cart_clear(callback.from_user.id)
    if callback.message:
        await callback.message.edit_text(cart_text(callback.from_user.id))


async def callback_checkout_start(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    sale_type = callback.data.split(":", 1)[1]
    if sale_type not in {"order", "reservation"}:
        return
    ok, error = validate_cart_stock(callback.from_user.id)
    if not ok:
        if callback.message:
            await callback.message.answer(error, reply_markup=main_menu(callback.from_user.id))
        return
    await state.clear()
    await state.update_data(sale_type=sale_type)
    await state.set_state(CheckoutState.waiting_name)
    if callback.message:
        await callback.message.answer(
            f"✅ <b>{'Оформление заказа' if sale_type == 'order' else 'Оформление брони'}</b>\n\nВведите ваше имя:",
            reply_markup=cancel_menu(),
        )


async def callback_sale_view(callback: CallbackQuery) -> None:
    await callback.answer()
    order_id = int(callback.data.split(":")[2])
    order = get_order(order_id)
    if not order or int(order["user_id"]) != callback.from_user.id:
        if callback.message:
            await callback.message.answer("Запись не найдена.")
        return
    if callback.message:
        await callback.message.answer(order_text(order))


async def noop_handler(callback: CallbackQuery) -> None:
    await callback.answer()


# =========================
# ADMIN HANDLERS
# =========================
async def admin_guard(callback: CallbackQuery) -> bool:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return False
    return True


async def callback_admin_back(callback: CallbackQuery) -> None:
    if not await admin_guard(callback):
        return
    await callback.answer()
    if callback.message:
        stats = get_stats()
        await callback.message.answer(
            "⚙️ <b>Админ-панель</b>\n\n"
            f"Товаров: <b>{stats['products']}</b>\n"
            f"Заказов: <b>{stats['orders']}</b>\n"
            f"Броней: <b>{stats['reservations']}</b>\n"
            f"Оборот: <b>{money(stats['revenue'])}</b>\n"
            f"Админ ID: <code>{MAIN_ADMIN_ID}</code>"
        )
        await callback.message.answer("Инструменты:", reply_markup=admin_kb())


async def callback_admin_stats(callback: CallbackQuery) -> None:
    if not await admin_guard(callback):
        return
    await callback.answer()
    stats = get_stats()
    if callback.message:
        await callback.message.answer(
            "📊 <b>Статистика магазина</b>\n\n"
            f"Товаров: <b>{stats['products']}</b>\n"
            f"Заказов: <b>{stats['orders']}</b>\n"
            f"Броней: <b>{stats['reservations']}</b>\n"
            f"Остаток на складе: <b>{stats['stock']} шт.</b>\n"
            f"Оборот: <b>{money(stats['revenue'])}</b>\n"
            f"Главный админ: <code>{MAIN_ADMIN_ID}</code>"
        )


async def callback_admin_list_sales(callback: CallbackQuery) -> None:
    if not await admin_guard(callback):
        return
    await callback.answer()
    sale_type = callback.data.split(":")[2]
    rows = get_recent_sales(20, sale_type)
    if not callback.message:
        return
    if not rows:
        await callback.message.answer("Пока записей нет.")
        return

    lines = [f"📋 <b>{'Заказы' if sale_type == 'order' else 'Брони'}</b>", ""]
    for row in rows:
        line = (
            f"{order_type_label(row['type'])} #{int(row['id'])} • "
            f"{escape_text(row['customer_name'])} • "
            f"{status_label(row['status'])} • "
            f"{money(int(row['total']))}"
        )
        if row["type"] == "reservation" and row["expires_at"] and row["status"] == "new":
            line += f" • до {human_dt(row['expires_at'])}"
        lines.append(line)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Открыть #{int(row['id'])}", callback_data=f"admin:sale:{int(row['id'])}")]
            for row in rows[:10]
        ] + [[InlineKeyboardButton(text="⬅️ Админ-панель", callback_data="admin:back")]]
    )
    await callback.message.answer("\n".join(lines), reply_markup=kb)


async def callback_admin_sale_view(callback: CallbackQuery) -> None:
    if not await admin_guard(callback):
        return
    await callback.answer()
    order_id = int(callback.data.split(":")[2])
    order = get_order(order_id)
    if not order:
        if callback.message:
            await callback.message.answer("Запись не найдена.")
        return
    if callback.message:
        await callback.message.answer(order_text(order), reply_markup=admin_sale_actions_kb(order_id, str(order["status"])))


async def callback_admin_status(callback: CallbackQuery, bot: Bot) -> None:
    if not await admin_guard(callback):
        return
    await callback.answer()
    _, _, order_id_str, status = callback.data.split(":")
    order_id = int(order_id_str)

    ok, text = update_sale_status(order_id, status)
    order = get_order(order_id)
    if callback.message:
        if not ok:
            await callback.message.answer(text)
            return
        await callback.message.answer(f"✅ {text}")
        if order:
            await callback.message.answer(
                order_text(order),
                reply_markup=admin_sale_actions_kb(order_id, str(order["status"])),
            )

    if order:
        entity = "Заказ" if order["type"] == "order" else "Бронь"
        user_text = f"ℹ️ <b>{entity} #{order_id}</b>: статус изменён на <b>{status_label(status)}</b>."
        await safe_send_user_message(bot, int(order["user_id"]), user_text)


async def callback_admin_products(callback: CallbackQuery) -> None:
    if not await admin_guard(callback):
        return
    await callback.answer()
    items = get_products()
    if callback.message:
        await callback.message.answer(
            products_text("🛠 Товары", items),
            reply_markup=admin_products_kb(items[:25]),
        )


async def callback_admin_product_view(callback: CallbackQuery) -> None:
    if not await admin_guard(callback):
        return
    await callback.answer()
    product_id = int(callback.data.split(":")[2])
    product = admin_get_product(product_id)
    if not product:
        if callback.message:
            await callback.message.answer("Товар не найден.")
        return
    if callback.message:
        await callback.message.answer(product_admin_text(product), reply_markup=admin_product_actions_kb(product_id))


async def callback_admin_toggle_product(callback: CallbackQuery) -> None:
    if not await admin_guard(callback):
        return
    await callback.answer()
    _, _, product_id_str, field = callback.data.split(":")
    product_id = int(product_id_str)
    product = admin_get_product(product_id)
    if not product:
        return
    new_value = 0 if int(product[field]) else 1
    update_product_field(product_id, field, new_value)
    product = admin_get_product(product_id)
    if callback.message and product:
        await callback.message.answer(product_admin_text(product), reply_markup=admin_product_actions_kb(product_id))


async def callback_admin_edit_product(callback: CallbackQuery, state: FSMContext) -> None:
    if not await admin_guard(callback):
        return
    await callback.answer()
    _, _, product_id_str, field = callback.data.split(":")
    product_id = int(product_id_str)
    product = admin_get_product(product_id)
    if not product:
        if callback.message:
            await callback.message.answer("Товар не найден.")
        return

    hints = {
        "title": "Введите новое название.",
        "price": "Введите новую цену числом.",
        "category": "Введите новую категорию.",
        "description": "Введите новое описание.",
        "photo": "Отправьте URL фото, file_id или саму фотографию.",
        "stock": "Введите новый остаток числом.",
    }
    await state.clear()
    await state.set_state(AdminProductEditState.waiting_value)
    await state.update_data(product_id=product_id, edit_field=field)
    if callback.message:
        await callback.message.answer(hints[field], reply_markup=cancel_menu())


async def admin_edit_product_value(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    product_id = int(data["product_id"])
    field = str(data["edit_field"])
    value_text = (message.text or "").strip()

    try:
        if field == "price":
            value: object = int(value_text)
            if int(value) < 0:
                raise ValueError
        elif field == "stock":
            value = int(value_text)
            if int(value) < 0:
                raise ValueError
        else:
            value = value_text
            if not str(value).strip():
                raise ValueError
    except Exception:
        await message.answer("Некорректное значение. Попробуйте ещё раз.")
        return

    update_product_field(product_id, field, value)
    await state.clear()
    product = admin_get_product(product_id)
    if product:
        await message.answer("✅ Товар обновлён.", reply_markup=main_menu(message.from_user.id))
        await message.answer(product_admin_text(product), reply_markup=admin_product_actions_kb(product_id))


async def admin_edit_product_photo(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    product_id = int(data["product_id"])
    field = str(data["edit_field"])
    if field != "photo" or not message.photo:
        return
    file_id = message.photo[-1].file_id
    update_product_field(product_id, "photo", file_id)
    await state.clear()
    product = admin_get_product(product_id)
    if product:
        await message.answer("✅ Фото обновлено.", reply_markup=main_menu(message.from_user.id))
        await message.answer(product_admin_text(product), reply_markup=admin_product_actions_kb(product_id))


async def callback_admin_add_product(callback: CallbackQuery, state: FSMContext) -> None:
    if not await admin_guard(callback):
        return
    await callback.answer()
    await state.clear()
    await state.set_state(AdminProductAddState.waiting_title)
    if callback.message:
        await callback.message.answer("Введите название нового товара.", reply_markup=cancel_menu())


async def admin_add_product_title(message: Message, state: FSMContext) -> None:
    title = (message.text or "").strip()
    if len(title) < 2:
        await message.answer("Название слишком короткое.")
        return
    await state.update_data(title=title)
    await state.set_state(AdminProductAddState.waiting_price)
    await message.answer("Введите цену числом.")


async def admin_add_product_price(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("Цена должна быть числом.")
        return
    await state.update_data(price=int(text))
    await state.set_state(AdminProductAddState.waiting_category)
    await message.answer("Введите категорию товара.")


async def admin_add_product_category(message: Message, state: FSMContext) -> None:
    category = (message.text or "").strip()
    if len(category) < 2:
        await message.answer("Категория слишком короткая.")
        return
    await state.update_data(category=category)
    await state.set_state(AdminProductAddState.waiting_description)
    await message.answer("Введите описание товара.")


async def admin_add_product_description(message: Message, state: FSMContext) -> None:
    description = (message.text or "").strip()
    if len(description) < 5:
        await message.answer("Описание слишком короткое.")
        return
    await state.update_data(description=description)
    await state.set_state(AdminProductAddState.waiting_photo)
    await message.answer("Отправьте URL фото, file_id, саму фотографию или напишите <b>нет</b>.")


async def admin_add_product_photo_text(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    await state.update_data(photo="" if text.lower() == "нет" else text)
    await state.set_state(AdminProductAddState.waiting_stock)
    await message.answer("Введите остаток числом.")


async def admin_add_product_photo_upload(message: Message, state: FSMContext) -> None:
    if not message.photo:
        return
    await state.update_data(photo=message.photo[-1].file_id)
    await state.set_state(AdminProductAddState.waiting_stock)
    await message.answer("Фото сохранено. Теперь введите остаток числом.")


async def admin_add_product_stock(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("Остаток должен быть числом.")
        return
    await state.update_data(stock=int(text))
    await state.set_state(AdminProductAddState.waiting_flags)
    await message.answer("Товар хит и/или новинка? Напишите в формате: <b>хит,новинка</b> или <b>нет</b>.")


async def admin_add_product_flags(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip().lower()
    is_hit = 1 if "хит" in text else 0
    is_new = 1 if "нов" in text else 0

    data = await state.get_data()
    product_id = add_product(
        {
            "title": data["title"],
            "price": data["price"],
            "category": data["category"],
            "description": data["description"],
            "photo": data.get("photo", ""),
            "stock": data["stock"],
            "is_hit": is_hit,
            "is_new": is_new,
        }
    )
    await state.clear()

    product = admin_get_product(product_id)
    await message.answer(f"✅ Товар #{product_id} добавлен.", reply_markup=main_menu(message.from_user.id))
    if product:
        await message.answer(product_admin_text(product), reply_markup=admin_product_actions_kb(product_id))


async def callback_admin_search_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not await admin_guard(callback):
        return
    await callback.answer()
    await state.clear()
    await state.set_state(AdminOrderSearchState.waiting_query)
    if callback.message:
        await callback.message.answer(
            "Введите номер заказа, имя клиента или телефон.",
            reply_markup=cancel_menu(),
        )


async def admin_search_input(message: Message, state: FSMContext) -> None:
    query = (message.text or "").strip()
    rows = admin_search_sales(query)
    await state.clear()
    if not rows:
        await message.answer("Ничего не найдено.", reply_markup=main_menu(message.from_user.id))
        return

    lines = ["🔎 <b>Результаты поиска</b>", ""]
    buttons = []
    for row in rows:
        lines.append(
            f"{order_type_label(row['type'])} #{int(row['id'])} • {escape_text(row['customer_name'])} • {escape_text(row['phone'])} • {status_label(row['status'])}"
        )
        buttons.append([InlineKeyboardButton(text=f"Открыть #{int(row['id'])}", callback_data=f"admin:sale:{int(row['id'])}")])

    await message.answer("\n".join(lines), reply_markup=main_menu(message.from_user.id))
    await message.answer("Открыть запись:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons[:15]))


async def callback_admin_export(callback: CallbackQuery) -> None:
    if not await admin_guard(callback):
        return
    await callback.answer("Готовлю CSV…")
    rows = get_recent_sales(1000)
    if not rows:
        if callback.message:
            await callback.message.answer("Нет данных для экспорта.")
        return

    with tempfile.NamedTemporaryFile("w", encoding="utf-8-sig", newline="", delete=False, suffix=".csv") as tmp:
        writer = csv.writer(tmp, delimiter=";")
        writer.writerow(
            [
                "id",
                "type",
                "status",
                "customer_name",
                "phone",
                "delivery_method",
                "payment_method",
                "address",
                "comment",
                "total",
                "created_at",
                "updated_at",
                "expires_at",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    int(row["id"]),
                    row["type"],
                    row["status"],
                    row["customer_name"],
                    row["phone"],
                    row["delivery_method"],
                    row["payment_method"],
                    row["address"],
                    row["comment"],
                    int(row["total"]),
                    row["created_at"],
                    row["updated_at"],
                    row["expires_at"],
                ]
            )
        file_path = tmp.name

    if callback.message:
        await callback.message.answer_document(
            FSInputFile(file_path, filename=f"shop_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        )


async def callback_admin_backup(callback: CallbackQuery) -> None:
    if not await admin_guard(callback):
        return
    await callback.answer("Отправляю бэкап…")
    if callback.message:
        await callback.message.answer_document(FSInputFile(DB_PATH, filename=Path(DB_PATH).name))
        if Path(LOG_PATH).exists():
            await callback.message.answer_document(FSInputFile(LOG_PATH, filename=Path(LOG_PATH).name))


# =========================
# MAIN
# =========================
async def main() -> None:
    configure_logging()
    init_db()

    if not BOT_TOKEN:
        raise ValueError("Укажите BOT_TOKEN в переменных окружения.")

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
    dp.callback_query.register(checkout_delivery_callback, F.data.startswith("delivery:"))
    dp.callback_query.register(checkout_payment_callback, F.data.startswith("payment:"))
    dp.message.register(checkout_address, CheckoutState.waiting_address)
    dp.message.register(checkout_comment, CheckoutState.waiting_comment)

    dp.callback_query.register(noop_handler, F.data == "noop")
    dp.callback_query.register(callback_catalog, F.data == "catalog:open")
    dp.callback_query.register(callback_section, F.data.startswith("section:"))
    dp.callback_query.register(callback_category, F.data.startswith("cat:"))
    dp.callback_query.register(callback_product, F.data.startswith("product:"))
    dp.callback_query.register(callback_add, F.data.startswith("add:"))
    dp.callback_query.register(callback_favorite, F.data.startswith("fav:"))
    dp.callback_query.register(callback_cart_open, F.data == "cart:open")
    dp.callback_query.register(callback_cart_edit, F.data.startswith("cart:plus:"))
    dp.callback_query.register(callback_cart_edit, F.data.startswith("cart:minus:"))
    dp.callback_query.register(callback_cart_edit, F.data.startswith("cart:del:"))
    dp.callback_query.register(callback_cart_clear, F.data == "cart:clear")
    dp.callback_query.register(callback_checkout_start, F.data.startswith("checkout:"))
    dp.callback_query.register(callback_sale_view, F.data.startswith("sale:view:"))

    dp.callback_query.register(callback_admin_back, F.data == "admin:back")
    dp.callback_query.register(callback_admin_stats, F.data == "admin:stats")
    dp.callback_query.register(callback_admin_list_sales, F.data.startswith("admin:list:"))
    dp.callback_query.register(callback_admin_sale_view, F.data.startswith("admin:sale:"))
    dp.callback_query.register(callback_admin_status, F.data.startswith("admin:status:"))
    dp.callback_query.register(callback_admin_products, F.data == "admin:products")
    dp.callback_query.register(callback_admin_product_view, F.data.startswith("admin:product:"))
    dp.callback_query.register(callback_admin_toggle_product, F.data.startswith("admin:toggle:"))
    dp.callback_query.register(callback_admin_edit_product, F.data.startswith("admin:edit:"))
    dp.callback_query.register(callback_admin_add_product, F.data == "admin:add_product")
    dp.callback_query.register(callback_admin_search_start, F.data == "admin:search")
    dp.callback_query.register(callback_admin_export, F.data == "admin:export")
    dp.callback_query.register(callback_admin_backup, F.data == "admin:backup")

    dp.message.register(admin_add_product_title, AdminProductAddState.waiting_title)
    dp.message.register(admin_add_product_price, AdminProductAddState.waiting_price)
    dp.message.register(admin_add_product_category, AdminProductAddState.waiting_category)
    dp.message.register(admin_add_product_description, AdminProductAddState.waiting_description)
    dp.message.register(admin_add_product_photo_upload, AdminProductAddState.waiting_photo, F.photo)
    dp.message.register(admin_add_product_photo_text, AdminProductAddState.waiting_photo, F.text)
    dp.message.register(admin_add_product_stock, AdminProductAddState.waiting_stock)
    dp.message.register(admin_add_product_flags, AdminProductAddState.waiting_flags)

    dp.message.register(admin_edit_product_photo, AdminProductEditState.waiting_value, F.photo)
    dp.message.register(admin_edit_product_value, AdminProductEditState.waiting_value, F.text)
    dp.message.register(admin_search_input, AdminOrderSearchState.waiting_query)

    dp.message.register(text_menu, F.text)

    asyncio.create_task(expire_reservations_job(bot))
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped")
