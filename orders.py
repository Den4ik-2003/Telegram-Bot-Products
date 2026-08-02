import asyncio
import csv
import io
import logging
import os
import uuid
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, BufferedInputFile,
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InputMediaPhoto,
)
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("products_bot")

BOT_TOKEN     = os.environ["BOT_TOKEN"]
BOT_PASSWORD  = os.environ["BOT_PASSWORD"]
CALC_PASSWORD = os.environ["CALC_PASSWORD"]
MONGO_URI     = os.environ["MONGO_URI"]

mongo_client: AsyncIOMotorClient | None = None
db = None
products_col = None
settings_col = None
auth_col = None
calc_col = None
users_col = None
baskets_col = None

def init_mongo():
    global mongo_client, db, products_col, settings_col, auth_col, calc_col, users_col, baskets_col
    mongo_client = AsyncIOMotorClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000,
        connectTimeoutMS=10000,
        socketTimeoutMS=20000,
        maxPoolSize=20,
        retryWrites=True,
        retryReads=True,
    )
    db = mongo_client["products_bot"]
    products_col = db["products"]
    settings_col = db["settings"]
    auth_col = db["auth"]
    calc_col = db["calc_results"]
    users_col = db["users"]
    baskets_col = db["baskets"]

DB_ERROR_TEXT = "⚠️ Тимчасова проблема з базою даних. Спробуйте ще раз через кілька секунд."
GENERIC_ERROR_TEXT = "⚠️ Сталася помилка під час обробки дії. Спробуйте ще раз або поверніться в меню нижче."

async def db_call(coro, default=None):
    try:
        return await coro
    except PyMongoError:
        logger.exception("MongoDB error")
        return default

user_roles: dict[int, str] = {}

async def load_user_roles():
    global user_roles
    try:
        docs = await auth_col.find({}, {"uid": 1, "role": 1}).to_list(length=None)
        user_roles = {d["uid"]: d.get("role", "full") for d in docs}
        logger.info("Завантажено %d авторизованих користувачів у кеш", len(user_roles))
    except PyMongoError:
        logger.exception("Не вдалось завантажити список авторизованих користувачів")

async def get_role(uid: int):
    if uid in user_roles:
        return user_roles[uid]
    try:
        doc = await auth_col.find_one({"uid": uid})
    except PyMongoError:
        logger.exception("MongoDB error in get_role")
        return "ERROR"
    if doc is not None:
        role = doc.get("role", "full")
        user_roles[uid] = role
        return role
    return None

def cached_role(uid: int) -> str:
    return user_roles.get(uid, "full")

async def authorize(uid: int, role: str = "full") -> bool:
    try:
        await auth_col.update_one({"uid": uid}, {"$set": {"uid": uid, "role": role}}, upsert=True)
        user_roles[uid] = role
        return True
    except PyMongoError:
        logger.exception("MongoDB error in authorize")
        return False

async def upsert_user(uid: int, username: str | None, first_name: str = ""):
    username_norm = (username or "").lower().lstrip("@")
    await db_call(users_col.update_one(
        {"uid": uid},
        {"$set": {
            "uid": uid,
            "username": username_norm,
            "first_name": first_name,
            "last_seen": datetime.now().isoformat(),
        }},
        upsert=True,
    ))

async def get_uid_by_username(username: str):
    username_norm = username.strip().lower().lstrip("@")
    if not username_norm:
        return None
    doc = await db_call(users_col.find_one({"username": username_norm}))
    return doc["uid"] if doc else None

async def next_product_id() -> int:
    doc = await db_call(products_col.find_one(sort=[("id", -1)]))
    return (doc["id"] + 1) if doc else 1

async def add_product(product: dict):
    await db_call(products_col.insert_one(product))

async def get_product(pid: int) -> dict | None:
    return await db_call(products_col.find_one({"id": pid}, {"_id": 0}))

async def delete_product(pid: int):
    await db_call(products_col.delete_one({"id": pid}))

async def get_all_products() -> list:
    cursor = products_col.find({}, {"_id": 0}).sort("id", -1)
    return await db_call(cursor.to_list(length=None), default=[]) or []

async def get_calc_settings() -> dict:
    doc = await db_call(settings_col.find_one({"_id": "calc_settings"}))
    defaults = {
        "price_per_kg_avia": 0.0,
        "price_per_kg_sea": 0.0,
        "yuan_rate": 0.0,
        "usd_rate": 0.0,
    }
    if doc:
        for k in defaults:
            if k in doc:
                defaults[k] = doc[k]
    return defaults

async def save_calc_setting(field: str, value: float):
    await db_call(settings_col.update_one({"_id": "calc_settings"}, {"$set": {field: value}}, upsert=True))

async def next_calc_id() -> int:
    doc = await db_call(calc_col.find_one(sort=[("id", -1)]))
    return (doc["id"] + 1) if doc else 1

async def add_calc(record: dict):
    await db_call(calc_col.insert_one(record))

async def get_all_calcs() -> list:
    cursor = calc_col.find({}, {"_id": 0}).sort("id", -1)
    return await db_call(cursor.to_list(length=None), default=[]) or []

async def get_calc(cid: int) -> dict | None:
    return await db_call(calc_col.find_one({"id": cid}, {"_id": 0}))

async def update_calc(cid: int, fields: dict):
    await db_call(calc_col.update_one({"id": cid}, {"$set": fields}))

async def delete_calc(cid: int):
    await db_call(calc_col.delete_one({"id": cid}))
    await db_call(baskets_col.update_many({}, {"$pull": {"items": {"calc_id": cid}}}))

async def recalc_calc_costs(cid: int):
    record = await get_calc(cid)
    if not record:
        return None
    s = await get_calc_settings()
    weight = parse_float(record.get("weight_kg", 0))
    price_yuan = parse_float(record.get("price_yuan", 0))
    price_uah = price_yuan * s["yuan_rate"]
    fields = {"price_uah": price_uah}
    for method in ("avia", "sea"):
        rate_per_kg_usd = s["price_per_kg_avia"] if method == "avia" else s["price_per_kg_sea"]
        delivery_cost_usd = weight * rate_per_kg_usd
        delivery_cost_uah = delivery_cost_usd * s["usd_rate"]
        cost_price_uah = price_uah + delivery_cost_uah
        cost_price_usd = cost_price_uah / s["usd_rate"] if s["usd_rate"] else 0.0
        fields[f"delivery_cost_usd_{method}"] = delivery_cost_usd
        fields[f"delivery_cost_uah_{method}"] = delivery_cost_uah
        fields[f"cost_price_uah_{method}"] = cost_price_uah
        fields[f"cost_price_usd_{method}"] = cost_price_usd
    await update_calc(cid, fields)
    return await get_calc(cid)

async def recalc_all_calcs():
    calcs = await get_all_calcs()
    for c in calcs:
        await recalc_calc_costs(c["id"])

async def next_basket_id() -> int:
    doc = await db_call(baskets_col.find_one(sort=[("id", -1)]))
    return (doc["id"] + 1) if doc else 1

async def add_basket(basket: dict):
    await db_call(baskets_col.insert_one(basket))

async def get_basket(bid: int) -> dict | None:
    return await db_call(baskets_col.find_one({"id": bid}, {"_id": 0}))

async def get_all_baskets() -> list:
    cursor = baskets_col.find({}, {"_id": 0}).sort("id", -1)
    return await db_call(cursor.to_list(length=None), default=[]) or []

async def delete_basket(bid: int):
    await db_call(baskets_col.delete_one({"id": bid}))

async def update_basket(bid: int, fields: dict):
    await db_call(baskets_col.update_one({"id": bid}, {"$set": fields}))

async def basket_add_item(bid: int, item: dict):
    await db_call(baskets_col.update_one({"id": bid}, {"$push": {"items": item}}))

async def basket_remove_item(bid: int, item_id: str):
    await db_call(baskets_col.update_one({"id": bid}, {"$pull": {"items": {"item_id": item_id}}}))

async def basket_update_item(bid: int, item_id: str, fields: dict):
    update = {f"items.$.{k}": v for k, v in fields.items()}
    await db_call(baskets_col.update_one({"id": bid, "items.item_id": item_id}, {"$set": update}))

async def calcs_map_by_id(ids: list) -> dict:
    ids = list(set(ids))
    if not ids:
        return {}
    cursor = calc_col.find({"id": {"$in": ids}}, {"_id": 0})
    docs = await db_call(cursor.to_list(length=None), default=[]) or []
    return {d["id"]: d for d in docs}

def parse_float(val) -> float:
    try:
        return float(str(val).replace(",", ".").strip())
    except (ValueError, TypeError):
        return 0.0

def parse_int(val) -> int:
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return 0

def is_valid_link(val: str) -> bool:
    val = (val or "").strip().lower()
    return val.startswith("http://") or val.startswith("https://")

def fmt_stars(rating: int) -> str:
    rating = max(0, min(10, rating))
    return "⭐" * rating + "☆" * (10 - rating) + f" ({rating}/10)"

def fmt_product(p: dict) -> str:
    cost = parse_float(p.get("cost_price", 0))
    return (
        f"*№{p['id']} — {p.get('name','')}*\n"
        f"💵 Собівартість: *{cost:,.0f} грн*"
    )

DELIVERY_METHOD_LABELS = {"avia": "✈️ Авіа", "sea": "🚢 Море"}

def fmt_calc(c: dict) -> str:
    rating = max(0, min(10, parse_int(c.get("rating", 0))))
    lines = [f"*№{c.get('id')} — {c.get('name','')}*"]
    if c.get("description"):
        lines.append(f"📝 {c['description']}")
    lines.append(f"⭐ Оцінка: {fmt_stars(rating)}")
    lines.append(f"💴 Ціна товару: *{c.get('price_yuan',0):,.2f} ¥* (*{c.get('price_uah',0):,.0f} грн*)")
    lines.append(f"⚖️ Вага: *{c.get('weight_kg',0):,.2f} кг*")
    lines.append("")
    lines.append(
        f"✈️ *Авіа* — доставка {c.get('delivery_cost_uah_avia',0):,.0f} грн "
        f"({c.get('delivery_cost_usd_avia',0):,.2f} $)"
    )
    lines.append(
        f"   Собівартість: *{c.get('cost_price_uah_avia',0):,.0f} грн* "
        f"(*{c.get('cost_price_usd_avia',0):,.2f} $*)"
    )
    lines.append(
        f"🚢 *Море* — доставка {c.get('delivery_cost_uah_sea',0):,.0f} грн "
        f"({c.get('delivery_cost_usd_sea',0):,.2f} $)"
    )
    lines.append(
        f"   Собівартість: *{c.get('cost_price_uah_sea',0):,.0f} грн* "
        f"(*{c.get('cost_price_usd_sea',0):,.2f} $*)"
    )
    if c.get("link"):
        lines.append("")
        lines.append("🔗 Посилання на товар додано (кнопка нижче)")
    return "\n".join(lines)

def fmt_basket(b: dict, calcs_by_id: dict) -> str:
    """
    Розраховує статистику кошика окремо по товару та по доставці,
    а не лише загальну собівартість.
    """
    items = b.get("items", [])
    total_product = 0.0
    total_delivery = 0.0
    item_lines = []
    for it in items:
        c = calcs_by_id.get(it["calc_id"])
        if not c:
            item_lines.append(f"• №{it['calc_id']} — ⚠️ товар видалено")
            continue
        qty = it.get("qty", 1)
        method = it.get("method", "avia")
        unit_product = parse_float(c.get("price_uah", 0))
        unit_delivery = parse_float(c.get(f"delivery_cost_uah_{method}", 0))
        line_product = unit_product * qty
        line_delivery = unit_delivery * qty
        line_total = line_product + line_delivery
        total_product += line_product
        total_delivery += line_delivery
        method_label = DELIVERY_METHOD_LABELS.get(method, "—")
        item_lines.append(
            f"• №{it['calc_id']} {c.get('name','')} x{qty} {method_label} — *{line_total:,.0f} грн*\n"
            f"   ↳ товар {line_product:,.0f} грн + доставка {line_delivery:,.0f} грн"
        )
    total = total_product + total_delivery
    budget = parse_float(b.get("budget", 0))
    remaining = budget - total
    status = "✅ У межах бюджету" if remaining >= 0 else "⚠️ Бюджет перевищено"
    lines = [
        f"*🧺 Кошик №{b.get('id')} — {b.get('name','')}*",
        f"💰 Бюджет: *{budget:,.0f} грн*",
        "",
        f"📦 Витрати на товар: *{total_product:,.0f} грн*",
        f"🚚 Витрати на доставку: *{total_delivery:,.0f} грн*",
        f"🧾 Разом: *{total:,.0f} грн*",
        f"{status} — залишок *{remaining:,.0f} грн*",
        f"🗂 Позицій у кошику: *{len(items)}*",
    ]
    if item_lines:
        lines.append("")
        lines.extend(item_lines)
    return "\n".join(lines)

def build_calc_csv(calcs: list) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "id", "name", "description", "link", "rating", "price_yuan", "price_uah", "weight_kg",
        "avia_delivery_cost_usd", "avia_delivery_cost_uah", "avia_cost_price_uah", "avia_cost_price_usd",
        "sea_delivery_cost_usd", "sea_delivery_cost_uah", "sea_cost_price_uah", "sea_cost_price_usd",
        "created_at",
    ])
    for c in calcs:
        writer.writerow([
            c.get("id", ""), c.get("name", ""), c.get("description", ""), c.get("link", ""),
            c.get("rating", "0"), c.get("price_yuan", ""), c.get("price_uah", ""), c.get("weight_kg", ""),
            c.get("delivery_cost_usd_avia", ""), c.get("delivery_cost_uah_avia", ""),
            c.get("cost_price_uah_avia", ""), c.get("cost_price_usd_avia", ""),
            c.get("delivery_cost_usd_sea", ""), c.get("delivery_cost_uah_sea", ""),
            c.get("cost_price_uah_sea", ""), c.get("cost_price_usd_sea", ""),
            c.get("created_at", ""),
        ])
    return output.getvalue().encode("utf-8-sig")

class Auth(StatesGroup):
    waiting_password = State()

class AddProduct(StatesGroup):
    photo      = State()
    name       = State()
    cost_price = State()

class SearchName(StatesGroup):
    typing = State()

class CalcSettingsEdit(StatesGroup):
    typing = State()

class CalcProduct(StatesGroup):
    photo       = State()
    name        = State()
    description = State()
    link        = State()
    rating      = State()
    price_yuan  = State()
    weight      = State()

class CalcEditField(StatesGroup):
    typing = State()

class SendCalc(StatesGroup):
    filter_choice    = State()
    filter_rating    = State()
    choosing         = State()
    action_choice    = State()
    waiting_username = State()

class BasketCreate(StatesGroup):
    name   = State()
    budget = State()

class BasketEditField(StatesGroup):
    typing = State()

class BasketItemAdd(StatesGroup):
    quantity = State()
    method   = State()

class BasketItemEdit(StatesGroup):
    quantity = State()
    method   = State()

def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Новий товар"), KeyboardButton(text="📋 Всі товари")],
        [KeyboardButton(text="🔍 За назвою")],
        [KeyboardButton(text="🧮 Калькулятор"), KeyboardButton(text="📤 CSV товарів")],
    ], resize_keyboard=True)

def kb_cancel() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Скасувати")]], resize_keyboard=True)

def kb_skip_cancel() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="⏩ Пропустити"), KeyboardButton(text="❌ Скасувати")]
    ], resize_keyboard=True)

def kb_calc_menu(role: str = "full") -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="➕ Порахувати товар")],
        [KeyboardButton(text="📄 Мої розрахунки")],
        [KeyboardButton(text="🧺 Кошики")],
        [KeyboardButton(text="📤 Переслати/Зберегти")],
        [KeyboardButton(text="⚙️ Налаштування калькулятора")],
    ]
    if role == "full":
        rows.append([KeyboardButton(text="◀️ Головне меню")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def ikb_product_actions(pid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Продано (видалити)", callback_data=f"sold:{pid}")],
        [InlineKeyboardButton(text="◀️ До списку", callback_data="back_to_list")],
    ])

def ikb_products_list(products: list, page: int = 0, per_page: int = 8) -> InlineKeyboardMarkup:
    start = page * per_page
    chunk = products[start:start + per_page]
    rows = []
    for p in chunk:
        cost = parse_float(p.get("cost_price", 0))
        label = f"№{p['id']} {p.get('name','')[:22]} — {cost:,.0f}грн"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"view:{p['id']}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"page:{page-1}"))
    total_pages = max(1, (len(products) - 1) // per_page + 1)
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
    if start + per_page < len(products):
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"page:{page+1}"))
    if nav:
        rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows)

def ikb_calc_settings() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✈️ Ціна доставки авіа (за 1 кг, $)", callback_data="calcset:price_per_kg_avia")],
        [InlineKeyboardButton(text="🚢 Ціна доставки морем (за 1 кг, $)", callback_data="calcset:price_per_kg_sea")],
        [InlineKeyboardButton(text="💱 Змінити курс юаня", callback_data="calcset:yuan_rate")],
        [InlineKeyboardButton(text="💵 Змінити курс долара", callback_data="calcset:usd_rate")],
        [InlineKeyboardButton(text="🔄 Перерахувати всі товари", callback_data="calcset_recalc_all")],
    ])

def ikb_calc_actions(cid: int, calc: dict | None = None) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🧺 У кошик", callback_data=f"tobasket:{cid}")],
        [InlineKeyboardButton(text="✏️ Редагувати", callback_data=f"editcalc:{cid}")],
    ]
    link = (calc or {}).get("link", "")
    if is_valid_link(link):
        rows.append([InlineKeyboardButton(text="🔗 Посилання на товар", url=link.strip())])
    rows.append([InlineKeyboardButton(text="🗑 Видалити", callback_data=f"delcalc:{cid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def ikb_calc_edit_fields(cid: int) -> InlineKeyboardMarkup:
    fields = [
        ("name", "Назва"), ("description", "Опис"),
        ("link", "Посилання"), ("rating", "Оцінка (0-10)"),
        ("price_yuan", "Ціна (¥)"), ("weight_kg", "Вага (кг)"),
    ]
    rows = []
    for i in range(0, len(fields), 2):
        row = [InlineKeyboardButton(text=label, callback_data=f"editcalcfield:{cid}:{key}")
               for key, label in fields[i:i+2]]
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)

def ikb_send_calc_filter() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗂 Усі", callback_data="filtercalc:all")],
        [InlineKeyboardButton(text="⭐ Мін. рейтинг", callback_data="filtercalc:rating")],
    ])

def ikb_send_calc_list(calcs: list, selected_ids) -> InlineKeyboardMarkup:
    selected = set(selected_ids)
    rows = []
    for c in calcs:
        mark = "✅ " if c["id"] in selected else ""
        label = f"{mark}№{c['id']} {c.get('name','')[:22]}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"selcalc:{c['id']}")])
    rows.append([InlineKeyboardButton(text="📌 Обрати всі", callback_data="selcalc_all")])
    rows.append([InlineKeyboardButton(text="✅ Готово", callback_data="selcalc_done")])
    rows.append([InlineKeyboardButton(text="❌ Скасувати", callback_data="selcalc_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def ikb_send_calc_action() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Переслати", callback_data="calcaction:send")],
        [InlineKeyboardButton(text="💾 Зберегти CSV", callback_data="calcaction:save")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="calcaction:cancel")],
    ])

def ikb_baskets_list(baskets: list, calcs_by_id: dict) -> InlineKeyboardMarkup:
    rows = []
    for b in baskets:
        items = b.get("items", [])
        total = sum(
            parse_float(calcs_by_id.get(it["calc_id"], {}).get(f"cost_price_uah_{it['method']}", 0)) * it.get("qty", 1)
            for it in items
        )
        budget = parse_float(b.get("budget", 0))
        mark = "✅" if total <= budget else "⚠️"
        label = f"{mark} №{b['id']} {b.get('name','')[:18]} — {total:,.0f}/{budget:,.0f}грн"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"basket_view:{b['id']}")])
    rows.append([InlineKeyboardButton(text="➕ Новий кошик", callback_data="basket_new")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def ikb_basket_actions(bid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Додати товар", callback_data=f"basket_add:{bid}:0")],
        [InlineKeyboardButton(text="✏️ Редагувати позиції", callback_data=f"basket_rmlist:{bid}")],
        [InlineKeyboardButton(text="📝 Редагувати кошик", callback_data=f"basket_edit:{bid}")],
        [InlineKeyboardButton(text="🔄 Оновити", callback_data=f"basket_view:{bid}")],
        [InlineKeyboardButton(text="🗑 Видалити кошик", callback_data=f"basket_del:{bid}")],
        [InlineKeyboardButton(text="◀️ До списку кошиків", callback_data="basket_back")],
    ])

def ikb_basket_edit_fields(bid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Назва кошика", callback_data=f"basketeditfield:{bid}:name")],
        [InlineKeyboardButton(text="💰 Бюджет", callback_data=f"basketeditfield:{bid}:budget")],
        [InlineKeyboardButton(text="◀️ Назад до кошика", callback_data=f"basket_view:{bid}")],
    ])

def ikb_basket_add_list(bid: int, calcs: list, in_basket: set, page: int = 0, per_page: int = 8) -> InlineKeyboardMarkup:
    available = [c for c in calcs if c["id"] not in in_basket]
    start = page * per_page
    chunk = available[start:start + per_page]
    rows = []
    for c in chunk:
        avia = parse_float(c.get("cost_price_uah_avia", 0))
        sea = parse_float(c.get("cost_price_uah_sea", 0))
        label = f"➕ №{c['id']} {c.get('name','')[:16]} ✈️{avia:,.0f}/🚢{sea:,.0f}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"basket_addsel:{bid}:{c['id']}:{page}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"basket_add:{bid}:{page-1}"))
    total_pages = max(1, (len(available) - 1) // per_page + 1) if available else 1
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
    if start + per_page < len(available):
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"basket_add:{bid}:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="✅ Готово", callback_data=f"basket_view:{bid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def ikb_basket_remove_list(bid: int, items: list, calcs_by_id: dict) -> InlineKeyboardMarkup:
    """Список позицій кошика — тап відкриває меню редагування/видалення позиції."""
    rows = []
    for it in items:
        c = calcs_by_id.get(it["calc_id"], {})
        method_label = DELIVERY_METHOD_LABELS.get(it["method"], "—")
        unit_cost = parse_float(c.get(f"cost_price_uah_{it['method']}", 0))
        qty = it.get("qty", 1)
        total = unit_cost * qty
        label = f"✏️ №{it['calc_id']} {c.get('name','')[:14]} x{qty} {method_label} — {total:,.0f}грн"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"basket_item_open:{bid}:{it['item_id']}")])
    rows.append([InlineKeyboardButton(text="✅ Готово", callback_data=f"basket_view:{bid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def ikb_basket_item_actions(bid: int, item_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Змінити кількість/доставку", callback_data=f"basket_item_edit:{bid}:{item_id}")],
        [InlineKeyboardButton(text="🗑 Видалити позицію", callback_data=f"basket_rm:{bid}:{item_id}")],
        [InlineKeyboardButton(text="◀️ Назад до кошика", callback_data=f"basket_view:{bid}")],
    ])

def ikb_pick_basket(cid: int, baskets: list) -> InlineKeyboardMarkup:
    rows = []
    for b in baskets:
        budget = parse_float(b.get("budget", 0))
        label = f"🧺 №{b['id']} {b.get('name','')[:18]} (бюджет {budget:,.0f}грн)"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"tobasketsel:{cid}:{b['id']}")])
    rows.append([InlineKeyboardButton(text="➕ Новий кошик", callback_data="basket_new")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def ikb_basket_item_method() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✈️ Авіа", callback_data="bimethod:avia")],
        [InlineKeyboardButton(text="🚢 Море", callback_data="bimethod:sea")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="bimethod:cancel")],
    ])

CALC_SETTING_LABELS = {
    "price_per_kg_avia": "ціну доставки авіа за 1 кг ($)",
    "price_per_kg_sea": "ціну доставки морем за 1 кг ($)",
    "yuan_rate": "курс юаня (грн за 1¥)",
    "usd_rate": "курс долара (грн за 1$)",
}

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp  = Dispatcher(storage=MemoryStorage())

user_list_cache: dict = {}
send_calc_cache: dict = {}

async def require_auth(msg: Message, state: FSMContext, need_full: bool = False):
    role = await get_role(msg.from_user.id)
    if role == "ERROR":
        await msg.answer(DB_ERROR_TEXT)
        return None
    if role is None:
        current_state = await state.get_state()
        if current_state != Auth.waiting_password:
            await state.set_state(Auth.waiting_password)
            await msg.answer("🔒 *Доступ закритий*\n\nВведіть пароль:", reply_markup=ReplyKeyboardRemove())
        return None
    if need_full and role != "full":
        await msg.answer(
            "⛔ У вас немає доступу до цього розділу.\nВам доступний лише розділ *🧮 Калькулятор*.",
            reply_markup=kb_calc_menu(role),
        )
        return None
    return role

async def require_full_cb(cb: CallbackQuery) -> bool:
    role = await get_role(cb.from_user.id)
    if role == "ERROR":
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass
        return False
    if role != "full":
        try:
            await cb.answer("⛔ Немає доступу до цього розділу.", show_alert=True)
        except TelegramAPIError:
            pass
        return False
    return True

async def send_basket_item_prompt(target: Message, state: FSMContext, bid: int, cid: int):
    calc = await get_calc(cid)
    if not calc:
        await target.answer("⚠️ Товар не знайдено.")
        return
    await state.set_state(BasketItemAdd.quantity)
    await state.update_data(bi_bid=bid, bi_cid=cid)
    text = (
        f"🧺 Додаємо *№{cid} — {calc.get('name','')}* у кошик №{bid}\n\n"
        f"✈️ Авіа собівартість за од.: *{calc.get('cost_price_uah_avia',0):,.0f} грн*\n"
        f"🚢 Море собівартість за од.: *{calc.get('cost_price_uah_sea',0):,.0f} грн*\n\n"
        "🔢 Введіть кількість (шт.):"
    )
    await target.answer(text, reply_markup=kb_cancel())

async def send_basket_photos(chat_id: int, items: list, calcs_by_id: dict):
    """Надсилає фото всіх товарів, що є в кошику (альбомами по 10, з підписом позиції)."""
    media = []
    for it in items:
        c = calcs_by_id.get(it["calc_id"])
        if not c or not c.get("photo_id"):
            continue
        method_label = DELIVERY_METHOD_LABELS.get(it.get("method"), "—")
        qty = it.get("qty", 1)
        unit_product = parse_float(c.get("price_uah", 0))
        unit_delivery = parse_float(c.get(f"delivery_cost_uah_{it.get('method')}", 0))
        line_total = (unit_product + unit_delivery) * qty
        caption = (
            f"№{it['calc_id']} — {c.get('name','')}\n"
            f"{qty} шт. {method_label}\n"
            f"Разом: {line_total:,.0f} грн"
        )
        media.append(InputMediaPhoto(media=c["photo_id"], caption=caption[:1024]))

    if not media:
        return

    for i in range(0, len(media), 10):
        chunk = media[i:i + 10]
        try:
            if len(chunk) == 1:
                m = chunk[0]
                await bot.send_photo(chat_id=chat_id, photo=m.media, caption=m.caption)
            else:
                await bot.send_media_group(chat_id=chat_id, media=chunk)
        except TelegramAPIError:
            logger.exception("Не вдалось надіслати фото товарів кошика")

async def show_basket(source, bid: int):
    """
    Показує повну картку кошика: текст зі статистикою (окремо товар/доставка),
    кнопки дій та фото всіх товарів, що в ньому лежать.
    `source` — Message або CallbackQuery.
    """
    is_cb = isinstance(source, CallbackQuery)
    chat_msg: Message = source.message if is_cb else source

    b = await get_basket(bid)
    if not b:
        text = "⚠️ Кошик не знайдено."
        if is_cb:
            try:
                await source.answer(text, show_alert=True)
            except TelegramAPIError:
                pass
        else:
            await chat_msg.answer(text)
        return

    await recalc_all_calcs()
    b = await get_basket(bid)
    items = b.get("items", [])
    calcs_by_id = await calcs_map_by_id([it["calc_id"] for it in items])
    text = fmt_basket(b, calcs_by_id)

    if is_cb:
        try:
            await chat_msg.edit_text(text, reply_markup=ikb_basket_actions(bid))
        except TelegramAPIError:
            await chat_msg.answer(text, reply_markup=ikb_basket_actions(bid))
    else:
        await chat_msg.answer(text, reply_markup=ikb_basket_actions(bid))

    await send_basket_photos(chat_msg.chat.id, items, calcs_by_id)

@dp.errors()
async def global_error_handler(event, exception=None):
    exc = exception if exception is not None else getattr(event, "exception", None)
    logger.exception("Unhandled error while processing update: %s", exc)
    try:
        update = getattr(event, "update", None)
        if update is None:
            return True
        chat_id = None
        if update.message:
            chat_id = update.message.chat.id
        elif update.callback_query:
            cbq = update.callback_query
            try:
                await cbq.answer("⚠️ Сталася помилка. Спробуйте ще раз.", show_alert=True)
            except TelegramAPIError:
                pass
            if cbq.message:
                chat_id = cbq.message.chat.id
        if chat_id is not None:
            try:
                await bot.send_message(chat_id, GENERIC_ERROR_TEXT, reply_markup=kb_main())
            except TelegramAPIError:
                pass
    except Exception:
        logger.exception("Failed to notify user about error from global_error_handler")
    return True

@dp.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    await upsert_user(msg.from_user.id, msg.from_user.username, msg.from_user.first_name or "")
    role = await get_role(msg.from_user.id)
    if role == "ERROR":
        await msg.answer(DB_ERROR_TEXT)
        return
    if role == "full":
        await msg.answer("👋 *Товари*\n\nОбери дію:", reply_markup=kb_main())
    elif role == "calc":
        await msg.answer("🧮 *Калькулятор*\n\nОбери дію:", reply_markup=kb_calc_menu(role))
    else:
        await state.set_state(Auth.waiting_password)
        await msg.answer("🔒 *Доступ закритий*\n\nВведіть пароль:", reply_markup=ReplyKeyboardRemove())

@dp.message(Auth.waiting_password)
async def check_password(msg: Message, state: FSMContext):
    if msg.text == BOT_PASSWORD:
        role = "full"
    elif msg.text == CALC_PASSWORD:
        role = "calc"
    else:
        await msg.answer("❌ Невірний пароль. Спробуй ще раз:")
        return

    ok = await authorize(msg.from_user.id, role)
    if not ok:
        await msg.answer(DB_ERROR_TEXT)
        return
    await state.clear()
    if role == "full":
        await msg.answer("✅ *Пароль вірний!*\n\nОбери дію:", reply_markup=kb_main())
    else:
        await msg.answer(
            "✅ *Пароль вірний!*\n\nВам доступний лише розділ *🧮 Калькулятор*.",
            reply_markup=kb_calc_menu(role),
        )

@dp.message(F.text == "◀️ Головне меню")
async def back_to_main(msg: Message, state: FSMContext):
    role = await require_auth(msg, state)
    if not role:
        return
    await state.clear()
    if role == "full":
        await msg.answer("👋 *Товари*\n\nОбери дію:", reply_markup=kb_main())
    else:
        await msg.answer("🧮 *Калькулятор*\n\nОбери дію:", reply_markup=kb_calc_menu(role))

@dp.message(F.text == "➕ Новий товар")
async def new_product_start(msg: Message, state: FSMContext):
    role = await require_auth(msg, state, need_full=True)
    if not role: return
    await state.set_state(AddProduct.photo)
    await msg.answer("📷 Надішліть *фото товару*:", reply_markup=kb_cancel())

@dp.message(AddProduct.photo, F.text == "❌ Скасувати")
async def ap_photo_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("Скасовано.", reply_markup=kb_main())

@dp.message(AddProduct.photo, F.photo)
async def ap_photo(msg: Message, state: FSMContext):
    file_id = msg.photo[-1].file_id
    await state.update_data(photo_id=file_id)
    await state.set_state(AddProduct.name)
    await msg.answer("✅ Фото збережено.\n\n📝 Введіть *назву товару*:", reply_markup=kb_cancel())

@dp.message(AddProduct.photo)
async def ap_photo_invalid(msg: Message, state: FSMContext):
    await msg.answer("📷 Будь ласка, надішліть фото товару:", reply_markup=kb_cancel())

@dp.message(AddProduct.name)
async def ap_name(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    await state.update_data(name=msg.text.strip())
    await state.set_state(AddProduct.cost_price)
    await msg.answer("💵 Введіть *собівартість* (грн):", reply_markup=kb_cancel())

@dp.message(AddProduct.cost_price)
async def ap_cost_price(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    value = parse_float(msg.text)
    if value <= 0:
        return await msg.answer("⚠️ Введіть додатнє число, наприклад *150*:", reply_markup=kb_cancel())

    fd = await state.get_data()
    await state.clear()
    product = {
        "id":         await next_product_id(),
        "photo_id":   fd.get("photo_id", ""),
        "name":       fd.get("name", ""),
        "cost_price": value,
        "created_at": datetime.now().isoformat(),
        "created_by": msg.from_user.id,
    }
    await add_product(product)
    await msg.answer_photo(
        photo=product["photo_id"],
        caption=f"✅ *Товар додано!*\n\n{fmt_product(product)}",
        reply_markup=ikb_product_actions(product["id"]),
    )
    await msg.answer("Готово ✅", reply_markup=kb_main())

@dp.message(F.text == "📋 Всі товари")
async def list_products(msg: Message, state: FSMContext):
    role = await require_auth(msg, state, need_full=True)
    if not role: return
    try:
        uid = msg.from_user.id
        products = await get_all_products()
        user_list_cache[uid] = products
        if not products:
            return await msg.answer("📭 Товарів ще немає.", reply_markup=kb_main())
        await msg.answer(f"📋 *Товари* — {len(products)} шт.", reply_markup=kb_main())
        await msg.answer("Обери товар:", reply_markup=ikb_products_list(products))
    except Exception:
        logger.exception("list_products failed")
        await msg.answer(DB_ERROR_TEXT, reply_markup=kb_main())

@dp.callback_query(F.data.startswith("page:"))
async def page_products(cb: CallbackQuery):
    if not await require_full_cb(cb):
        return
    try:
        uid = cb.from_user.id
        page = int(cb.data.split(":")[1])
        products = user_list_cache.get(uid) or await get_all_products()
        user_list_cache[uid] = products
        await cb.message.edit_reply_markup(reply_markup=ikb_products_list(products, page))
        await cb.answer()
    except Exception:
        logger.exception("page_products failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data == "back_to_list")
async def back_to_list(cb: CallbackQuery):
    if not await require_full_cb(cb):
        return
    try:
        uid = cb.from_user.id
        products = await get_all_products()
        user_list_cache[uid] = products
        if not products:
            await cb.message.delete()
            await cb.message.answer("📭 Товарів ще немає.", reply_markup=kb_main())
            return await cb.answer()
        await cb.message.delete()
        await cb.message.answer("Обери товар:", reply_markup=ikb_products_list(products))
        await cb.answer()
    except Exception:
        logger.exception("back_to_list failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data == "noop")
async def noop(cb: CallbackQuery):
    await cb.answer()

@dp.callback_query(F.data.startswith("view:"))
async def view_product(cb: CallbackQuery):
    if not await require_full_cb(cb):
        return
    try:
        pid = int(cb.data.split(":")[1])
        product = await get_product(pid)
        if not product:
            return await cb.answer("Не знайдено!", show_alert=True)
        await cb.message.delete()
        await cb.message.answer_photo(
            photo=product.get("photo_id") or None,
            caption=fmt_product(product),
            reply_markup=ikb_product_actions(pid),
        )
        await cb.answer()
    except Exception:
        logger.exception("view_product failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data.startswith("sold:"))
async def sold_product(cb: CallbackQuery):
    if not await require_full_cb(cb):
        return
    try:
        pid = int(cb.data.split(":")[1])
        product = await get_product(pid)
        if not product:
            return await cb.answer("Не знайдено!", show_alert=True)
        await delete_product(pid)
        await cb.message.edit_caption(caption=f"✅ *Продано!* Товар №{pid} видалено зі складу.")
        await cb.answer("Продано!")
    except Exception:
        logger.exception("sold_product failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.message(F.text == "🔍 За назвою")
async def search_name_start(msg: Message, state: FSMContext):
    role = await require_auth(msg, state, need_full=True)
    if not role: return
    await state.set_state(SearchName.typing)
    await msg.answer("🔍 Введіть *назву товару* (або її частину):", reply_markup=kb_cancel())

@dp.message(SearchName.typing)
async def search_name_do(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    await state.clear()
    q = msg.text.strip().lower()
    all_products = await get_all_products()
    found = [p for p in all_products if q in str(p.get("name", "")).lower()]
    if not found:
        return await msg.answer(f"🔍 За запитом *{q}* нічого не знайдено.", reply_markup=kb_main())
    uid = msg.from_user.id
    user_list_cache[uid] = found
    await msg.answer(f"🔍 Знайдено: *{len(found)}*", reply_markup=kb_main())
    await msg.answer("Результати:", reply_markup=ikb_products_list(found))

@dp.message(F.text == "📤 CSV товарів")
async def export_products_csv(msg: Message, state: FSMContext):
    role = await require_auth(msg, state, need_full=True)
    if not role: return
    try:
        products = await get_all_products()
        if not products:
            return await msg.answer("📭 Товарів ще немає.", reply_markup=kb_main())

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["id", "name", "cost_price", "created_at"])
        for p in products:
            writer.writerow([p.get("id", ""), p.get("name", ""), p.get("cost_price", ""), p.get("created_at", "")])
        csv_bytes = output.getvalue().encode("utf-8-sig")
        filename = f"products_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        file = BufferedInputFile(csv_bytes, filename=filename)
        await msg.answer_document(document=file, caption=f"📤 Експорт товарів — {len(products)} шт.")
        await msg.answer("Готово ✅", reply_markup=kb_main())
    except Exception:
        logger.exception("export_products_csv failed")
        await msg.answer(DB_ERROR_TEXT, reply_markup=kb_main())

@dp.message(F.text == "🧮 Калькулятор")
async def calc_menu(msg: Message, state: FSMContext):
    role = await require_auth(msg, state)
    if not role: return
    await state.clear()
    await msg.answer("🧮 *Калькулятор*\n\nОбери дію:", reply_markup=kb_calc_menu(role))

@dp.message(F.text == "⚙️ Налаштування калькулятора")
async def calc_settings_menu(msg: Message, state: FSMContext):
    role = await require_auth(msg, state)
    if not role: return
    s = await get_calc_settings()
    text = (
        "⚙️ *Налаштування калькулятора*\n\n"
        f"✈️ Доставка авіа за 1 кг: *{s['price_per_kg_avia']:.2f} $*\n"
        f"🚢 Доставка морем за 1 кг: *{s['price_per_kg_sea']:.2f} $*\n"
        f"💱 Курс юаня: *{s['yuan_rate']:.2f} грн*\n"
        f"💵 Курс долара: *{s['usd_rate']:.2f} грн*\n\n"
        "Обери, що змінити:"
    )
    await msg.answer(text, reply_markup=ikb_calc_settings())

@dp.callback_query(F.data.startswith("calcset:"))
async def calcset_choose(cb: CallbackQuery, state: FSMContext):
    try:
        field = cb.data.split(":", 1)[1]
        await state.set_state(CalcSettingsEdit.typing)
        await state.update_data(calcset_field=field)
        await cb.message.answer(
            f"Введіть нове значення для {CALC_SETTING_LABELS.get(field, field)}:",
            reply_markup=kb_cancel(),
        )
        await cb.answer()
    except Exception:
        logger.exception("calcset_choose failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data == "calcset_recalc_all")
async def calcset_recalc_all_cb(cb: CallbackQuery):
    try:
        calcs = await get_all_calcs()
        await recalc_all_calcs()
        await cb.message.answer(f"🔄 Перераховано {len(calcs)} товар(ів).")
        await cb.answer("Готово!")
    except Exception:
        logger.exception("calcset_recalc_all_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.message(CalcSettingsEdit.typing)
async def calcset_save(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    fd = await state.get_data()
    field = fd["calcset_field"]
    value = parse_float(msg.text)
    if value <= 0:
        return await msg.answer("⚠️ Введіть додатнє число, наприклад *12.5*:", reply_markup=kb_cancel())
    await state.clear()
    await save_calc_setting(field, value)
    await recalc_all_calcs()
    s = await get_calc_settings()
    text = (
        "✅ Збережено!\n\n"
        f"✈️ Доставка авіа за 1 кг: *{s['price_per_kg_avia']:.2f} $*\n"
        f"🚢 Доставка морем за 1 кг: *{s['price_per_kg_sea']:.2f} $*\n"
        f"💱 Курс юаня: *{s['yuan_rate']:.2f} грн*\n"
        f"💵 Курс долара: *{s['usd_rate']:.2f} грн*"
    )
    await msg.answer(text, reply_markup=kb_calc_menu(role))

@dp.message(F.text == "➕ Порахувати товар")
async def calc_start(msg: Message, state: FSMContext):
    role = await require_auth(msg, state)
    if not role: return
    s = await get_calc_settings()
    if not all(s.values()):
        return await msg.answer(
            "⚠️ Спочатку задайте ціни доставки (авіа і морем, у $), курс юаня і курс долара "
            "в *⚙️ Налаштування калькулятора*.",
            reply_markup=kb_calc_menu(role),
        )
    await state.set_state(CalcProduct.photo)
    await msg.answer("📷 Надішліть *фото товару* (або пропустіть):", reply_markup=kb_skip_cancel())

@dp.message(CalcProduct.photo, F.photo)
async def calc_photo(msg: Message, state: FSMContext):
    file_id = msg.photo[-1].file_id
    await state.update_data(calc_photo_id=file_id)
    await state.set_state(CalcProduct.name)
    await msg.answer("📝 Введіть *назву товару*:", reply_markup=kb_cancel())

@dp.message(CalcProduct.photo)
async def calc_photo_text(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    if msg.text == "⏩ Пропустити":
        await state.update_data(calc_photo_id="")
        await state.set_state(CalcProduct.name)
        return await msg.answer("📝 Введіть *назву товару*:", reply_markup=kb_cancel())
    await msg.answer("📷 Надішліть фото або натисніть «⏩ Пропустити»:", reply_markup=kb_skip_cancel())

@dp.message(CalcProduct.name)
async def calc_name(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    await state.update_data(calc_name=msg.text.strip())
    await state.set_state(CalcProduct.description)
    await msg.answer("📝 Введіть *опис товару* (можна пропустити):", reply_markup=kb_skip_cancel())

@dp.message(CalcProduct.description)
async def calc_description(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    await state.update_data(calc_description="" if msg.text == "⏩ Пропустити" else msg.text.strip())
    await state.set_state(CalcProduct.link)
    await msg.answer("🔗 Введіть *посилання на товар* (можна пропустити):", reply_markup=kb_skip_cancel())

@dp.message(CalcProduct.link)
async def calc_link(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    if msg.text == "⏩ Пропустити":
        await state.update_data(calc_link="")
    else:
        value = msg.text.strip()
        if not is_valid_link(value):
            return await msg.answer(
                "⚠️ Посилання має починатись з *http://* або *https://*, або натисніть «⏩ Пропустити»:",
                reply_markup=kb_skip_cancel(),
            )
        await state.update_data(calc_link=value)
    await state.set_state(CalcProduct.rating)
    await msg.answer("⭐ Оцініть товар від *0 до 10* (можна пропустити):", reply_markup=kb_skip_cancel())

@dp.message(CalcProduct.rating)
async def calc_rating(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    if msg.text == "⏩ Пропустити":
        await state.update_data(calc_rating="0")
    else:
        try:
            rv = int(msg.text.strip())
        except ValueError:
            return await msg.answer("⚠️ Введіть ціле число від 0 до 10, або пропустіть:", reply_markup=kb_skip_cancel())
        if not (0 <= rv <= 10):
            return await msg.answer("⚠️ Оцінка має бути від 0 до 10:", reply_markup=kb_skip_cancel())
        await state.update_data(calc_rating=str(rv))
    await state.set_state(CalcProduct.price_yuan)
    await msg.answer("💴 Введіть *ціну товару в юанях*:", reply_markup=kb_cancel())

@dp.message(CalcProduct.price_yuan)
async def calc_price(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    value = parse_float(msg.text)
    if value <= 0:
        return await msg.answer("⚠️ Введіть додатнє число, наприклад *25.5*:", reply_markup=kb_cancel())
    await state.update_data(calc_price_yuan=value)
    await state.set_state(CalcProduct.weight)
    await msg.answer("⚖️ Введіть *вагу товару* (кг):", reply_markup=kb_cancel())

@dp.message(CalcProduct.weight)
async def calc_weight(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    weight = parse_float(msg.text)
    if weight <= 0:
        return await msg.answer("⚠️ Введіть додатнє число, наприклад *0.5*:", reply_markup=kb_cancel())

    fd = await state.get_data()
    name = fd["calc_name"]
    description = fd.get("calc_description", "")
    link = fd.get("calc_link", "")
    rating = fd.get("calc_rating", "0")
    price_yuan = fd["calc_price_yuan"]
    photo_id = fd.get("calc_photo_id", "")
    await state.clear()

    cid = await next_calc_id()
    record = {
        "id":          cid,
        "name":        name,
        "description": description,
        "link":        link,
        "rating":      rating,
        "photo_id":    photo_id,
        "price_yuan":  price_yuan,
        "weight_kg":   weight,
        "created_at":  datetime.now().isoformat(),
        "created_by":  msg.from_user.id,
    }
    await add_calc(record)
    record = await recalc_calc_costs(cid)

    if record:
        if record.get("photo_id"):
            await msg.answer_photo(photo=record["photo_id"], caption=fmt_calc(record), reply_markup=ikb_calc_actions(record["id"], record))
        else:
            await msg.answer(fmt_calc(record), reply_markup=ikb_calc_actions(record["id"], record))
    await msg.answer("Готово ✅ Розрахунок збережено.", reply_markup=kb_calc_menu(role))

@dp.message(F.text == "📄 Мої розрахунки")
async def list_calcs(msg: Message, state: FSMContext):
    role = await require_auth(msg, state)
    if not role: return
    await recalc_all_calcs()
    calcs = await get_all_calcs()
    if not calcs:
        return await msg.answer("📭 Розрахунків ще немає.", reply_markup=kb_calc_menu(role))
    await msg.answer(f"📄 *Розрахунки* — {len(calcs)} шт.", reply_markup=kb_calc_menu(role))
    for c in calcs:
        if c.get("photo_id"):
            await msg.answer_photo(photo=c["photo_id"], caption=fmt_calc(c), reply_markup=ikb_calc_actions(c["id"], c))
        else:
            await msg.answer(fmt_calc(c), reply_markup=ikb_calc_actions(c["id"], c))

@dp.callback_query(F.data.startswith("editcalc:"))
async def editcalc_cb(cb: CallbackQuery):
    try:
        cid = int(cb.data.split(":")[1])
        await cb.message.answer(f"✏️ *Редагування розрахунку №{cid}*\nОберіть поле:", reply_markup=ikb_calc_edit_fields(cid))
        await cb.answer()
    except Exception:
        logger.exception("editcalc_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data.startswith("editcalcfield:"))
async def editcalcfield_choose(cb: CallbackQuery, state: FSMContext):
    try:
        _, cid_s, field = cb.data.split(":", 2)
        cid = int(cid_s)
        await state.set_state(CalcEditField.typing)
        await state.update_data(edit_cid=cid, edit_field=field)
        labels = {
            "name": "Назву", "description": "Опис", "link": "Посилання на товар",
            "rating": "Оцінку (0-10)", "price_yuan": "Ціну товару (¥)", "weight_kg": "Вагу (кг)",
        }
        hint = ""
        if field == "link":
            hint = "\n_Має починатись з http:// або https://. Щоб прибрати посилання — надішліть «-»._"
        elif field == "description":
            hint = "\n_Щоб прибрати опис — надішліть «-»._"
        await cb.message.answer(f"Введіть нове значення для *{labels.get(field, field)}*:{hint}", reply_markup=kb_cancel())
        await cb.answer()
    except Exception:
        logger.exception("editcalcfield_choose failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.message(CalcEditField.typing)
async def editcalcfield_save(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    fd = await state.get_data()
    cid, field = fd["edit_cid"], fd["edit_field"]
    value = msg.text.strip()
    update_fields = {}

    if field in ("description", "link") and value == "-":
        value = ""
    if field == "link" and value and not is_valid_link(value):
        return await msg.answer(
            "⚠️ Посилання має починатись з *http://* або *https://*, або надішліть «-»:",
            reply_markup=kb_cancel(),
        )
    if field == "rating":
        try:
            rv = int(value)
        except ValueError:
            return await msg.answer("⚠️ Введіть ціле число від 0 до 10:", reply_markup=kb_cancel())
        if not (0 <= rv <= 10):
            return await msg.answer("⚠️ Оцінка має бути від 0 до 10:", reply_markup=kb_cancel())
        value = str(rv)
    if field in ("price_yuan", "weight_kg"):
        fv = parse_float(value)
        if fv <= 0:
            return await msg.answer("⚠️ Введіть додатнє число:", reply_markup=kb_cancel())
        update_fields[field] = fv
    else:
        update_fields[field] = value

    await state.clear()
    await update_calc(cid, update_fields)
    if field in ("price_yuan", "weight_kg"):
        await recalc_calc_costs(cid)
    record = await get_calc(cid)
    if record:
        if record.get("photo_id"):
            await msg.answer_photo(photo=record["photo_id"], caption=f"✅ Оновлено!\n\n{fmt_calc(record)}", reply_markup=ikb_calc_actions(cid, record))
        else:
            await msg.answer(f"✅ Оновлено!\n\n{fmt_calc(record)}", reply_markup=ikb_calc_actions(cid, record))
        await msg.answer("Готово ✅", reply_markup=kb_calc_menu(role))
    else:
        await msg.answer("Розрахунок не знайдено.", reply_markup=kb_calc_menu(role))

@dp.callback_query(F.data.startswith("delcalc:"))
async def delete_calc_cb(cb: CallbackQuery):
    try:
        cid = int(cb.data.split(":")[1])
        await delete_calc(cid)
        text = f"🗑 Розрахунок №{cid} видалено."
        if cb.message.photo:
            await cb.message.edit_caption(caption=text)
        else:
            await cb.message.edit_text(text)
        await cb.answer("Видалено!")
    except Exception:
        logger.exception("delete_calc_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

async def _proceed_to_choosing(target, state: FSMContext, filtered: list, uid: int, role: str):
    if not filtered:
        await state.clear()
        send_calc_cache.pop(uid, None)
        return await target.answer("📭 Немає розрахунків за цим критерієм.", reply_markup=kb_calc_menu(role))
    send_calc_cache[uid] = filtered
    await state.set_state(SendCalc.choosing)
    await state.update_data(send_selected=[])
    await target.answer(
        f"Знайдено *{len(filtered)}* — оберіть потрібні кнопками нижче (можна декілька, або «Обрати всі»):",
        reply_markup=kb_cancel(),
    )
    await target.answer("Список розрахунків:", reply_markup=ikb_send_calc_list(filtered, []))

@dp.message(F.text == "📤 Переслати/Зберегти")
async def send_calc_start(msg: Message, state: FSMContext):
    role = await require_auth(msg, state)
    if not role: return
    calcs = await get_all_calcs()
    if not calcs:
        return await msg.answer("📭 Розрахунків ще немає.", reply_markup=kb_calc_menu(role))
    await state.set_state(SendCalc.filter_choice)
    await msg.answer("📤 Оберіть критерій фільтра розрахунків нижче.", reply_markup=kb_cancel())
    await msg.answer("Критерій:", reply_markup=ikb_send_calc_filter())

@dp.message(SendCalc.filter_choice)
async def send_calc_filter_text(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    await msg.answer("Оберіть критерій кнопками вище, або натисніть ❌ Скасувати.", reply_markup=kb_cancel())

@dp.callback_query(F.data.startswith("filtercalc:"), SendCalc.filter_choice)
async def filtercalc_choose(cb: CallbackQuery, state: FSMContext):
    try:
        criterion = cb.data.split(":", 1)[1]
        uid = cb.from_user.id
        role = cached_role(uid)
        calcs = await get_all_calcs()
        if criterion == "all":
            await _proceed_to_choosing(cb.message, state, calcs, uid, role)
        elif criterion == "rating":
            await state.set_state(SendCalc.filter_rating)
            await cb.message.answer("⭐ Введіть мінімальний рейтинг (0-10):", reply_markup=kb_cancel())
        await cb.answer()
    except Exception:
        logger.exception("filtercalc_choose failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.message(SendCalc.filter_rating)
async def filter_rating_do(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    try:
        threshold = int(msg.text.strip())
    except ValueError:
        return await msg.answer("⚠️ Введіть ціле число від 0 до 10:", reply_markup=kb_cancel())
    if not (0 <= threshold <= 10):
        return await msg.answer("⚠️ Рейтинг має бути від 0 до 10:", reply_markup=kb_cancel())
    calcs = await get_all_calcs()
    filtered = [c for c in calcs if parse_int(c.get("rating", 0)) >= threshold]
    await _proceed_to_choosing(msg, state, filtered, msg.from_user.id, role)

@dp.callback_query(F.data.startswith("selcalc:"), SendCalc.choosing)
async def toggle_selcalc(cb: CallbackQuery, state: FSMContext):
    try:
        cid = int(cb.data.split(":")[1])
        fd = await state.get_data()
        selected = set(fd.get("send_selected", []))
        if cid in selected:
            selected.discard(cid)
        else:
            selected.add(cid)
        await state.update_data(send_selected=list(selected))
        uid = cb.from_user.id
        calcs = send_calc_cache.get(uid) or await get_all_calcs()
        send_calc_cache[uid] = calcs
        await cb.message.edit_reply_markup(reply_markup=ikb_send_calc_list(calcs, selected))
        await cb.answer()
    except Exception:
        logger.exception("toggle_selcalc failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data == "selcalc_all", SendCalc.choosing)
async def select_all_calc(cb: CallbackQuery, state: FSMContext):
    try:
        uid = cb.from_user.id
        calcs = send_calc_cache.get(uid) or await get_all_calcs()
        send_calc_cache[uid] = calcs
        all_ids = [c["id"] for c in calcs]
        await state.update_data(send_selected=all_ids)
        await cb.message.edit_reply_markup(reply_markup=ikb_send_calc_list(calcs, set(all_ids)))
        await cb.answer("Обрано всі")
    except Exception:
        logger.exception("select_all_calc failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data == "selcalc_cancel", SendCalc.choosing)
async def selcalc_cancel(cb: CallbackQuery, state: FSMContext):
    try:
        role = cached_role(cb.from_user.id)
        await state.clear()
        send_calc_cache.pop(cb.from_user.id, None)
        try:
            await cb.message.delete()
        except TelegramAPIError:
            pass
        await cb.message.answer("Скасовано.", reply_markup=kb_calc_menu(role))
        await cb.answer()
    except Exception:
        logger.exception("selcalc_cancel failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data == "selcalc_done", SendCalc.choosing)
async def selcalc_done(cb: CallbackQuery, state: FSMContext):
    try:
        fd = await state.get_data()
        selected = fd.get("send_selected", [])
        if not selected:
            return await cb.answer("Оберіть хоча б один розрахунок!", show_alert=True)
        await state.set_state(SendCalc.action_choice)
        await cb.message.answer("Що зробити з обраними розрахунками?", reply_markup=ikb_send_calc_action())
        await cb.answer()
    except Exception:
        logger.exception("selcalc_done failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.message(SendCalc.action_choice)
async def send_calc_action_text(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear()
        send_calc_cache.pop(msg.from_user.id, None)
        return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    await msg.answer("Оберіть дію кнопками вище, або натисніть ❌ Скасувати.", reply_markup=kb_cancel())

@dp.callback_query(F.data == "calcaction:send", SendCalc.action_choice)
async def calcaction_send(cb: CallbackQuery, state: FSMContext):
    try:
        await state.set_state(SendCalc.waiting_username)
        await cb.message.answer(
            "👤 Введіть *username* отримувача (наприклад, @username).\n"
            "Отримувач має бути хоча б раз написати боту /start.",
            reply_markup=kb_cancel(),
        )
        await cb.answer()
    except Exception:
        logger.exception("calcaction_send failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data == "calcaction:save", SendCalc.action_choice)
async def calcaction_save(cb: CallbackQuery, state: FSMContext):
    try:
        fd = await state.get_data()
        selected_ids = set(fd.get("send_selected", []))
        role = cached_role(cb.from_user.id)
        await state.clear()
        uid = cb.from_user.id
        send_calc_cache.pop(uid, None)
        all_calcs = await get_all_calcs()
        to_save = [c for c in all_calcs if c["id"] in selected_ids]
        if not to_save:
            return await cb.message.answer("⚠️ Обрані розрахунки не знайдено.", reply_markup=kb_calc_menu(role))
        csv_bytes = build_calc_csv(to_save)
        filename = f"calcs_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        file = BufferedInputFile(csv_bytes, filename=filename)
        await cb.message.answer_document(document=file, caption=f"💾 Збережено — {len(to_save)} шт.")
        await cb.message.answer("Готово ✅", reply_markup=kb_calc_menu(role))
        await cb.answer()
    except Exception:
        logger.exception("calcaction_save failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data == "calcaction:cancel", SendCalc.action_choice)
async def calcaction_cancel(cb: CallbackQuery, state: FSMContext):
    try:
        role = cached_role(cb.from_user.id)
        await state.clear()
        send_calc_cache.pop(cb.from_user.id, None)
        await cb.message.answer("Скасовано.", reply_markup=kb_calc_menu(role))
        await cb.answer()
    except Exception:
        logger.exception("calcaction_cancel failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.message(SendCalc.waiting_username)
async def send_calc_do(msg: Message, state: FSMContext):
    uid = msg.from_user.id
    role = cached_role(uid)
    if msg.text == "❌ Скасувати":
        await state.clear()
        send_calc_cache.pop(uid, None)
        return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))

    username = msg.text.strip()
    fd = await state.get_data()
    selected_ids = set(fd.get("send_selected", []))
    await state.clear()
    send_calc_cache.pop(uid, None)

    target_uid = await get_uid_by_username(username)
    if not target_uid:
        return await msg.answer(
            f"⚠️ Користувача {username} не знайдено.\n"
            "Отримувач має спершу написати боту /start (тоді бот зможе йому щось надіслати).",
            reply_markup=kb_calc_menu(role),
        )

    all_calcs = await get_all_calcs()
    to_send = [c for c in all_calcs if c["id"] in selected_ids]
    if not to_send:
        return await msg.answer("⚠️ Обрані розрахунки не знайдено (можливо, вже видалені).", reply_markup=kb_calc_menu(role))

    sent = 0
    for c in to_send:
        try:
            if c.get("photo_id"):
                await bot.send_photo(chat_id=target_uid, photo=c["photo_id"], caption=fmt_calc(c))
            else:
                await bot.send_message(chat_id=target_uid, text=fmt_calc(c))
            sent += 1
        except TelegramAPIError:
            logger.exception("Не вдалось надіслати розрахунок №%s", c.get("id"))

    await msg.answer(f"📤 Надіслано {sent}/{len(to_send)} розрахунків користувачу {username}.", reply_markup=kb_calc_menu(role))

@dp.message(F.text == "🧺 Кошики")
async def list_baskets(msg: Message, state: FSMContext):
    role = await require_auth(msg, state)
    if not role: return
    await state.clear()
    try:
        await recalc_all_calcs()
        baskets = await get_all_baskets()
        all_ids = []
        for b in baskets:
            all_ids.extend(it["calc_id"] for it in b.get("items", []))
        calcs_by_id = await calcs_map_by_id(all_ids)
        text = f"🧺 *Кошики* — {len(baskets)} шт." if baskets else "📭 Кошиків ще немає."
        await msg.answer(text, reply_markup=kb_calc_menu(role))
        await msg.answer("Обери кошик або створи новий:", reply_markup=ikb_baskets_list(baskets, calcs_by_id))
    except Exception:
        logger.exception("list_baskets failed")
        await msg.answer(DB_ERROR_TEXT, reply_markup=kb_calc_menu(role))

@dp.callback_query(F.data == "basket_new")
async def basket_new_start(cb: CallbackQuery, state: FSMContext):
    try:
        await state.set_state(BasketCreate.name)
        await cb.message.answer("📝 Введіть *назву кошика* (наприклад «Замовлення для клієнта»):", reply_markup=kb_cancel())
        await cb.answer()
    except Exception:
        logger.exception("basket_new_start failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.message(BasketCreate.name)
async def basket_new_name(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    await state.update_data(basket_name=msg.text.strip())
    await state.set_state(BasketCreate.budget)
    await msg.answer("💰 Введіть *бюджет* кошика (грн):", reply_markup=kb_cancel())

@dp.message(BasketCreate.budget)
async def basket_new_budget(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    budget = parse_float(msg.text)
    if budget <= 0:
        return await msg.answer("⚠️ Введіть додатнє число, наприклад *5000*:", reply_markup=kb_cancel())
    fd = await state.get_data()
    await state.clear()
    bid = await next_basket_id()
    basket = {
        "id":         bid,
        "name":       fd.get("basket_name", ""),
        "budget":     budget,
        "items":      [],
        "created_at": datetime.now().isoformat(),
        "created_by": msg.from_user.id,
    }
    await add_basket(basket)
    await msg.answer("✅ *Кошик створено!*", reply_markup=kb_calc_menu(role))
    await show_basket(msg, bid)

@dp.callback_query(F.data.startswith("basket_view:"))
async def basket_view_cb(cb: CallbackQuery):
    try:
        bid = int(cb.data.split(":")[1])
        await show_basket(cb, bid)
        await cb.answer()
    except Exception:
        logger.exception("basket_view_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data == "basket_back")
async def basket_back_cb(cb: CallbackQuery):
    try:
        baskets = await get_all_baskets()
        all_ids = []
        for b in baskets:
            all_ids.extend(it["calc_id"] for it in b.get("items", []))
        calcs_by_id = await calcs_map_by_id(all_ids)
        text = f"🧺 *Кошики* — {len(baskets)} шт." if baskets else "📭 Кошиків ще немає."
        try:
            await cb.message.edit_text(text, reply_markup=ikb_baskets_list(baskets, calcs_by_id))
        except TelegramAPIError:
            await cb.message.answer(text, reply_markup=ikb_baskets_list(baskets, calcs_by_id))
        await cb.answer()
    except Exception:
        logger.exception("basket_back_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data.startswith("basket_del:"))
async def basket_del_cb(cb: CallbackQuery):
    try:
        bid = int(cb.data.split(":")[1])
        await delete_basket(bid)
        text = f"🗑 Кошик №{bid} видалено."
        try:
            await cb.message.edit_text(text)
        except TelegramAPIError:
            await cb.message.answer(text)
        await cb.answer("Видалено!")
    except Exception:
        logger.exception("basket_del_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data.startswith("basket_edit:"))
async def basket_edit_start(cb: CallbackQuery):
    try:
        bid = int(cb.data.split(":")[1])
        b = await get_basket(bid)
        if not b:
            return await cb.answer("Кошик не знайдено!", show_alert=True)
        await cb.message.answer(
            f"✏️ *Редагування кошика №{bid} — {b.get('name','')}*\nОберіть, що змінити:",
            reply_markup=ikb_basket_edit_fields(bid),
        )
        await cb.answer()
    except Exception:
        logger.exception("basket_edit_start failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data.startswith("basketeditfield:"))
async def basketeditfield_choose(cb: CallbackQuery, state: FSMContext):
    try:
        _, bid_s, field = cb.data.split(":", 2)
        bid = int(bid_s)
        await state.set_state(BasketEditField.typing)
        await state.update_data(bef_bid=bid, bef_field=field)
        label = "назву кошика" if field == "name" else "бюджет кошика (грн)"
        await cb.message.answer(f"Введіть нове значення для {label}:", reply_markup=kb_cancel())
        await cb.answer()
    except Exception:
        logger.exception("basketeditfield_choose failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.message(BasketEditField.typing)
async def basketeditfield_save(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    fd = await state.get_data()
    bid, field = fd["bef_bid"], fd["bef_field"]

    if field == "name":
        value = msg.text.strip()
        if not value:
            return await msg.answer("⚠️ Назва не може бути порожньою:", reply_markup=kb_cancel())
        await update_basket(bid, {"name": value})
    else:
        value = parse_float(msg.text)
        if value <= 0:
            return await msg.answer("⚠️ Введіть додатнє число, наприклад *5000*:", reply_markup=kb_cancel())
        await update_basket(bid, {"budget": value})

    await state.clear()
    await msg.answer("✅ Кошик оновлено!", reply_markup=kb_calc_menu(role))
    await show_basket(msg, bid)

@dp.callback_query(F.data.startswith("basket_add:"))
async def basket_add_cb(cb: CallbackQuery):
    try:
        _, bid_s, page_s = cb.data.split(":")
        bid, page = int(bid_s), int(page_s)
        b = await get_basket(bid)
        if not b:
            return await cb.answer("Кошик не знайдено!", show_alert=True)
        calcs = await get_all_calcs()
        if not calcs:
            return await cb.answer("Немає жодного розрахунку. Спочатку створіть товар у калькуляторі.", show_alert=True)
        in_basket = {it["calc_id"] for it in b.get("items", [])}
        text = f"➕ *Додавання товарів у кошик №{bid}*\nОберіть товар зі списку (показано ціну ✈️авіа/🚢море):"
        try:
            await cb.message.edit_text(text, reply_markup=ikb_basket_add_list(bid, calcs, in_basket, page))
        except TelegramAPIError:
            await cb.message.answer(text, reply_markup=ikb_basket_add_list(bid, calcs, in_basket, page))
        await cb.answer()
    except Exception:
        logger.exception("basket_add_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data.startswith("basket_addsel:"))
async def basket_addsel_cb(cb: CallbackQuery, state: FSMContext):
    try:
        _, bid_s, cid_s, page_s = cb.data.split(":")
        bid, cid = int(bid_s), int(cid_s)
        b = await get_basket(bid)
        if not b:
            return await cb.answer("Кошик не знайдено!", show_alert=True)
        await send_basket_item_prompt(cb.message, state, bid, cid)
        await cb.answer()
    except Exception:
        logger.exception("basket_addsel_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data.startswith("basket_rmlist:"))
async def basket_rmlist_cb(cb: CallbackQuery):
    try:
        bid = int(cb.data.split(":")[1])
        b = await get_basket(bid)
        if not b:
            return await cb.answer("Кошик не знайдено!", show_alert=True)
        items = b.get("items", [])
        if not items:
            return await cb.answer("У кошику ще немає товарів.", show_alert=True)
        calcs_by_id = await calcs_map_by_id([it["calc_id"] for it in items])
        text = f"✏️ *Редагування позицій кошика №{bid}*\nОберіть позицію:"
        try:
            await cb.message.edit_text(text, reply_markup=ikb_basket_remove_list(bid, items, calcs_by_id))
        except TelegramAPIError:
            await cb.message.answer(text, reply_markup=ikb_basket_remove_list(bid, items, calcs_by_id))
        await cb.answer()
    except Exception:
        logger.exception("basket_rmlist_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data.startswith("basket_item_open:"))
async def basket_item_open_cb(cb: CallbackQuery):
    try:
        _, bid_s, item_id = cb.data.split(":", 2)
        bid = int(bid_s)
        b = await get_basket(bid)
        if not b:
            return await cb.answer("Кошик не знайдено!", show_alert=True)
        item = next((it for it in b.get("items", []) if it["item_id"] == item_id), None)
        if not item:
            return await cb.answer("Позицію не знайдено!", show_alert=True)
        calc = await get_calc(item["calc_id"])
        name = calc.get("name", "") if calc else "⚠️ товар видалено"
        method_label = DELIVERY_METHOD_LABELS.get(item["method"], "—")
        text = (
            f"№{item['calc_id']} — {name}\n"
            f"🔢 Кількість: *{item.get('qty',1)} шт.*\n"
            f"🚚 Доставка: {method_label}"
        )
        await cb.message.answer(text, reply_markup=ikb_basket_item_actions(bid, item_id))
        await cb.answer()
    except Exception:
        logger.exception("basket_item_open_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data.startswith("basket_item_edit:"))
async def basket_item_edit_start(cb: CallbackQuery, state: FSMContext):
    try:
        _, bid_s, item_id = cb.data.split(":", 2)
        bid = int(bid_s)
        b = await get_basket(bid)
        if not b:
            return await cb.answer("Кошик не знайдено!", show_alert=True)
        item = next((it for it in b.get("items", []) if it["item_id"] == item_id), None)
        if not item:
            return await cb.answer("Позицію не знайдено!", show_alert=True)
        calc = await get_calc(item["calc_id"])
        if not calc:
            return await cb.answer("Товар за цією позицією не знайдено.", show_alert=True)
        await state.set_state(BasketItemEdit.quantity)
        await state.update_data(bie_bid=bid, bie_item_id=item_id, bie_cid=item["calc_id"])
        await cb.message.answer(
            f"🔢 Поточна кількість: *{item.get('qty',1)} шт.*\nВведіть нову кількість:",
            reply_markup=kb_cancel(),
        )
        await cb.answer()
    except Exception:
        logger.exception("basket_item_edit_start failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.message(BasketItemEdit.quantity)
async def basket_item_edit_qty(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    qty = parse_int(msg.text)
    if qty <= 0:
        return await msg.answer("⚠️ Введіть ціле додатне число, наприклад *2*:", reply_markup=kb_cancel())
    fd = await state.get_data()
    cid = fd["bie_cid"]
    calc = await get_calc(cid)
    if not calc:
        await state.clear()
        return await msg.answer("⚠️ Товар не знайдено.", reply_markup=kb_calc_menu(role))
    await state.update_data(bie_qty=qty)
    await state.set_state(BasketItemEdit.method)
    avia_total = parse_float(calc.get("cost_price_uah_avia", 0)) * qty
    sea_total = parse_float(calc.get("cost_price_uah_sea", 0)) * qty
    text = (
        f"🔢 Кількість: *{qty} шт.*\n\n"
        f"✈️ Авіа — *{avia_total:,.0f} грн*\n"
        f"🚢 Море — *{sea_total:,.0f} грн*\n\n"
        "Оберіть спосіб доставки:"
    )
    await msg.answer(text, reply_markup=ikb_basket_item_method())

@dp.callback_query(F.data.startswith("bimethod:"), BasketItemEdit.method)
async def basket_item_edit_method(cb: CallbackQuery, state: FSMContext):
    try:
        action = cb.data.split(":", 1)[1]
        role = cached_role(cb.from_user.id)
        if action == "cancel":
            await state.clear()
            await cb.message.answer("Скасовано.", reply_markup=kb_calc_menu(role))
            return await cb.answer()

        method = action
        fd = await state.get_data()
        bid, item_id, qty = fd["bie_bid"], fd["bie_item_id"], fd["bie_qty"]
        await state.clear()

        b = await get_basket(bid)
        if not b:
            await cb.message.answer("⚠️ Кошик не знайдено.", reply_markup=kb_calc_menu(role))
            return await cb.answer()

        await basket_update_item(bid, item_id, {"qty": qty, "method": method})
        await cb.message.answer("✅ Позицію оновлено!", reply_markup=kb_calc_menu(role))
        await show_basket(cb.message, bid)
        await cb.answer("Оновлено!")
    except Exception:
        logger.exception("basket_item_edit_method failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data.startswith("basket_rm:"))
async def basket_rm_cb(cb: CallbackQuery):
    try:
        _, bid_s, item_id = cb.data.split(":", 2)
        bid = int(bid_s)
        await basket_remove_item(bid, item_id)
        await cb.answer("Прибрано!")
        await show_basket(cb.message, bid)
    except Exception:
        logger.exception("basket_rm_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data.startswith("tobasket:"))
async def tobasket_cb(cb: CallbackQuery):
    try:
        cid = int(cb.data.split(":")[1])
        baskets = await get_all_baskets()
        if not baskets:
            await cb.message.answer("📭 Кошиків ще немає. Створіть новий:", reply_markup=ikb_pick_basket(cid, []))
            return await cb.answer()
        await cb.message.answer(f"Оберіть кошик для товару №{cid}:", reply_markup=ikb_pick_basket(cid, baskets))
        await cb.answer()
    except Exception:
        logger.exception("tobasket_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data.startswith("tobasketsel:"))
async def tobasketsel_cb(cb: CallbackQuery, state: FSMContext):
    try:
        _, cid_s, bid_s = cb.data.split(":")
        cid, bid = int(cid_s), int(bid_s)
        b = await get_basket(bid)
        if not b:
            return await cb.answer("Кошик не знайдено!", show_alert=True)
        await send_basket_item_prompt(cb.message, state, bid, cid)
        await cb.answer()
    except Exception:
        logger.exception("tobasketsel_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.message(BasketItemAdd.quantity)
async def basket_item_qty(msg: Message, state: FSMContext):
    role = cached_role(msg.from_user.id)
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu(role))
    qty = parse_int(msg.text)
    if qty <= 0:
        return await msg.answer("⚠️ Введіть ціле додатне число, наприклад *2*:", reply_markup=kb_cancel())
    fd = await state.get_data()
    bid, cid = fd["bi_bid"], fd["bi_cid"]
    calc = await get_calc(cid)
    if not calc:
        await state.clear()
        return await msg.answer("⚠️ Товар не знайдено.", reply_markup=kb_calc_menu(role))
    await state.update_data(bi_qty=qty)
    await state.set_state(BasketItemAdd.method)
    avia_total = parse_float(calc.get("cost_price_uah_avia", 0)) * qty
    sea_total = parse_float(calc.get("cost_price_uah_sea", 0)) * qty
    text = (
        f"🔢 Кількість: *{qty} шт.*\n\n"
        f"✈️ Авіа — *{avia_total:,.0f} грн*\n"
        f"🚢 Море — *{sea_total:,.0f} грн*\n\n"
        "Оберіть спосіб доставки:"
    )
    await msg.answer(text, reply_markup=ikb_basket_item_method())

@dp.callback_query(F.data.startswith("bimethod:"), BasketItemAdd.method)
async def basket_item_method(cb: CallbackQuery, state: FSMContext):
    try:
        action = cb.data.split(":", 1)[1]
        role = cached_role(cb.from_user.id)
        if action == "cancel":
            await state.clear()
            await cb.message.answer("Скасовано.", reply_markup=kb_calc_menu(role))
            return await cb.answer()

        method = action
        fd = await state.get_data()
        bid, cid, qty = fd["bi_bid"], fd["bi_cid"], fd["bi_qty"]
        await state.clear()

        calc = await get_calc(cid)
        b = await get_basket(bid)
        if not calc or not b:
            await cb.message.answer("⚠️ Товар або кошик не знайдено.", reply_markup=kb_calc_menu(role))
            return await cb.answer()

        item_id = uuid.uuid4().hex[:8]
        item = {"item_id": item_id, "calc_id": cid, "qty": qty, "method": method}
        await basket_add_item(bid, item)

        await cb.message.answer(f"✅ Додано в кошик №{bid}!", reply_markup=kb_calc_menu(role))
        await show_basket(cb.message, bid)
        await cb.answer("Додано!")
    except Exception:
        logger.exception("basket_item_method failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.message()
async def fallback_message_handler(msg: Message, state: FSMContext):
    role = await require_auth(msg, state)
    if not role:
        return
    await state.clear()
    if role == "full":
        await msg.answer("🤔 Не розпізнав цю дію. Повертаю в головне меню.", reply_markup=kb_main())
    else:
        await msg.answer("🤔 Не розпізнав цю дію. Повертаю в меню калькулятора.", reply_markup=kb_calc_menu(role))

@dp.callback_query()
async def fallback_callback_handler(cb: CallbackQuery, state: FSMContext):
    try:
        await cb.answer("⚠️ Ця дія вже неактуальна. Скористайтесь меню.", show_alert=True)
    except TelegramAPIError:
        pass

from aiohttp import web

async def health(request):
    try:
        await mongo_client.admin.command("ping")
        return web.Response(text="OK")
    except Exception:
        return web.Response(text="DB_DOWN", status=503)

async def main():
    init_mongo()
    try:
        await mongo_client.admin.command("ping")
        logger.info("MongoDB connection OK")
    except Exception:
        logger.exception("MongoDB connection FAILED at startup")

    await load_user_roles()
    await recalc_all_calcs()

    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.environ.get("PORT", 8080)))
    await site.start()

    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Бот товарів запущено (MongoDB, без Cloudinary)...")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())