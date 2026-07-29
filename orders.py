import asyncio
import logging
import os
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
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
)
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("products_bot")

BOT_TOKEN    = os.environ["BOT_TOKEN"]
BOT_PASSWORD = os.environ["BOT_PASSWORD"]
MONGO_URI    = os.environ["MONGO_URI"]

mongo_client: AsyncIOMotorClient | None = None
db = None
products_col = None
settings_col = None
auth_col = None
calc_col = None

def init_mongo():
    global mongo_client, db, products_col, settings_col, auth_col, calc_col
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

DB_ERROR_TEXT = "⚠️ Тимчасова проблема з базою даних. Спробуйте ще раз через кілька секунд."

async def db_call(coro, default=None):
    try:
        return await coro
    except PyMongoError:
        logger.exception("MongoDB error")
        return default

# ---------------------------------------------------------------------------
# Auth. Кешуємо авторизованих юзерів у пам'яті процесу, щоб уникнути ситуації,
# коли одразу після успішного логіну наступний read з MongoDB (наприклад, з
# secondary-репліки, яка ще не встигла реплікувати запис) не знаходить щойно
# створений документ і бот знову просить пароль. Кеш заповнюється при старті
# і оновлюється відразу після кожної успішної авторизації.
# ---------------------------------------------------------------------------
authorized_uids: set[int] = set()

async def load_authorized_uids():
    global authorized_uids
    try:
        docs = await auth_col.find({}, {"uid": 1}).to_list(length=None)
        authorized_uids = {d["uid"] for d in docs}
        logger.info("Завантажено %d авторизованих користувачів у кеш", len(authorized_uids))
    except PyMongoError:
        logger.exception("Не вдалось завантажити список авторизованих користувачів")

async def is_authorized(uid: int):
    if uid in authorized_uids:
        return True
    try:
        doc = await auth_col.find_one({"uid": uid})
    except PyMongoError:
        logger.exception("MongoDB error in is_authorized")
        return None
    if doc is not None:
        authorized_uids.add(uid)
        return True
    return False

async def authorize(uid: int) -> bool:
    try:
        await auth_col.update_one({"uid": uid}, {"$set": {"uid": uid}}, upsert=True)
        authorized_uids.add(uid)
        return True
    except PyMongoError:
        logger.exception("MongoDB error in authorize")
        return False

async def get_categories() -> list:
    doc = await db_call(settings_col.find_one({"_id": "categories"}))
    return doc["values"] if doc else []

async def save_categories(categories: list):
    await db_call(settings_col.update_one({"_id": "categories"}, {"$set": {"values": categories}}, upsert=True))

async def next_product_id() -> int:
    doc = await db_call(products_col.find_one(sort=[("id", -1)]))
    return (doc["id"] + 1) if doc else 1

async def add_product(product: dict):
    await db_call(products_col.insert_one(product))

async def get_product(pid: int) -> dict | None:
    return await db_call(products_col.find_one({"id": pid}, {"_id": 0}))

async def update_product(pid: int, fields: dict):
    await db_call(products_col.update_one({"id": pid}, {"$set": fields}))

async def delete_product(pid: int):
    await db_call(products_col.delete_one({"id": pid}))

async def get_all_products() -> list:
    cursor = products_col.find({}, {"_id": 0}).sort("id", -1)
    return await db_call(cursor.to_list(length=None), default=[]) or []

# ---------------------------------------------------------------------------
# Калькулятор: налаштування (ціна доставки за 1 кг, курс юаня, курс долара)
# ---------------------------------------------------------------------------
async def get_calc_settings() -> dict:
    doc = await db_call(settings_col.find_one({"_id": "calc_settings"}))
    defaults = {"price_per_kg": 0.0, "yuan_rate": 0.0, "usd_rate": 0.0}
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

async def delete_calc(cid: int):
    await db_call(calc_col.delete_one({"id": cid}))

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

def fmt_product(p: dict) -> str:
    cost = parse_float(p.get("cost_price", 0))
    sale = parse_float(p.get("sale_price", 0))
    margin = sale - cost
    lines = [
        f"*№{p['id']} — {p.get('name','')}*",
        f"🔖 Артикул: `{p.get('sku','—')}`",
        f"🏷 Категорія: *{p.get('category','—')}*",
    ]
    if p.get("size"):
        lines.append(f"📏 Розмір: *{p['size']}*")
    if p.get("color"):
        lines.append(f"🎨 Колір: *{p['color']}*")
    if p.get("description"):
        lines.append(f"📝 {p['description']}")
    lines += [
        f"💵 Собівартість: *{cost:,.0f} грн*",
        f"💰 Ціна продажу: *{sale:,.0f} грн*",
        f"📈 Маржа: *{margin:,.0f} грн*",
        f"📦 На складі: *{parse_int(p.get('stock_qty',0))} шт*",
    ]
    return "\n".join(lines)

def fmt_calc(c: dict) -> str:
    return (
        f"*№{c.get('id')} — {c.get('name','')}*\n\n"
        f"💴 Ціна товару: *{c.get('price_yuan',0):,.2f} ¥*\n"
        f"⚖️ Вага: *{c.get('weight_kg',0):,.2f} кг*\n"
        f"🚚 Доставка: *{c.get('delivery_cost_uah',0):,.0f} грн*\n"
        f"💰 Собівартість: *{c.get('cost_price_uah',0):,.0f} грн* "
        f"(*{c.get('cost_price_usd',0):,.2f} $*)"
    )

class Auth(StatesGroup):
    waiting_password = State()

class AddProduct(StatesGroup):
    photo         = State()
    name          = State()
    description   = State()
    sku           = State()
    category      = State()
    category_manual = State()
    size          = State()
    color         = State()
    cost_price    = State()
    sale_price    = State()
    stock_qty     = State()

class EditField(StatesGroup):
    typing = State()

class SearchName(StatesGroup):
    typing = State()

class SearchSku(StatesGroup):
    typing = State()

class SearchCategory(StatesGroup):
    typing = State()

class AddCategory(StatesGroup):
    typing = State()

class DelCategory(StatesGroup):
    choosing = State()

class CalcSettingsEdit(StatesGroup):
    typing = State()

class CalcProduct(StatesGroup):
    name       = State()
    price_yuan = State()
    weight     = State()

def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Новий товар"), KeyboardButton(text="📋 Всі товари")],
        [KeyboardButton(text="🔍 За назвою"), KeyboardButton(text="🔠 За артикулом")],
        [KeyboardButton(text="🏷 За категорією"), KeyboardButton(text="⚙️ Категорії")],
        [KeyboardButton(text="🧮 Калькулятор")],
    ], resize_keyboard=True)

def kb_cancel() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Скасувати")]], resize_keyboard=True)

def kb_skip_cancel() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="⏩ Пропустити"), KeyboardButton(text="❌ Скасувати")]
    ], resize_keyboard=True)

def kb_from_list(items: list, with_manual=False) -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton(text=item)] for item in items]
    extra = []
    if with_manual:
        extra.append(KeyboardButton(text="✏️ Ввести вручну"))
    extra.append(KeyboardButton(text="❌ Скасувати"))
    rows.append(extra)
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def kb_calc_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Порахувати товар")],
        [KeyboardButton(text="📄 Мої розрахунки")],
        [KeyboardButton(text="⚙️ Налаштування калькулятора")],
        [KeyboardButton(text="◀️ Головне меню")],
    ], resize_keyboard=True)

def ikb_product_actions(pid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Редагувати", callback_data=f"edit:{pid}")],
        [InlineKeyboardButton(text="📷 Змінити фото", callback_data=f"editphoto:{pid}")],
        [InlineKeyboardButton(text="✅ Продано", callback_data=f"sold:{pid}")],
        [InlineKeyboardButton(text="🗑 Видалити", callback_data=f"delete:{pid}")],
        [InlineKeyboardButton(text="◀️ До списку", callback_data="back_to_list")],
    ])

def ikb_edit_fields(pid: int) -> InlineKeyboardMarkup:
    fields = [
        ("name", "Назва"), ("description", "Опис"),
        ("sku", "Артикул"), ("category", "Категорія"),
        ("size", "Розмір"), ("color", "Колір"),
        ("cost_price", "Собівартість"), ("sale_price", "Ціна продажу"),
        ("stock_qty", "Кількість"),
    ]
    rows = []
    for i in range(0, len(fields), 2):
        row = [InlineKeyboardButton(text=label, callback_data=f"editfield:{pid}:{key}")
               for key, label in fields[i:i+2]]
        rows.append(row)
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"view:{pid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def ikb_products_list(products: list, page: int = 0, per_page: int = 8) -> InlineKeyboardMarkup:
    start = page * per_page
    chunk = products[start:start + per_page]
    rows = []
    for p in chunk:
        label = f"№{p['id']} {p.get('name','')[:20]} — {p.get('sku','')}"
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

def ikb_categories_settings() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Додати категорію", callback_data="cfg:add_cat")],
        [InlineKeyboardButton(text="🗑 Видалити категорію", callback_data="cfg:del_cat")],
    ])

def ikb_calc_settings() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💴 Змінити ціну доставки за 1 кг", callback_data="calcset:price_per_kg")],
        [InlineKeyboardButton(text="💱 Змінити курс юаня", callback_data="calcset:yuan_rate")],
        [InlineKeyboardButton(text="💵 Змінити курс долара", callback_data="calcset:usd_rate")],
    ])

def ikb_calc_actions(cid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Видалити", callback_data=f"delcalc:{cid}")],
    ])

CALC_SETTING_LABELS = {
    "price_per_kg": "ціну доставки за 1 кг (грн)",
    "yuan_rate": "курс юаня (грн за 1¥)",
    "usd_rate": "курс долара (грн за 1$)",
}

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp  = Dispatcher(storage=MemoryStorage())

user_list_cache: dict = {}

async def require_auth(msg: Message, state: FSMContext) -> bool:
    authorized = await is_authorized(msg.from_user.id)
    if authorized is None:
        await msg.answer(DB_ERROR_TEXT)
        return False
    if authorized:
        return True
    current_state = await state.get_state()
    if current_state != Auth.waiting_password:
        await state.set_state(Auth.waiting_password)
        await msg.answer("🔒 *Доступ закритий*\n\nВведіть пароль:", reply_markup=ReplyKeyboardRemove())
    return False

@dp.errors()
async def global_error_handler(event, exception=None):
    exc = exception if exception is not None else getattr(event, "exception", None)
    logger.exception("Unhandled error while processing update: %s", exc)
    return True

@dp.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    authorized = await is_authorized(msg.from_user.id)
    if authorized is None:
        await msg.answer(DB_ERROR_TEXT)
        return
    if authorized:
        await msg.answer("👋 *Товари*\n\nОбери дію:", reply_markup=kb_main())
    else:
        await state.set_state(Auth.waiting_password)
        await msg.answer("🔒 *Доступ закритий*\n\nВведіть пароль:", reply_markup=ReplyKeyboardRemove())

@dp.message(Auth.waiting_password)
async def check_password(msg: Message, state: FSMContext):
    if msg.text == BOT_PASSWORD:
        ok = await authorize(msg.from_user.id)
        if not ok:
            await msg.answer(DB_ERROR_TEXT)
            return
        await state.clear()
        await msg.answer("✅ *Пароль вірний!*\n\nОбери дію:", reply_markup=kb_main())
    else:
        await msg.answer("❌ Невірний пароль. Спробуй ще раз:")

@dp.message(F.text == "◀️ Головне меню")
async def back_to_main(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("👋 *Товари*\n\nОбери дію:", reply_markup=kb_main())

@dp.message(F.text == "➕ Новий товар")
async def new_product_start(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    await state.set_state(AddProduct.photo)
    await msg.answer("📷 Надішліть *фото товару*:", reply_markup=kb_cancel())

@dp.message(AddProduct.photo, F.text == "❌ Скасувати")
async def ap_photo_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("Скасовано.", reply_markup=kb_main())

@dp.message(AddProduct.photo, F.photo)
async def ap_photo(msg: Message, state: FSMContext):
    # Фото нікуди не завантажуємо - зберігаємо лише file_id, який Telegram
    # вже назавжди зберігає на своїх серверах. Саме цей ідентифікатор і
    # записується у MongoDB як "фото в БД" - без Cloudinary та без важких
    # base64-блобів у документах.
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
    await state.set_state(AddProduct.description)
    await msg.answer("📝 Введіть *опис товару*:", reply_markup=kb_skip_cancel())

@dp.message(AddProduct.description)
async def ap_description(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    await state.update_data(description="" if msg.text == "⏩ Пропустити" else msg.text.strip())
    await state.set_state(AddProduct.sku)
    await msg.answer("🔖 Введіть *артикул (SKU)*:", reply_markup=kb_cancel())

@dp.message(AddProduct.sku)
async def ap_sku(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    await state.update_data(sku=msg.text.strip())
    categories = await get_categories()
    await state.set_state(AddProduct.category)
    if categories:
        await msg.answer("🏷 Оберіть *категорію*:", reply_markup=kb_from_list(categories, with_manual=True))
    else:
        await msg.answer("🏷 Введіть *категорію*:", reply_markup=kb_cancel())

@dp.message(AddProduct.category)
async def ap_category(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    if msg.text == "✏️ Ввести вручну":
        await state.set_state(AddProduct.category_manual)
        return await msg.answer("🏷 Введіть категорію:", reply_markup=kb_cancel())
    await _ap_category_save(msg, state, msg.text.strip())

@dp.message(AddProduct.category_manual)
async def ap_category_manual(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    await _ap_category_save(msg, state, msg.text.strip())

async def _ap_category_save(msg: Message, state: FSMContext, category_value: str):
    await state.update_data(category=category_value)
    await state.set_state(AddProduct.size)
    await msg.answer(f"✅ Категорія: *{category_value}*\n\n📏 Введіть *розмір*:", reply_markup=kb_skip_cancel())

@dp.message(AddProduct.size)
async def ap_size(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    await state.update_data(size="" if msg.text == "⏩ Пропустити" else msg.text.strip())
    await state.set_state(AddProduct.color)
    await msg.answer("🎨 Введіть *колір*:", reply_markup=kb_skip_cancel())

@dp.message(AddProduct.color)
async def ap_color(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    await state.update_data(color="" if msg.text == "⏩ Пропустити" else msg.text.strip())
    await state.set_state(AddProduct.cost_price)
    await msg.answer("💵 Введіть *собівартість* (грн):", reply_markup=kb_cancel())

@dp.message(AddProduct.cost_price)
async def ap_cost_price(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    try:
        float(msg.text.strip().replace(",", "."))
    except ValueError:
        return await msg.answer("⚠️ Введіть число, наприклад *150*:", reply_markup=kb_cancel())
    await state.update_data(cost_price=msg.text.strip())
    await state.set_state(AddProduct.sale_price)
    await msg.answer("💰 Введіть *ціну продажу* (грн):", reply_markup=kb_cancel())

@dp.message(AddProduct.sale_price)
async def ap_sale_price(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    try:
        float(msg.text.strip().replace(",", "."))
    except ValueError:
        return await msg.answer("⚠️ Введіть число, наприклад *300*:", reply_markup=kb_cancel())
    await state.update_data(sale_price=msg.text.strip())
    await state.set_state(AddProduct.stock_qty)
    await msg.answer("📦 Введіть *кількість на складі*:", reply_markup=kb_cancel())

@dp.message(AddProduct.stock_qty)
async def ap_stock_qty(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    try:
        int(msg.text.strip())
    except ValueError:
        return await msg.answer("⚠️ Введіть ціле число, наприклад *10*:", reply_markup=kb_cancel())

    fd = await state.get_data()
    await state.clear()
    product = {
        "id":         await next_product_id(),
        "photo_id":   fd.get("photo_id", ""),
        "name":       fd.get("name", ""),
        "description": fd.get("description", ""),
        "sku":        fd.get("sku", ""),
        "category":   fd.get("category", ""),
        "size":       fd.get("size", ""),
        "color":      fd.get("color", ""),
        "cost_price": fd.get("cost_price", ""),
        "sale_price": fd.get("sale_price", ""),
        "stock_qty":  msg.text.strip(),
        "created_at": datetime.now().isoformat(),
        "created_by": msg.from_user.id,
    }
    categories = await get_categories()
    if product["category"] and product["category"] not in categories:
        categories.append(product["category"])
        await save_categories(categories)

    await add_product(product)
    await msg.answer_photo(
        photo=product["photo_id"],
        caption=f"✅ *Товар додано!*\n\n{fmt_product(product)}",
        reply_markup=kb_main(),
    )

@dp.message(F.text == "📋 Всі товари")
async def list_products(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
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

@dp.callback_query(F.data.startswith("delete:"))
async def delete_product_cb(cb: CallbackQuery):
    try:
        pid = int(cb.data.split(":")[1])
        product = await get_product(pid)
        if not product:
            return await cb.answer("Не знайдено!", show_alert=True)
        await delete_product(pid)
        await cb.message.edit_caption(caption=f"🗑 Товар №{pid} видалено.")
        await cb.answer("Видалено!")
    except Exception:
        logger.exception("delete_product_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data.startswith("edit:"))
async def edit_product_cb(cb: CallbackQuery):
    try:
        pid = int(cb.data.split(":")[1])
        await cb.message.answer(f"✏️ *Редагування №{pid}*\nОберіть поле:", reply_markup=ikb_edit_fields(pid))
        await cb.answer()
    except Exception:
        logger.exception("edit_product_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.callback_query(F.data.startswith("editfield:"))
async def edit_field_choose(cb: CallbackQuery, state: FSMContext):
    try:
        _, pid_s, field = cb.data.split(":", 2)
        pid = int(pid_s)
        labels = {
            "name": "Назву", "description": "Опис", "sku": "Артикул",
            "category": "Категорію", "size": "Розмір", "color": "Колір",
            "cost_price": "Собівартість (грн)", "sale_price": "Ціну продажу (грн)",
            "stock_qty": "Кількість на складі",
        }
        await state.set_state(EditField.typing)
        await state.update_data(edit_pid=pid, edit_field=field)
        await cb.message.answer(f"Введіть нове значення для *{labels.get(field, field)}*:", reply_markup=kb_cancel())
        await cb.answer()
    except Exception:
        logger.exception("edit_field_choose failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.message(EditField.typing)
async def edit_field_save(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    fd = await state.get_data()
    pid, field = fd["edit_pid"], fd["edit_field"]
    value = msg.text.strip()
    if field in ("cost_price", "sale_price"):
        try:
            float(value.replace(",", "."))
        except ValueError:
            return await msg.answer("⚠️ Введіть число:", reply_markup=kb_cancel())
    if field == "stock_qty":
        try:
            int(value)
        except ValueError:
            return await msg.answer("⚠️ Введіть ціле число:", reply_markup=kb_cancel())
    await state.clear()
    await update_product(pid, {field: value})
    if field == "category":
        categories = await get_categories()
        if value not in categories:
            categories.append(value)
            await save_categories(categories)
    product = await get_product(pid)
    if product:
        await msg.answer_photo(
            photo=product.get("photo_id") or None,
            caption=f"✅ Оновлено!\n\n{fmt_product(product)}",
            reply_markup=kb_main(),
        )
    else:
        await msg.answer("Товар не знайдено.", reply_markup=kb_main())

@dp.callback_query(F.data.startswith("editphoto:"))
async def edit_photo_cb(cb: CallbackQuery, state: FSMContext):
    try:
        pid = int(cb.data.split(":")[1])
        await state.set_state(EditField.typing)
        await state.update_data(edit_pid=pid, edit_field="__photo__")
        await cb.message.answer(f"📷 Надішліть нове фото для товару №{pid}:", reply_markup=kb_cancel())
        await cb.answer()
    except Exception:
        logger.exception("edit_photo_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.message(EditField.typing, F.photo)
async def edit_photo_save(msg: Message, state: FSMContext):
    fd = await state.get_data()
    if fd.get("edit_field") != "__photo__":
        return
    pid = fd["edit_pid"]
    await state.clear()
    file_id = msg.photo[-1].file_id
    await update_product(pid, {"photo_id": file_id})
    product = await get_product(pid)
    if product:
        await msg.answer_photo(photo=file_id, caption=f"✅ Фото оновлено!\n\n{fmt_product(product)}", reply_markup=kb_main())
    else:
        await msg.answer("Товар не знайдено.", reply_markup=kb_main())

@dp.message(F.text == "🔍 За назвою")
async def search_name_start(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
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
    await _show_search_results(msg, found, q)

@dp.message(F.text == "🔠 За артикулом")
async def search_sku_start(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    await state.set_state(SearchSku.typing)
    await msg.answer("🔠 Введіть *артикул* (або його частину):", reply_markup=kb_cancel())

@dp.message(SearchSku.typing)
async def search_sku_do(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    await state.clear()
    q = msg.text.strip().lower()
    all_products = await get_all_products()
    found = [p for p in all_products if q in str(p.get("sku", "")).lower()]
    await _show_search_results(msg, found, q)

@dp.message(F.text == "🏷 За категорією")
async def search_category_start(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    categories = await get_categories()
    await state.set_state(SearchCategory.typing)
    if categories:
        await msg.answer("🏷 Оберіть категорію:", reply_markup=kb_from_list(categories))
    else:
        await msg.answer("🏷 Введіть назву категорії:", reply_markup=kb_cancel())

@dp.message(SearchCategory.typing)
async def search_category_do(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_main())
    await state.clear()
    q = msg.text.strip().lower()
    all_products = await get_all_products()
    found = [p for p in all_products if q in str(p.get("category", "")).lower()]
    await _show_search_results(msg, found, q)

async def _show_search_results(msg: Message, found: list, q: str):
    if not found:
        return await msg.answer(f"🔍 За запитом *{q}* нічого не знайдено.", reply_markup=kb_main())
    uid = msg.from_user.id
    user_list_cache[uid] = found
    await msg.answer(f"🔍 Знайдено: *{len(found)}*", reply_markup=kb_main())
    await msg.answer("Результати:", reply_markup=ikb_products_list(found))

@dp.message(F.text == "⚙️ Категорії")
async def categories_menu(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    categories = await get_categories()
    cats_txt = "\n".join(f"  • {c}" for c in categories) or "  (порожньо)"
    await msg.answer(f"⚙️ *Категорії*\n\n{cats_txt}", reply_markup=ikb_categories_settings())

@dp.callback_query(F.data == "cfg:add_cat")
async def cfg_add_cat(cb: CallbackQuery, state: FSMContext):
    try:
        await state.set_state(AddCategory.typing)
        await cb.message.answer("🏷 Введіть назву *нової категорії*:", reply_markup=kb_cancel())
        await cb.answer()
    except Exception:
        logger.exception("cfg_add_cat failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.message(AddCategory.typing)
async def cfg_add_cat_save(msg: Message, state: FSMContext):
    await state.clear()
    if msg.text == "❌ Скасувати": return await msg.answer("Скасовано.", reply_markup=kb_main())
    val = msg.text.strip()
    categories = await get_categories()
    if val in categories:
        await msg.answer(f"⚠️ Категорія *{val}* вже є.", reply_markup=kb_main())
    else:
        categories.append(val)
        await save_categories(categories)
        await msg.answer(f"✅ Категорія *{val}* додана!\n\n" + "\n".join(f"  • {c}" for c in categories),
                         reply_markup=kb_main())

@dp.callback_query(F.data == "cfg:del_cat")
async def cfg_del_cat(cb: CallbackQuery, state: FSMContext):
    try:
        categories = await get_categories()
        if not categories: return await cb.answer("Список порожній!", show_alert=True)
        await state.set_state(DelCategory.choosing)
        await cb.message.answer("🗑 Оберіть категорію для видалення:", reply_markup=kb_from_list(categories))
        await cb.answer()
    except Exception:
        logger.exception("cfg_del_cat failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
        except TelegramAPIError:
            pass

@dp.message(DelCategory.choosing)
async def cfg_del_cat_do(msg: Message, state: FSMContext):
    await state.clear()
    if msg.text == "❌ Скасувати": return await msg.answer("Скасовано.", reply_markup=kb_main())
    val = msg.text.strip()
    categories = await get_categories()
    if val in categories:
        categories.remove(val)
        await save_categories(categories)
        cats_txt = "\n".join(f"  • {c}" for c in categories) or "  (порожньо)"
        await msg.answer(f"🗑 Категорія *{val}* видалена.\n\n{cats_txt}", reply_markup=kb_main())
    else:
        await msg.answer("Не знайдено.", reply_markup=kb_main())

# ---------------------------------------------------------------------------
# Калькулятор
# ---------------------------------------------------------------------------
@dp.message(F.text == "🧮 Калькулятор")
async def calc_menu(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    await state.clear()
    await msg.answer("🧮 *Калькулятор*\n\nОбери дію:", reply_markup=kb_calc_menu())

@dp.message(F.text == "⚙️ Налаштування калькулятора")
async def calc_settings_menu(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    s = await get_calc_settings()
    text = (
        "⚙️ *Налаштування калькулятора*\n\n"
        f"💴 Ціна доставки за 1 кг: *{s['price_per_kg']:.2f} грн*\n"
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

@dp.message(CalcSettingsEdit.typing)
async def calcset_save(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu())
    fd = await state.get_data()
    field = fd["calcset_field"]
    value = parse_float(msg.text)
    if value <= 0:
        return await msg.answer("⚠️ Введіть додатнє число, наприклад *12.5*:", reply_markup=kb_cancel())
    await state.clear()
    await save_calc_setting(field, value)
    s = await get_calc_settings()
    text = (
        "✅ Збережено!\n\n"
        f"💴 Ціна доставки за 1 кг: *{s['price_per_kg']:.2f} грн*\n"
        f"💱 Курс юаня: *{s['yuan_rate']:.2f} грн*\n"
        f"💵 Курс долара: *{s['usd_rate']:.2f} грн*"
    )
    await msg.answer(text, reply_markup=kb_calc_menu())

@dp.message(F.text == "➕ Порахувати товар")
async def calc_start(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    s = await get_calc_settings()
    if not all(s.values()):
        return await msg.answer(
            "⚠️ Спочатку задайте ціну доставки, курс юаня і курс долара "
            "в *⚙️ Налаштування калькулятора*.",
            reply_markup=kb_calc_menu(),
        )
    await state.set_state(CalcProduct.name)
    await msg.answer("📝 Введіть *назву товару*:", reply_markup=kb_cancel())

@dp.message(CalcProduct.name)
async def calc_name(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu())
    await state.update_data(calc_name=msg.text.strip())
    await state.set_state(CalcProduct.price_yuan)
    await msg.answer("💴 Введіть *ціну товару в юанях*:", reply_markup=kb_cancel())

@dp.message(CalcProduct.price_yuan)
async def calc_price(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu())
    value = parse_float(msg.text)
    if value <= 0:
        return await msg.answer("⚠️ Введіть додатнє число, наприклад *25.5*:", reply_markup=kb_cancel())
    await state.update_data(calc_price_yuan=value)
    await state.set_state(CalcProduct.weight)
    await msg.answer("⚖️ Введіть *вагу товару* (кг):", reply_markup=kb_cancel())

@dp.message(CalcProduct.weight)
async def calc_weight(msg: Message, state: FSMContext):
    if msg.text == "❌ Скасувати":
        await state.clear(); return await msg.answer("Скасовано.", reply_markup=kb_calc_menu())
    weight = parse_float(msg.text)
    if weight <= 0:
        return await msg.answer("⚠️ Введіть додатнє число, наприклад *0.5*:", reply_markup=kb_cancel())

    fd = await state.get_data()
    name = fd["calc_name"]
    price_yuan = fd["calc_price_yuan"]
    await state.clear()

    s = await get_calc_settings()
    delivery_cost_uah = weight * s["price_per_kg"]
    price_uah = price_yuan * s["yuan_rate"]
    cost_price_uah = price_uah + delivery_cost_uah
    cost_price_usd = cost_price_uah / s["usd_rate"] if s["usd_rate"] else 0.0

    record = {
        "id":                await next_calc_id(),
        "name":              name,
        "price_yuan":        price_yuan,
        "weight_kg":         weight,
        "delivery_cost_uah": delivery_cost_uah,
        "cost_price_uah":    cost_price_uah,
        "cost_price_usd":    cost_price_usd,
        "created_at":        datetime.now().isoformat(),
        "created_by":        msg.from_user.id,
    }
    await add_calc(record)

    await msg.answer(fmt_calc(record), reply_markup=ikb_calc_actions(record["id"]))
    await msg.answer("Готово ✅ Розрахунок збережено.", reply_markup=kb_calc_menu())

@dp.message(F.text == "📄 Мої розрахунки")
async def list_calcs(msg: Message, state: FSMContext):
    if not await require_auth(msg, state): return
    calcs = await get_all_calcs()
    if not calcs:
        return await msg.answer("📭 Розрахунків ще немає.", reply_markup=kb_calc_menu())
    await msg.answer(f"📄 *Розрахунки* — {len(calcs)} шт.", reply_markup=kb_calc_menu())
    for c in calcs:
        await msg.answer(fmt_calc(c), reply_markup=ikb_calc_actions(c["id"]))

@dp.callback_query(F.data.startswith("delcalc:"))
async def delete_calc_cb(cb: CallbackQuery):
    try:
        cid = int(cb.data.split(":")[1])
        await delete_calc(cid)
        await cb.message.edit_text(f"🗑 Розрахунок №{cid} видалено.")
        await cb.answer("Видалено!")
    except Exception:
        logger.exception("delete_calc_cb failed")
        try:
            await cb.answer(DB_ERROR_TEXT, show_alert=True)
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

    await load_authorized_uids()

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