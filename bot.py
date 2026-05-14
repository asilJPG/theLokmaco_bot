"""
iiko Daily Report Telegram Bot
Отправляет отчёт по кассе в Telegram после закрытия смены.
Только чтение данных — никаких изменений в iiko.
"""

import os
import json
import hashlib
import logging
import traceback
from datetime import datetime, timedelta

import aiohttp
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

load_dotenv()

# ─── Конфигурация ───────────────────────────────────────────────
IIKO_SERVER = os.getenv("IIKO_SERVER", "").rstrip("/")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

# ─── Логгер ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("iiko-bot")
logging.getLogger("aiogram").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.INFO)

# ─── Бот и роутер ───────────────────────────────────────────────
bot = Bot(token=TG_BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)


# ═══════════════════════════════════════════════════════════════════
#  iiko Server API — только чтение
# ═══════════════════════════════════════════════════════════════════

def sha1_hash(password: str) -> str:
    return hashlib.sha1(password.encode("utf-8")).hexdigest()


async def iiko_auth(session: aiohttp.ClientSession) -> str | None:
    url = f"{IIKO_SERVER}/resto/api/auth"
    params = {"login": IIKO_LOGIN, "pass": sha1_hash(IIKO_PASSWORD)}
    try:
        async with session.get(url, params=params, ssl=False) as resp:
            if resp.status == 200:
                token = (await resp.text()).strip()
                log.info("🔑 Авторизация ОК")
                return token
            else:
                log.error(f"🔑 Авторизация ❌ status={resp.status}")
                return None
    except Exception as e:
        log.error(f"🔑 Ошибка авторизации: {e}")
        return None


async def iiko_logout(session: aiohttp.ClientSession, token: str):
    try:
        url = f"{IIKO_SERVER}/resto/api/logout"
        async with session.get(url, params={"key": token}, ssl=False) as resp:
            log.info(f"🚪 Logout: {resp.status}")
    except Exception as e:
        log.warning(f"🚪 Logout ошибка: {e}")


async def fetch_olap(
    session: aiohttp.ClientSession,
    token: str,
    date_from: str,
    date_to: str,
    group_fields: list[str],
    aggregate_fields: list[str],
) -> dict | None:
    """Универсальный OLAP-запрос."""
    url = f"{IIKO_SERVER}/resto/api/v2/reports/olap"
    headers = {"Cookie": f"key={token}", "Content-Type": "application/json"}
    body = {
        "reportType": "SALES",
        "buildSummary": "true",
        "groupByRowFields": group_fields,
        "groupByColFields": [],
        "aggregateFields": aggregate_fields,
        "filters": {
            "OpenDate.Typed": {
                "filterType": "DateRange",
                "periodType": "CUSTOM",
                "from": date_from,
                "to": date_to,
                "includeLow": "true",
                "includeHigh": "true",
            },
            "DeletedWithWriteoff": {
                "filterType": "ExcludeValues",
                "values": ["DELETED_WITHOUT_WRITEOFF"],
            },
        },
    }
    try:
        async with session.post(url, json=body, headers=headers, ssl=False) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                raw = await resp.text()
                log.error(f"OLAP ❌ status={resp.status}, body={raw[:300]}")
                return None
    except Exception as e:
        log.error(f"OLAP ❌ ошибка: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
#  Форматирование
# ═══════════════════════════════════════════════════════════════════

def fmt(n: float) -> str:
    return f"{round(n):,}".replace(",", " ")


def build_cash_message(olap_pay, summary, report_date: str) -> str:
    lines = [f"📊 <b>Касса за {report_date}</b>", ""]

    total_revenue = 0.0
    total_orders = 0
    total_guests = 0

    if summary and summary.get("data"):
        for row in summary["data"]:
            total_revenue += float(row.get("DishDiscountSumInt", 0))
            total_orders += int(float(row.get("UniqOrderId.OrdersCount", 0)))
            total_guests += int(float(row.get("GuestNum", 0)))
    elif summary and summary.get("summary"):
        s = summary["summary"]
        total_revenue = float(s.get("DishDiscountSumInt", 0))
        total_orders = int(float(s.get("UniqOrderId.OrdersCount", 0)))
        total_guests = int(float(s.get("GuestNum", 0)))

    avg_check = total_revenue / total_orders if total_orders > 0 else 0

    lines.append(f"💰 Выручка: <b>{fmt(total_revenue)} сум</b>")
    lines.append(f"🧾 Чеков: <b>{total_orders}</b>")
    lines.append(f"📈 Средний чек: <b>{fmt(avg_check)} сум</b>")
    if total_guests > 0:
        lines.append(f"👥 Гостей: <b>{total_guests}</b>")

    if olap_pay and olap_pay.get("data"):
        lines.append("")
        lines.append("<b>По типам оплат:</b>")
        payments = []
        for row in olap_pay["data"]:
            pt = row.get("PayTypes", "Не указан")
            rev = float(row.get("DishDiscountSumInt", 0))
            if rev > 0:
                payments.append((pt, rev))
        payments.sort(key=lambda x: x[1], reverse=True)
        for pt, rev in payments:
            pct = (rev / total_revenue * 100) if total_revenue > 0 else 0
            icon = "💵" if "нал" in pt.lower() else "💳"
            lines.append(f"  {icon} {pt}: {fmt(rev)} сум ({pct:.0f}%)")

    return "\n".join(lines)


def build_top_message(top_data, report_date: str) -> str:
    lines = [f"🍽 <b>Топ продаж за {report_date}</b>", ""]

    if not top_data or not top_data.get("data"):
        lines.append("<i>Нет данных</i>")
        return "\n".join(lines)

    categories = {}
    for row in top_data["data"]:
        cat = row.get("DishCategory", "Без категории") or "Без категории"
        name = row.get("DishName", "—")
        amount = float(row.get("DishAmountInt", 0))
        revenue = float(row.get("DishDiscountSumInt", 0))
        if amount <= 0:
            continue
        if cat not in categories:
            categories[cat] = []
        categories[cat].append((name, amount, revenue))

    for cat in categories:
        categories[cat].sort(key=lambda x: x[2], reverse=True)

    sorted_cats = sorted(
        categories.items(),
        key=lambda x: sum(r[2] for r in x[1]),
        reverse=True,
    )

    for cat, dishes in sorted_cats:
        cat_total = sum(d[2] for d in dishes)
        cat_qty = sum(d[1] for d in dishes)
        lines.append(f"📂 <b>{cat}</b>  ({fmt(cat_total)} сум, {int(cat_qty)} шт)")
        for i, (name, amount, revenue) in enumerate(dishes[:3]):
            medal = ["🥇", "🥈", "🥉"][i]
            lines.append(f"  {medal} {name} — {int(amount)} шт, {fmt(revenue)} сум")
        lines.append("")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════
#  Отправка отчётов
# ═══════════════════════════════════════════════════════════════════

async def send_daily_report(target_date: str | None = None):
    if target_date is None:
        now = datetime.utcnow() + timedelta(hours=5)
        target_date = now.strftime("%Y-%m-%d")

    display = datetime.strptime(target_date, "%Y-%m-%d").strftime("%d.%m.%Y")
    log.info(f"📊 Касса за {target_date}")

    async with aiohttp.ClientSession() as session:
        token = await iiko_auth(session)
        if not token:
            try: await bot.send_message(TG_CHAT_ID, "❌ Не удалось подключиться к iiko.")
            except: pass
            return
        try:
            olap_pay = await fetch_olap(session, token, target_date, target_date,
                ["PayTypes"], ["DishDiscountSumInt", "OrderNum", "UniqOrderId.OrdersCount"])
            summary = await fetch_olap(session, token, target_date, target_date,
                [], ["DishDiscountSumInt", "OrderNum", "UniqOrderId.OrdersCount", "GuestNum"])
            msg = build_cash_message(olap_pay, summary, display)
            await bot.send_message(TG_CHAT_ID, msg, parse_mode="HTML")
            log.info("📊 ✅ Отправлено")
        except Exception as e:
            log.error(f"📊 ❌ {e}")
            try: await bot.send_message(TG_CHAT_ID, f"❌ Ошибка: {e}")
            except: pass
        finally:
            await iiko_logout(session, token)


async def send_top_report(target_date: str | None = None):
    if target_date is None:
        now = datetime.utcnow() + timedelta(hours=5)
        target_date = now.strftime("%Y-%m-%d")

    display = datetime.strptime(target_date, "%Y-%m-%d").strftime("%d.%m.%Y")
    log.info(f"🍽 Топ за {target_date}")

    async with aiohttp.ClientSession() as session:
        token = await iiko_auth(session)
        if not token:
            try: await bot.send_message(TG_CHAT_ID, "❌ Не удалось подключиться к iiko.")
            except: pass
            return
        try:
            top_data = await fetch_olap(session, token, target_date, target_date,
                ["DishCategory", "DishName"], ["DishAmountInt", "DishDiscountSumInt"])
            msg = build_top_message(top_data, display)
            if len(msg) > 4000:
                parts = msg.split("\n📂 ")
                current = parts[0]
                for part in parts[1:]:
                    chunk = "\n📂 " + part
                    if len(current) + len(chunk) > 4000:
                        await bot.send_message(TG_CHAT_ID, current, parse_mode="HTML")
                        current = chunk
                    else:
                        current += chunk
                if current:
                    await bot.send_message(TG_CHAT_ID, current, parse_mode="HTML")
            else:
                await bot.send_message(TG_CHAT_ID, msg, parse_mode="HTML")
            log.info("🍽 ✅ Отправлено")
        except Exception as e:
            log.error(f"🍽 ❌ {e}")
            try: await bot.send_message(TG_CHAT_ID, f"❌ Ошибка: {e}")
            except: pass
        finally:
            await iiko_logout(session, token)


async def send_nightly_report():
    """Ночной отчёт в 2:00 — касса + топ-3 за вчера."""
    now = datetime.utcnow() + timedelta(hours=5)
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    display = datetime.strptime(yesterday, "%Y-%m-%d").strftime("%d.%m.%Y")
    log.info(f"🌙 Ночной отчёт за {yesterday}")

    async with aiohttp.ClientSession() as session:
        token = await iiko_auth(session)
        if not token:
            try: await bot.send_message(TG_CHAT_ID, "❌ Не удалось подключиться к iiko.")
            except: pass
            return
        try:
            # Касса
            olap_pay = await fetch_olap(session, token, yesterday, yesterday,
                ["PayTypes"], ["DishDiscountSumInt", "OrderNum", "UniqOrderId.OrdersCount"])
            summary = await fetch_olap(session, token, yesterday, yesterday,
                [], ["DishDiscountSumInt", "OrderNum", "UniqOrderId.OrdersCount", "GuestNum"])
            cash_msg = build_cash_message(olap_pay, summary, display)

            # Топ
            top_data = await fetch_olap(session, token, yesterday, yesterday,
                ["DishCategory", "DishName"], ["DishAmountInt", "DishDiscountSumInt"])
            top_msg = build_top_message(top_data, display)

            combined = cash_msg + "\n\n━━━━━━━━━━━━━━━━━━━━\n\n" + top_msg
            if len(combined) > 4000:
                await bot.send_message(TG_CHAT_ID, cash_msg, parse_mode="HTML")
                await bot.send_message(TG_CHAT_ID, top_msg, parse_mode="HTML")
            else:
                await bot.send_message(TG_CHAT_ID, combined, parse_mode="HTML")

            log.info("🌙 ✅ Ночной отчёт отправлен")
        except Exception as e:
            log.error(f"🌙 ❌ {e}")
            try: await bot.send_message(TG_CHAT_ID, f"❌ Ошибка: {e}")
            except: pass
        finally:
            await iiko_logout(session, token)


# ═══════════════════════════════════════════════════════════════════
#  Команды бота
# ═══════════════════════════════════════════════════════════════════

@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "👋 Бот отчётов iiko\n\n"
        "Команды:\n"
        "/report — касса за сегодня\n"
        "/report_yesterday — касса за вчера\n"
        "/top — топ продаж за сегодня\n"
        "/products — список товаров (файл)\n"
        "/suppliers — список поставщиков (файл)\n"
        "/stores — список складов (файл)\n"
        "/chat_id — узнать ID этого чата\n\n"
        "Или отправь дату:\n"
        "13.05.2026 — касса\n"
        "топ 13.05.2026 — топ продаж"
    )

@router.message(Command("report"))
async def cmd_report(message: Message):
    await message.answer("⏳ Формирую кассу за сегодня...")
    await send_daily_report()

@router.message(Command("report_yesterday"))
async def cmd_report_yesterday(message: Message):
    await message.answer("⏳ Формирую кассу за вчера...")
    now = datetime.utcnow() + timedelta(hours=5)
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    await send_daily_report(target_date=yesterday)

@router.message(Command("top"))
async def cmd_top(message: Message):
    await message.answer("⏳ Формирую топ продаж за сегодня...")
    await send_top_report()

@router.message(Command("chat_id"))
async def cmd_chat_id(message: Message):
    await message.answer(f"Chat ID: <code>{message.chat.id}</code>", parse_mode="HTML")


# ═══════════════════════════════════════════════════════════════════
#  Справочники iiko → файлом в личку
# ═══════════════════════════════════════════════════════════════════

async def fetch_iiko_list(endpoint: str) -> list | None:
    """GET запрос к справочнику iiko, вернуть JSON."""
    async with aiohttp.ClientSession() as session:
        token = await iiko_auth(session)
        if not token:
            return None
        try:
            url = f"{IIKO_SERVER}/resto/api/{endpoint}"
            headers = {"Cookie": f"key={token}"}
            async with session.get(url, headers=headers, ssl=False) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    raw = await resp.text()
                    log.error(f"GET {endpoint} ❌ {resp.status}: {raw[:300]}")
                    return None
        except Exception as e:
            log.error(f"GET {endpoint} ❌ {e}")
            return None
        finally:
            await iiko_logout(session, token)


async def fetch_iiko_raw(endpoint: str) -> tuple[str | None, str | None]:
    """GET запрос, вернуть сырой текст и content-type."""
    async with aiohttp.ClientSession() as session:
        token = await iiko_auth(session)
        if not token:
            return None, None
        try:
            url = f"{IIKO_SERVER}/resto/api/{endpoint}"
            headers = {"Cookie": f"key={token}"}
            async with session.get(url, headers=headers, ssl=False) as resp:
                raw = await resp.text()
                ct = resp.content_type or ""
                log.info(f"GET {endpoint} → {resp.status}, type={ct}, size={len(raw)}")
                if resp.status == 200:
                    return raw, ct
                else:
                    log.error(f"GET {endpoint} ❌ {resp.status}: {raw[:300]}")
                    return None, None
        except Exception as e:
            log.error(f"GET {endpoint} ❌ {e}")
            return None, None
        finally:
            await iiko_logout(session, token)


from aiogram.types import BufferedInputFile

@router.message(Command("products"))
async def cmd_products(message: Message):
    await message.answer("⏳ Загружаю список товаров из iiko...")
    data = await fetch_iiko_list("v2/entities/products/list")
    if not data:
        await message.answer("❌ Не удалось получить товары")
        return

    lines = ["ID | Название | Тип | Ед.изм"]
    lines.append("-" * 80)
    for item in data:
        pid = item.get("id", "—")
        name = item.get("name", "—")
        ptype = item.get("type", "—")
        unit = item.get("mainUnit", "—")
        lines.append(f"{pid} | {name} | {ptype} | {unit}")

    text = "\n".join(lines)
    file = BufferedInputFile(text.encode("utf-8"), filename="products.txt")
    await message.answer_document(file, caption=f"📦 Товаров: {len(data)}")


@router.message(Command("suppliers"))
async def cmd_suppliers(message: Message):
    await message.answer("⏳ Загружаю список поставщиков...")

    raw, ct = await fetch_iiko_raw("suppliers")
    if not raw:
        await message.answer("❌ Не удалось получить поставщиков.")
        return

    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(raw)
        lines = ["ID | Название"]
        lines.append("-" * 80)
        count = 0
        for emp in root.findall("employee"):
            is_supplier = emp.findtext("supplier", "false")
            is_deleted = emp.findtext("deleted", "false")
            if is_supplier == "true" and is_deleted == "false":
                sid = emp.findtext("id", "—")
                name = emp.findtext("name", "—")
                lines.append(f"{sid} | {name}")
                count += 1

        text = "\n".join(lines)
        file = BufferedInputFile(text.encode("utf-8"), filename="suppliers.txt")
        await message.answer_document(file, caption=f"🏭 Поставщиков: {count}")
    except Exception as e:
        log.error(f"XML parse error: {e}")
        file = BufferedInputFile(raw.encode("utf-8"), filename="suppliers_raw.txt")
        await message.answer_document(file, caption=f"❌ Ошибка парсинга, сырой ответ")


@router.message(Command("stores"))
async def cmd_stores(message: Message):
    await message.answer("⏳ Загружаю список складов...")

    raw, ct = await fetch_iiko_raw("corporation/stores")
    if not raw:
        await message.answer("❌ Не удалось получить склады.")
        return

    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(raw)
        lines = ["ID | Название"]
        lines.append("-" * 80)
        count = 0
        # Пробуем разные варианты структуры
        stores = root.findall("corporateItemDto") or root.findall("store") or root.findall(".//")
        for store in stores:
            sid = store.findtext("id", None)
            name = store.findtext("name", None)
            if sid and name:
                lines.append(f"{sid} | {name}")
                count += 1

        if count == 0:
            # Если не распарсили — шлём сырой
            file = BufferedInputFile(raw.encode("utf-8"), filename="stores_raw.txt")
            await message.answer_document(file, caption="🏪 Не удалось распарсить, сырой ответ")
        else:
            text = "\n".join(lines)
            file = BufferedInputFile(text.encode("utf-8"), filename="stores.txt")
            await message.answer_document(file, caption=f"🏪 Складов: {count}")
    except Exception as e:
        log.error(f"XML parse error: {e}")
        file = BufferedInputFile(raw.encode("utf-8"), filename="stores_raw.txt")
        await message.answer_document(file, caption=f"❌ Ошибка парсинга, сырой ответ")

@router.message()
async def handle_date(message: Message):
    text = message.text.strip() if message.text else ""

    is_top = False
    date_text = text
    if text.lower().startswith(("топ ", "top ")):
        is_top = True
        date_text = text.split(" ", 1)[1].strip()

    target_date = None
    for f in ("%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            target_date = datetime.strptime(date_text, f).strftime("%Y-%m-%d")
            break
        except ValueError:
            continue

    if not target_date:
        return

    if is_top:
        await message.answer(f"⏳ Топ продаж за {date_text}...")
        await send_top_report(target_date=target_date)
    else:
        await message.answer(f"⏳ Касса за {date_text}...")
        await send_daily_report(target_date=target_date)


# ═══════════════════════════════════════════════════════════════════
#  Запуск
# ═══════════════════════════════════════════════════════════════════

async def main():
    log.info("🤖 iiko Report Bot запущен")

    scheduler = AsyncIOScheduler(timezone="Asia/Tashkent")
    scheduler.add_job(
        send_nightly_report,
        CronTrigger(hour=2, minute=0),
        id="nightly_report",
        name="Nightly report (cash + top)",
    )
    scheduler.start()
    log.info("⏰ Ночной отчёт: каждый день в 02:00 (Ташкент)")

    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())