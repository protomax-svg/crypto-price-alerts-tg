import os
import json
import time
import uuid
import asyncio
import logging
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple, Deque
from collections import deque
from telegram import Update
from telegram.ext import ContextTypes

from dotenv import load_dotenv
import aiohttp
import websockets

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# =========================
# Config (same as your file)
# =========================
load_dotenv()  # reads .env from current folder

DATA_DIR = os.path.join(os.path.dirname(__file__), "data_users")
BINANCE_REST_PRICE = "https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
BINANCE_WS_BASE = "wss://stream.binance.com:9443/ws"
WS_STREAM_SUFFIX = "@trade"


TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
if not TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN in env/.env")

os.makedirs(DATA_DIR, exist_ok=True)

# Conversation states (same idea as your file)
ADD_SYMBOL, ADD_PRICE = range(2)
REMOVE_ID = range(2, 3)

# =========================
# Logs (/logs 258079 N)
# =========================
LOGS_PASSWORD = "258079"
LOG_BUFFER_MAX = 5000
LOG_BUFFER: Deque[str] = deque(maxlen=LOG_BUFFER_MAX)
LAST_LOGS_MESSAGE_ID_BY_CHAT: Dict[int, int] = {}

logger = logging.getLogger("price_alerts")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

_console = logging.StreamHandler()
_console.setFormatter(_fmt)
logger.addHandler(_console)


class RingBufferHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            LOG_BUFFER.append(self.format(record))
        except Exception:
            pass


_ring = RingBufferHandler()
_ring.setFormatter(_fmt)
logger.addHandler(_ring)

# =========================
# Data model (same as your file)
# =========================


@dataclass
class Alert:
    id: str
    symbol: str
    target: float
    direction: str  # "up" or "down"
    created_at: int
    market_at_create: float


def user_file_path(user_id: int) -> str:
    return os.path.join(DATA_DIR, f"{user_id}.json")


def load_user_alerts(user_id: int) -> List[Alert]:
    path = user_file_path(user_id)
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        alerts: List[Alert] = []
        for a in raw.get("alerts", []):
            alerts.append(Alert(**a))
        return alerts
    except Exception as e:
        logger.exception(f"load_user_alerts failed user={user_id}: {e}")
        return []


def save_user_alerts(user_id: int, alerts: List[Alert]) -> None:
    path = user_file_path(user_id)
    payload = {"user_id": user_id, "alerts": [asdict(a) for a in alerts]}
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# =========================
# Global runtime state (same)
# =========================
ALERTS_BY_USER: Dict[int, List[Alert]] = {}
WS_TASK_BY_SYMBOL: Dict[str, asyncio.Task] = {}
LAST_PRICE: Dict[str, float] = {}
STATE_LOCK = asyncio.Lock()

# =========================
# Binance helpers (same + ticker-not-found handling)
# =========================


def normalize_symbol(s: str) -> str:
    s = (s or "").strip().upper()
    s = s.replace("/", "").replace("-", "").replace("_", "")
    return s


def normalize_symbol_fast(s: str) -> str:
    s = normalize_symbol(s)
    # If user wrote "BTC" or "XRP" -> assume USDT quote
    if s.isalnum() and ("USDT" not in s) and (3 <= len(s) <= 5):
        s = f"{s}USDT"
    return s


async def fetch_market_price(symbol: str) -> float:
    """
    Returns market price or raises ValueError('SYMBOL_NOT_FOUND') for invalid tickers.
    """
    url = BINANCE_REST_PRICE.format(symbol=symbol)
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as resp:
            txt = await resp.text()
            if resp.status != 200:
                # Binance often returns: {"code":-1121,"msg":"Invalid symbol."}
                try:
                    j = json.loads(txt)
                    if isinstance(j, dict) and j.get("code") == -1121:
                        raise ValueError("SYMBOL_NOT_FOUND")
                except json.JSONDecodeError:
                    pass
                raise RuntimeError(f"Binance REST error {resp.status}: {txt}")

            data = json.loads(txt)
            return float(data["price"])


# =========================
# Logs message auto-delete
# =========================
async def delete_last_logs_message_if_any(app: Application, chat_id: int) -> None:
    msg_id = LAST_LOGS_MESSAGE_ID_BY_CHAT.pop(chat_id, None)
    if not msg_id:
        return
    try:
        await app.bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except Exception:
        pass


async def pre_command_cleanup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Any command except /logs will delete previous logs message.
    """
    try:
        if not update.effective_message or not update.effective_message.text:
            return
        txt = update.effective_message.text.strip()
        if not txt.startswith("/"):
            return
        if txt.startswith("/logs"):
            return
        await delete_last_logs_message_if_any(context.application, update.effective_chat.id)
    except Exception:
        pass


def _chunk_text(text: str, limit: int = 3500) -> List[str]:
    chunks = []
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut == -1:
            cut = limit
        chunks.append(text[:cut])
        text = text[cut:].lstrip("\n")
    if text:
        chunks.append(text)
    return chunks


# =========================
# WS management (same logic)
# =========================
async def ensure_ws_for_symbol(app: Application, symbol: str) -> None:
    if symbol in WS_TASK_BY_SYMBOL and not WS_TASK_BY_SYMBOL[symbol].done():
        return
    task = asyncio.create_task(ws_price_listener(app, symbol))
    WS_TASK_BY_SYMBOL[symbol] = task
    logger.info(f"WS started: {symbol}")


async def maybe_stop_ws_for_symbol(symbol: str) -> None:
    for alerts in ALERTS_BY_USER.values():
        for a in alerts:
            if a.symbol == symbol:
                return
    task = WS_TASK_BY_SYMBOL.get(symbol)
    if task and not task.done():
        task.cancel()
    WS_TASK_BY_SYMBOL.pop(symbol, None)
    LAST_PRICE.pop(symbol, None)
    logger.info(f"WS stopped: {symbol}")


async def ws_price_listener(app: Application, symbol: str) -> None:
    stream = f"{symbol.lower()}{WS_STREAM_SUFFIX}"
    url = f"{BINANCE_WS_BASE}/{stream}"

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                async for msg in ws:
                    data = json.loads(msg)
                    price = float(data.get("p"))
                    async with STATE_LOCK:
                        LAST_PRICE[symbol] = price
                    await check_alerts_and_notify(app, symbol, price)
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.exception(f"WS error {symbol}: {e}")
            await asyncio.sleep(2)
            async with STATE_LOCK:
                still_needed = any(
                    a.symbol == symbol for alerts in ALERTS_BY_USER.values() for a in alerts
                )
            if not still_needed:
                return


async def check_alerts_and_notify(app: Application, symbol: str, price: float) -> None:
    triggered: List[Tuple[int, Alert]] = []

    async with STATE_LOCK:
        for user_id, alerts in ALERTS_BY_USER.items():
            for a in alerts:
                if a.symbol != symbol:
                    continue
                if a.direction == "up" and price >= a.target:
                    triggered.append((user_id, a))
                elif a.direction == "down" and price <= a.target:
                    triggered.append((user_id, a))

        if not triggered:
            return

        for user_id, alert in triggered:
            ALERTS_BY_USER[user_id] = [
                x for x in ALERTS_BY_USER[user_id] if x.id != alert.id]
            save_user_alerts(user_id, ALERTS_BY_USER[user_id])

    for user_id, alert in triggered:
        direction_arrow = "🟢📈🟢" if alert.direction == "up" else "🔴📉🔴"
        text = (
            f"<b>🚨🚨ALERT TRIGGERED🚨🚨</b>\n"
            f"<b>Symbol:</b> <code>{alert.symbol}</code>\n"
            f"<b>Target:</b> <code>{alert.target}</code>\n"
            f"<b>Direction:</b> <code>{direction_arrow}</code>\n"
            f"<b>Last price:</b> <code>{price}</code>\n"
            f"<b>Alert ID:</b> <code>{alert.id}</code>"
        )
        try:
            await app.bot.send_message(chat_id=user_id, text=text, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.exception(f"send_message failed user={user_id}: {e}")

    async with STATE_LOCK:
        await maybe_stop_ws_for_symbol(symbol)


# =========================
# Telegram handlers (same commands + requested extras)
# =========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        user_id = update.effective_user.id
        async with STATE_LOCK:
            if user_id not in ALERTS_BY_USER:
                ALERTS_BY_USER[user_id] = load_user_alerts(user_id)

        msg = (
            "Hi! I can set Binance price alerts per ticker.\n\n"
            "Commands:\n"
            "• /info — show your alerts\n"
            "• /add BTC 65000 — add alert fast\n"
            "• /remove — /remove <id>\n"
        )
        await update.message.reply_text(msg)
        logger.info(f"/start user={user_id}")
    except Exception as e:
        logger.exception(f"cmd_start crashed: {e}")
        await update.message.reply_text("Error in /start.")


async def cmd_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        user_id = update.effective_user.id
        async with STATE_LOCK:
            if user_id not in ALERTS_BY_USER:
                ALERTS_BY_USER[user_id] = load_user_alerts(user_id)
            alerts = ALERTS_BY_USER[user_id]

        if not alerts:
            await update.message.reply_text("You have no alerts. Use /add BTC 65000 or /addw.")
            return

        lines = ["<b>Your alerts:</b>\n"]
        for a in alerts:
            arrow = "📈" if a.direction == "up" else "📉"
            lines.append(
                f"{arrow} <code>{a.symbol}</code> target <code>{a.target}</code> "
                f"(<code>{a.direction}</code>) — id: <code>{a.id}</code>"
            )
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
        logger.info(f"/info user={user_id} alerts={len(alerts)}")
    except Exception as e:
        logger.exception(f"cmd_info crashed: {e}")
        await update.message.reply_text("Error in /info.")


async def cmd_add_fast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /add BTC 65000
    /add BTCUSDT 65000
    """
    try:
        user_id = update.effective_user.id

        if not context.args or len(context.args) < 2:
            await update.message.reply_text("Usage: /add SYMBOL PRICE  (example: /add BTC 65000)")
            return

        symbol = normalize_symbol_fast(context.args[0])
        price_raw = context.args[1].replace(",", ".")
        try:
            target = float(price_raw)
            if target <= 0:
                raise ValueError()
        except ValueError:
            await update.message.reply_text("Invalid price. Example: /add BTC 65000")
            return

        try:
            market = await fetch_market_price(symbol)
        except ValueError as e:
            if str(e) == "SYMBOL_NOT_FOUND":
                await update.message.reply_text(f"❌ Ticker <code>{symbol}</code> not found on Binance.", parse_mode=ParseMode.HTML)
                return
            raise
        except Exception as e:
            await update.message.reply_text(f"Could not fetch market price for {symbol}.\nError: {e}")
            return

        if target > market:
            direction = "up"
        elif target < market:
            direction = "down"
        else:
            await update.message.reply_text(f"Target equals current price ({market}). Pick a different target.")
            return

        alert = Alert(
            id=uuid.uuid4().hex[:10],
            symbol=symbol,
            target=target,
            direction=direction,
            created_at=int(time.time()),
            market_at_create=market,
        )

        async with STATE_LOCK:
            if user_id not in ALERTS_BY_USER:
                ALERTS_BY_USER[user_id] = load_user_alerts(user_id)

            ALERTS_BY_USER[user_id].append(alert)
            save_user_alerts(user_id, ALERTS_BY_USER[user_id])
            await ensure_ws_for_symbol(context.application, symbol)

        arrow = "📈" if direction == "up" else "📉"
        await update.message.reply_text(
            f"{arrow} Added alert:\n"
            f"Symbol: {symbol}\n"
            f"Market now: {market}\n"
            f"Target: {target}\n"
            f"Direction: {direction}\n"
            f"ID: {alert.id}"
        )
        logger.info(
            f"add user={user_id} symbol={symbol} target={target} dir={direction}")
    except Exception as e:
        logger.exception(f"cmd_add_fast crashed: {e}")
        await update.message.reply_text("Error in /add.")


# ---- /add wizard (kept same as your file) ----
async def add_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Send ticker symbol (e.g. BTCUSDT, XRPUSDT):")
    return ADD_SYMBOL


async def add_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    symbol = normalize_symbol(update.message.text or "")
    if not symbol.isalnum() or len(symbol) < 6:
        await update.message.reply_text("Invalid symbol. Example: BTCUSDT")
        return ADD_SYMBOL

    context.user_data["add_symbol"] = symbol
    await update.message.reply_text(f"Now send alert price for {symbol} (example: 65000):")
    return ADD_PRICE


async def add_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        user_id = update.effective_user.id
        symbol = context.user_data.get("add_symbol")
        if not symbol:
            await update.message.reply_text("Something went wrong. Try /addw again.")
            return ConversationHandler.END

        raw = (update.message.text or "").strip().replace(",", ".")
        try:
            target = float(raw)
            if target <= 0:
                raise ValueError()
        except ValueError:
            await update.message.reply_text("Invalid price. Send a number, e.g. 65000 or 0.55")
            return ADD_PRICE

        try:
            market = await fetch_market_price(symbol)
        except ValueError as e:
            if str(e) == "SYMBOL_NOT_FOUND":
                await update.message.reply_text(f"❌ Ticker <code>{symbol}</code> not found on Binance.", parse_mode=ParseMode.HTML)
                return ConversationHandler.END
            raise
        except Exception as e:
            await update.message.reply_text(f"Could not fetch market price for {symbol}.\nError: {e}")
            return ConversationHandler.END

        if target > market:
            direction = "up"
        elif target < market:
            direction = "down"
        else:
            await update.message.reply_text(f"Target equals current price ({market}). Choose a different target.")
            return ConversationHandler.END

        alert = Alert(
            id=uuid.uuid4().hex[:10],
            symbol=symbol,
            target=target,
            direction=direction,
            created_at=int(time.time()),
            market_at_create=market,
        )

        async with STATE_LOCK:
            if user_id not in ALERTS_BY_USER:
                ALERTS_BY_USER[user_id] = load_user_alerts(user_id)

            ALERTS_BY_USER[user_id].append(alert)
            save_user_alerts(user_id, ALERTS_BY_USER[user_id])
            await ensure_ws_for_symbol(context.application, symbol)

        arrow = "📈" if direction == "up" else "📉"
        await update.message.reply_text(
            f"{arrow} Added alert:\n"
            f"Symbol: {symbol}\n"
            f"Market now: {market}\n"
            f"Target: {target}\n"
            f"Direction: {direction}\n"
            f"ID: {alert.id}"
        )
        logger.info(
            f"addw user={user_id} symbol={symbol} target={target} dir={direction}")
        return ConversationHandler.END
    except Exception as e:
        logger.exception(f"add_price crashed: {e}")
        await update.message.reply_text("Error in /addw flow.")
        return ConversationHandler.END


async def add_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


# ---- remove helpers + /remove wizard (same as your file, plus /remove <id>) ----
async def remove_id_from_text(update: Update, context: ContextTypes.DEFAULT_TYPE, alert_id: str) -> None:
    user_id = update.effective_user.id
    async with STATE_LOCK:
        if user_id not in ALERTS_BY_USER:
            ALERTS_BY_USER[user_id] = load_user_alerts(user_id)

        before = ALERTS_BY_USER[user_id]
        removed = next((a for a in before if a.id == alert_id), None)
        if not removed:
            await update.message.reply_text("No alert with that ID.")
            return

        ALERTS_BY_USER[user_id] = [a for a in before if a.id != alert_id]
        save_user_alerts(user_id, ALERTS_BY_USER[user_id])
        await maybe_stop_ws_for_symbol(removed.symbol)

    await update.message.reply_text(f"Removed alert {alert_id} for {removed.symbol}.")
    logger.info(f"remove user={user_id} id={alert_id} symbol={removed.symbol}")


async def remove_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # If user did: /remove <id> -> remove immediately
    if context.args and len(context.args) >= 1:
        await remove_id_from_text(update, context, context.args[0].strip())
        return ConversationHandler.END

    await update.message.reply_text("Send alert ID to remove (you can see IDs in /info):")
    return REMOVE_ID


async def remove_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    alert_id = (update.message.text or "").strip()
    await remove_id_from_text(update, context, alert_id)
    return ConversationHandler.END


async def remove_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


async def cmd_remove(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /remove <id>\nExample: /remove a1b2c3d4e5")
        return

    alert_id = context.args[0].strip()
    await remove_id_from_text(update, context, alert_id)

# =========================
# /logs 258079 50
# =========================


async def cmd_logs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        if not context.args or len(context.args) < 1:
            return
        if context.args[0].strip() != LOGS_PASSWORD:
            return

        n = 50
        if len(context.args) >= 2:
            try:
                n = int(context.args[1])
            except ValueError:
                n = 50
        n = max(1, min(n, 200))

        lines = list(LOG_BUFFER)[-n:]
        if not lines:
            text = "No logs."
        else:
            text = "\n".join(lines)

        # One message only; if too big, user should request smaller n
        parts = _chunk_text(text)
        sent = await update.message.reply_text(f"<pre>{parts[0]}</pre>", parse_mode=ParseMode.HTML)
        LAST_LOGS_MESSAGE_ID_BY_CHAT[update.effective_chat.id] = sent.message_id

        if len(parts) > 1:
            await update.message.reply_text("Logs too long for one message. Use smaller N, e.g. /logs 258079 50")

    except Exception as e:
        logger.exception(f"cmd_logs crashed: {e}")


# =========================
# Startup: load existing user configs (same)
# =========================
def load_all_users_from_disk() -> Dict[int, List[Alert]]:
    out: Dict[int, List[Alert]] = {}
    for name in os.listdir(DATA_DIR):
        if not name.endswith(".json"):
            continue
        try:
            user_id = int(name.replace(".json", ""))
        except ValueError:
            continue
        out[user_id] = load_user_alerts(user_id)
    return out


async def on_startup(app: Application) -> None:
    try:
        global ALERTS_BY_USER
        async with STATE_LOCK:
            ALERTS_BY_USER = load_all_users_from_disk()

            # Start WS only if alerts exist
            symbols_needed = sorted(
                {a.symbol for alerts in ALERTS_BY_USER.values() for a in alerts})
            for sym in symbols_needed:
                await ensure_ws_for_symbol(app, sym)

        logger.info(
            f"startup users={len(ALERTS_BY_USER)} symbols={len(symbols_needed)}")
    except Exception as e:
        logger.exception(f"on_startup crashed: {e}")


# =========================
# Main (keeps your handlers + fixes)
# =========================
def main() -> None:
    app = Application.builder().token(TOKEN).build()

    # Cleanup helper for logs message (must NOT block other command handlers)
    app.add_handler(
        MessageHandler(filters.COMMAND, pre_command_cleanup, block=False),
        group=0,
    )

    # Basic commands
    app.add_handler(CommandHandler("start", cmd_start), group=1)
    app.add_handler(CommandHandler("info", cmd_info), group=1)
    app.add_handler(CommandHandler("add", cmd_add_fast), group=1)
    app.add_handler(CommandHandler("logs", cmd_logs), group=1)

    # /add wizard
    add_conv = ConversationHandler(
        entry_points=[CommandHandler("addw", add_start)],
        states={
            ADD_SYMBOL: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_symbol)],
            ADD_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_price)],
        },
        fallbacks=[CommandHandler("cancel", add_cancel)],
    )
    app.add_handler(add_conv, group=2)

    remove_conv = ConversationHandler(
        entry_points=[CommandHandler("remove", remove_start)],
        states={
            REMOVE_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, remove_id)],
        },
        fallbacks=[CommandHandler("cancel", remove_cancel)],
    )
    app.add_handler(remove_conv, group=2)
    app.post_init = on_startup
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
