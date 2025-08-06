#!/usr/bin/env python3
import telebot
import json
import os
import sys
import importlib.util
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))

# Load configuration from standardized config system
CONFIG_PATH = '/home/smilax/trillium_api/data/configs/telegram_config.json'
SUBSCRIBERS_PATH = '/home/smilax/trillium_api/data/telegram/subscribers.json'

def load_config():
    """Load telegram configuration from standardized config file."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = json.load(f)
        return config['telegram']
    except Exception as e:
        logger.error(f"Failed to load telegram config from {CONFIG_PATH}: {e}")
        raise

def ensure_data_directory():
    """Ensure the telegram data directory exists."""
    data_dir = os.path.dirname(SUBSCRIBERS_PATH)
    os.makedirs(data_dir, exist_ok=True)

# Load configuration
try:
    telegram_config = load_config()
    BOT_TOKEN = telegram_config['bot_token']
    ADMIN_ID = int(telegram_config['chat_configs']['general_alerts']['chat_id'])
    
    # Message formatting settings
    msg_format = telegram_config.get('message_formatting', {})
    PARSE_MODE = msg_format.get('parse_mode', 'MarkdownV2')
    DISABLE_WEB_PREVIEW = msg_format.get('disable_web_page_preview', True)
    
    logger.info("Telegram bot configuration loaded successfully")
except Exception as e:
    logger.critical(f"Failed to initialize telegram bot: {e}")
    sys.exit(1)

# Ensure data directory exists
ensure_data_directory()

bot = telebot.TeleBot(BOT_TOKEN)

# Ensure the subscribers JSON file exists
if not os.path.exists(SUBSCRIBERS_PATH):
    with open(SUBSCRIBERS_PATH, 'w') as f:
        json.dump([], f)
    logger.info(f"Created new subscribers file at {SUBSCRIBERS_PATH}")

def load_subscribers():
    """Load subscribers from JSON file."""
    try:
        with open(SUBSCRIBERS_PATH, 'r') as f:
            return set(json.load(f))
    except Exception as e:
        logger.error(f"Error loading subscribers: {str(e)}")
        return set()

def save_subscribers(subscribers):
    """Save subscribers to JSON file."""
    try:
        with open(SUBSCRIBERS_PATH, 'w') as f:
            json.dump(list(subscribers), f)
        logger.debug(f"Saved {len(subscribers)} subscribers to file")
    except Exception as e:
        logger.error(f"Error saving subscribers: {str(e)}")

subscribers = load_subscribers()
logger.info(f"Loaded {len(subscribers)} existing subscribers")

@bot.message_handler(commands=['start'])
def send_welcome(message):
    """Handle /start command."""
    try:
        keyboard = InlineKeyboardMarkup()
        keyboard.row(InlineKeyboardButton("Subscribe", callback_data="subscribe"))
        keyboard.row(InlineKeyboardButton("Privacy Policy", url="https://trillium.so/pages/trillium-privacy-policy.html"))
        welcome_text = "Welcome to Trillium API Updates\\! Click the button below to subscribe\\. Please review our Privacy Policy\\."
        bot.reply_to(message, welcome_text, reply_markup=keyboard, parse_mode=PARSE_MODE, disable_web_page_preview=DISABLE_WEB_PREVIEW)
        logger.info(f"User {message.from_user.id} started the bot")
    except Exception as e:
        logger.error(f"Error in send_welcome: {str(e)}")

@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    """Handle inline button callbacks."""
    try:
        if call.data == "subscribe":
            user_id = call.from_user.id
            if user_id not in subscribers:
                subscribers.add(user_id)
                save_subscribers(subscribers)
                bot.answer_callback_query(call.id, "You've successfully subscribed to Trillium API updates!")
                logger.info(f"User {user_id} subscribed (total subscribers: {len(subscribers)})")
            else:
                bot.answer_callback_query(call.id, "You're already subscribed.")
                logger.info(f"User {user_id} attempted to subscribe again")
        else:
            logger.warning(f"Received unexpected callback data: {call.data}")
    except Exception as e:
        logger.error(f"Error in callback_query: {str(e)}")

@bot.message_handler(commands=['unsubscribe'])
def unsubscribe(message):
    """Handle /unsubscribe command."""
    try:
        user_id = message.from_user.id
        if user_id in subscribers:
            subscribers.remove(user_id)
            save_subscribers(subscribers)
            bot.send_message(message.chat.id, "You've been unsubscribed from Trillium API updates\\.", parse_mode=PARSE_MODE)
            logger.info(f"User {user_id} unsubscribed (total subscribers: {len(subscribers)})")
        else:
            bot.send_message(message.chat.id, "You're not currently subscribed\\.", parse_mode=PARSE_MODE)
            logger.info(f"User {user_id} attempted to unsubscribe but was not subscribed")
    except Exception as e:
        logger.error(f"Error in unsubscribe: {str(e)}")

@bot.message_handler(commands=['help'])
def send_help(message):
    """Handle /help command."""
    try:
        help_text = """
━━━━━━━━━━━━━━━━━
* Trillium API Updates Bot Help *
━━━━━━━━━━━━━━━━━

/start \\- Start the bot and see the subscribe option
/unsubscribe \\- Unsubscribe from Trillium API updates
/help \\- Get information about using this bot

For more information, visit [https://trillium\\.so](https://trillium.so)
To view our privacy policy, visit [https://trillium\\.so/pages/trillium\\-privacy\\-policy\\.html](https://trillium.so/pages/trillium-privacy-policy.html)
        """
        bot.send_message(message.chat.id, help_text, parse_mode=PARSE_MODE, disable_web_page_preview=DISABLE_WEB_PREVIEW)
        logger.info(f"User {message.from_user.id} requested help")
    except Exception as e:
        logger.error(f"Error in send_help: {str(e)}")

def format_update_message(epoch_number):
    """Format epoch update message."""
    return f"""🔔 Trillium API Update 🔔

Epoch: {epoch_number}
Status: Data updated
For more information, visit https://trillium\\.so"""

@bot.message_handler(func=lambda message: message.from_user.id == ADMIN_ID and message.text.startswith('/send_update'))
def send_update(message):
    """Handle admin /send_update command."""
    try:
        epoch_number = int(message.text.split()[1])
        update_text = format_update_message(epoch_number)
        logger.info(f"Admin initiated update broadcast for epoch {epoch_number}")
    except (IndexError, ValueError):
        bot.send_message(message.chat.id, "Please provide the epoch number after the /send\\_update command\\.", parse_mode=PARSE_MODE)
        logger.warning("Admin attempted to send an update with invalid format")
        return

    success_count = 0
    failed_count = 0
    
    for user_id in subscribers:
        try:
            bot.send_message(user_id, update_text, parse_mode=PARSE_MODE, disable_web_page_preview=DISABLE_WEB_PREVIEW)
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to send message to {user_id}: {e}")
            failed_count += 1

    bot.send_message(message.chat.id, f"Update for epoch {epoch_number} sent to {success_count} out of {len(subscribers)} subscribers\\.", parse_mode=PARSE_MODE)
    logger.info(f"Epoch {epoch_number} update sent to {success_count} subscribers, {failed_count} failed")

@bot.message_handler(commands=['stats'])
def send_stats(message):
    """Handle admin /stats command."""
    if message.from_user.id != ADMIN_ID:
        return
        
    try:
        stats_text = f"""📊 *Telegram Bot Statistics*

👥 Total Subscribers: {len(subscribers)}
🤖 Bot Status: Running
📍 Config Location: {CONFIG_PATH}
💾 Data Location: {SUBSCRIBERS_PATH}"""
        
        bot.send_message(message.chat.id, stats_text, parse_mode=PARSE_MODE)
        logger.info(f"Admin {message.from_user.id} requested bot statistics")
    except Exception as e:
        logger.error(f"Error in send_stats: {str(e)}")

if __name__ == "__main__":
    logger.info("Telegram bot starting up...")
    logger.info(f"Bot configuration loaded from: {CONFIG_PATH}")
    logger.info(f"Subscribers data location: {SUBSCRIBERS_PATH}")
    logger.info(f"Admin ID: {ADMIN_ID}")
    logger.info(f"Initial subscriber count: {len(subscribers)}")
    
    try:
        logger.info("Starting bot polling...")
        bot.polling(none_stop=True)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.critical(f"Critical error in bot polling: {str(e)}")
        sys.exit(1)