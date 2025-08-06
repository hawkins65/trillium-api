# ss-discord-update-channel-with-stake.py

# Standard library
import argparse
import asyncio
import importlib.util

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
import os
import sys
from pathlib import Path

# Third-party
import discord
from discord.ext import commands
from dotenv import load_dotenv

# Set up logging
# Logging config moved to unified configurations %(levelname)-8s %(message)s')

# ── QUIET MODE: suppress discord.py’s own INFO chatter ───
for lib in ('discord', 'discord.gateway', 'discord.client', 'discord.ext.commands'):
    logging.getLogger(lib).setLevel(logging.WARNING)

# Get script basename for .env file and logging
script_basename = os.path.splitext(os.path.basename(__file__))[0]

# Resolve this script’s directory
script_dir = Path(__file__).resolve().parent

# 1) Primary: look for .env.<basename> next to this script
env_file = script_dir / f'.env.{script_basename}'

# 2) Fallback: ~/ss-discord-update-channel/.env.<basename>
if not env_file.exists():
    env_file = Path.home() / 'ss-discord-update-channel' / f'.env.{script_basename}'

# 3) Exit if still not found
if not env_file.exists():
    sys.exit(
        f"ERROR: dotenv file not found at either:\n"
        f"  {script_dir}/.env.{script_basename}\n"
        f"  {Path.home()}/ss-discord-update-channel/.env.{script_basename}"
    )

# Load it
load_dotenv(dotenv_path=env_file)

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Update a Discord channel name.')
parser.add_argument('--server-id', type=int, required=True, help='The Discord server (guild) ID')
parser.add_argument('--channel-id', type=int, required=True, help='The Discord channel ID')
parser.add_argument('--channel-name', type=str, required=True, help='The new channel name')
args = parser.parse_args()

# Get bot token
bot_token = os.getenv('BOT_TOKEN')
if not bot_token:
    logger.error(f"No BOT_TOKEN found in .env.{script_basename} ❌")
    sys.exit(1)
else:
    logger.info(f"Successfully read BOT_TOKEN ending in {bot_token[-5:]}")

# Initialize the bot with required intents
intents = discord.Intents.default()
intents.guilds = True  # Required to access guild (server) data
intents.message_content = False  # Disable message content intent
bot = commands.Bot(command_prefix='!', intents=intents)

# Event: Bot is ready
@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user}')
    
    # Define the target server by Guild ID
    guild = bot.get_guild(args.server_id)
    if guild is None:
        logger.error(f"Server with ID {args.server_id} not found! ❌")
        await bot.close()
        return

    # Log guild-wide role permissions (now at DEBUG level)
    bot_member = guild.get_member(bot.user.id)
    role_permissions = bot_member.guild_permissions
    logger.debug(f"Bot guild-wide permissions: {role_permissions}")
    logger.debug(f"Guild Manage Channels: {role_permissions.manage_channels}")

    # Find the channel by Channel ID
    channel = guild.get_channel(args.channel_id)
    if channel is None:
        logger.error(f"Channel with ID {args.channel_id} not found! ❌")
        await bot.close()
        return

    # Log bot's permissions for the channel (now at DEBUG level)
    permissions = channel.permissions_for(guild.me)
    logger.debug(f"Bot permissions for channel {channel.id}: {permissions}")
    if not permissions.manage_channels:
        logger.error("Bot lacks 'Manage Channels' permission for this channel! ❌")

    # Log category permissions if channel is in a category (now at DEBUG level)
    if channel.category:
        category_permissions = channel.category.permissions_for(guild.me)
        logger.debug(f"Bot permissions for category {channel.category.name} (ID: {channel.category.id}): {category_permissions}")
        logger.debug(f"Category Manage Channels: {category_permissions.manage_channels}")

    # New channel name
    new_name = args.channel_name
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Update the channel name
            await channel.edit(name=new_name)
            logger.info(f"Updated channel name to '{new_name}' ✅")
            break
        except discord.errors.HTTPException as e:
            if e.code == 429:  # Rate limit error
                retry_after = e.retry_after or 10  # Default to 10 seconds if missing
                logger.warning(f"Rate limited on attempt {attempt + 1}. Retrying after {retry_after} seconds ⏳")
                await asyncio.sleep(retry_after)
                if attempt == max_retries - 1:
                    logger.error("Max retries reached. Failed to update channel name due to rate limits ❌")
            else:
                logger.error(f"Failed to update channel name: {e} ❌")
                break
        except discord.errors.Forbidden:
            logger.error("Bot lacks permission to edit the channel! ❌")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e} ❌")
            break
    else:
        logger.error("Failed to update channel name after all retries ❌")

    # Close the bot connection
    try:
        await bot.close()
        logger.info("Bot has been shut down.")
    except Exception as e:
        logger.error(f"Error closing bot: {e} ❌")

# Run the bot
bot.run(bot_token)
