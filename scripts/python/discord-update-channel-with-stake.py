import discord
from discord.ext import commands
import asyncio
import importlib.util

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
from dotenv import load_dotenv
import os
import argparse

# Set up logging
# Logging config moved to unified configurations %(levelname)-8s %(message)s')

# Get script basename for .env file and logging
script_basename = os.path.splitext(os.path.basename(__file__))[0]

# Load environment variables from specific .env file
load_dotenv(dotenv_path=f'.env.{script_basename}')

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
    exit(1)

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
    
    # Find the channel by Channel ID
    channel = guild.get_channel(args.channel_id)
    if channel is None:
        logger.error(f"Channel with ID {args.channel_id} not found! ❌")
        await bot.close()
        return
    
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
                retry_after = e.retry_after or 10  # Default to 10 seconds if retry_after is missing
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