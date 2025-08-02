import discord
from discord.ext import commands
import asyncio
import logging
from dotenv import load_dotenv
import os
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')

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
    logging.error(f"No BOT_TOKEN found in .env.{script_basename} ❌")
    exit(1)

# Initialize the bot with required intents
intents = discord.Intents.default()
intents.guilds = True  # Required to access guild (server) data
intents.message_content = False  # Disable message content intent
bot = commands.Bot(command_prefix='!', intents=intents)

# Event: Bot is ready
@bot.event
async def on_ready():
    logging.info(f'Logged in as {bot.user}')
    
    # Define the target server by Guild ID
    guild = bot.get_guild(args.server_id)
    if guild is None:
        logging.error(f"Server with ID {args.server_id} not found! ❌")
        await bot.close()
        return
    
    # Find the channel by Channel ID
    channel = guild.get_channel(args.channel_id)
    if channel is None:
        logging.error(f"Channel with ID {args.channel_id} not found! ❌")
        await bot.close()
        return
    
    # New channel name
    new_name = args.channel_name
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Update the channel name
            await channel.edit(name=new_name)
            logging.info(f"Updated channel name to '{new_name}' ✅")
            break
        except discord.errors.HTTPException as e:
            if e.code == 429:  # Rate limit error
                retry_after = e.retry_after or 10  # Default to 10 seconds if retry_after is missing
                logging.warning(f"Rate limited on attempt {attempt + 1}. Retrying after {retry_after} seconds ⏳")
                await asyncio.sleep(retry_after)
                if attempt == max_retries - 1:
                    logging.error("Max retries reached. Failed to update channel name due to rate limits ❌")
            else:
                logging.error(f"Failed to update channel name: {e} ❌")
                break
        except discord.errors.Forbidden:
            logging.error("Bot lacks permission to edit the channel! ❌")
            break
        except Exception as e:
            logging.error(f"Unexpected error: {e} ❌")
            break
    else:
        logging.error("Failed to update channel name after all retries ❌")

    # Close the bot connection
    try:
        await bot.close()
        logging.info("Bot has been shut down.")
    except Exception as e:
        logging.error(f"Error closing bot: {e} ❌")

# Run the bot
bot.run(bot_token)