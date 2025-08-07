import discord
import openai
import os
from collections import Counter

DISCORD_TOKEN = os.getenv('DISCORD_BOT_TOKEN')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Initialize OpenAI and Discord clients
openai.api_key = os.getenv('OPENAI_API_KEY')
client = discord.Client(intents=discord.Intents.all())

@client.event
async def on_ready():
    print(f'{client.user} has connected to Discord!')

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.content.startswith('!ask'):
        user_query = message.content[len('!ask '):]
        # Fetch history
        messages = await message.channel.history(limit=100).flatten()

        # Analyze responses
        responses = [msg.content for msg in messages if msg.author != client.user]
        most_common_response = Counter(responses).most_common(1)[0][0]

        # Send to OpenAI
        prompt = f"User asked: {user_query}\nMost popular response: {most_common_response}\nChatGPT, what do you think?"
        openai_response = openai.Completion.create(
            engine="gpt-4",
            prompt=prompt,
            max_tokens=150
        )

        # Send ChatGPT response to Discord
        await message.channel.send(openai_response.choices[0].text.strip())

client.run(DISCORD_TOKEN)
