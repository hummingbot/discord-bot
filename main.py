import asyncio
import os
import pandas as pd
import discord
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

intents = discord.Intents.default()
intents.messages = True
client = discord.Client(intents=intents)
CHANNEL_IDS = [int(cid) for cid in os.environ.get('CHANNEL_IDS', "").split(',')]  # List of channel IDs
TOKEN = os.environ.get('DISCORD_TOKEN')
START_DATE = '2024-03-01'
END_DATE = '2024-03-22'
WHITELISTED_USERS = {'david_alvero'}  # Set of user names
WHITELISTED_REACTIONS = {'1️⃣', '2️⃣', "3️⃣"}  # Set of allowed reactions
excel_path = 'data/discord_messages.xlsx'


async def fetch_messages(channel_id):
    messages_info = []
    target_channel = client.get_channel(channel_id)
    if target_channel:
        start_date = datetime.strptime(START_DATE, '%Y-%m-%d').date()
        end_date = datetime.strptime(END_DATE, '%Y-%m-%d').date()
        async for msg in target_channel.history(limit=1000):
            message_date = msg.created_at.date()
            if start_date <= message_date <= end_date:
                for reaction in msg.reactions:
                    if str(reaction.emoji) in WHITELISTED_REACTIONS:
                        async for user in reaction.users():
                            if user.name in WHITELISTED_USERS:
                                message_url = f"https://discord.com/channels/{msg.guild.id}/{channel_id}/{msg.id}"
                                messages_info.append({
                                    'id': msg.id,
                                    'author': msg.author.name,
                                    'message': msg.content,
                                    'reaction': str(reaction),
                                    'user': user.name,
                                    'url': message_url
                                })
                                break
                    else:
                        message_url = f"https://discord.com/channels/{msg.guild.id}/{channel_id}/{msg.id}"
                        messages_info.append({
                            'id': msg.id,
                            'author': msg.author.name,
                            'message': msg.content,
                            'reaction': "",
                            'user': "",
                            'url': message_url
                        })
    return messages_info



def read_existing_data(path):
    try:
        return pd.read_excel(path)
    except FileNotFoundError:
        # If file doesn't exist, return an empty DataFrame
        return pd.DataFrame()


def save_to_excel(data, path):
    df = pd.DataFrame(data)
    df.to_excel(path, index=False)


def append_new_messages(existing_df, new_data):
    new_df = pd.DataFrame(new_data)
    updated_df = pd.concat([existing_df, new_df]).drop_duplicates(subset='id', keep='last').reset_index(drop=True)
    return updated_df


@client.event
async def on_ready():
    print(f'{client.user} has connected to Discord!')
    await asyncio.sleep(1)
    existing_df = read_existing_data(excel_path)
    all_messages = []
    for channel_id in CHANNEL_IDS:
        messages_data = await fetch_messages(channel_id)
        all_messages.extend(messages_data)
    updated_df = append_new_messages(existing_df, all_messages)
    save_to_excel(updated_df, excel_path)
    await client.close()


client.run(TOKEN)
