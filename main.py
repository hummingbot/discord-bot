import asyncio
import logging
import os
import pandas as pd
import discord
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

START_DATE = '2024-03-01'
END_DATE = '2024-03-28'
WHITELISTED_USERS = {'david_alvero', 'fengtality'}  # Set of user names
WHITELISTED_REACTIONS = {'1️⃣', '2️⃣', "3️⃣"}  # Set of allowed reactions
excel_path = 'data/discord_messages.xlsx'


intents = discord.Intents.default()
intents.messages = True
client = discord.Client(intents=intents)
TOKEN = os.environ.get('DISCORD_TOKEN')
CHANNEL_IDS = [int(cid) for cid in os.environ.get('CHANNEL_IDS', "").split(',')]  # List of channel IDs

logging.basicConfig(level=logging.INFO)


async def fetch_messages(channel_id):
    messages_info = []
    total_messages = 0
    target_channel = client.get_channel(channel_id)
    start_date = datetime.strptime(START_DATE, '%Y-%m-%d')
    end_date = datetime.strptime(END_DATE, '%Y-%m-%d')
    if target_channel:
        after_date = start_date
        before_date = start_date + pd.Timedelta(days=1)
        while after_date != end_date:
            logging.info(f"Fetching messages from channel {target_channel.name}")
            logging.info(f"After date: {after_date} - Before date: {before_date}")
            async for msg in target_channel.history(limit=1000, before=before_date, after=after_date):
                total_messages += 1
                message_url = f"https://discord.com/channels/{msg.guild.id}/{channel_id}/{msg.id}"
                users_reacted = set()
                reactions = set()
                for reaction in msg.reactions:
                    if str(reaction.emoji) in WHITELISTED_REACTIONS:
                        async for user in reaction.users():
                            users_reacted.add(user.name)
                            reactions.add(str(reaction.emoji))

                messages_info.append({
                    'id': msg.id,
                    'channel': target_channel.name,
                    'author': msg.author.name,
                    'message': msg.content,
                    'reaction': reactions if len(reactions) > 0 else None,
                    'user': users_reacted if len(users_reacted) > 0 else None,
                    'url': message_url
                })
            after_date = before_date
            before_date = before_date + pd.Timedelta(days=1)
    logging.info(f"Total messages fetched: {total_messages} for channel {target_channel.name}")
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
