import asyncio
import logging
import os
import pandas as pd
import discord
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Configuration variables
START_DATE = '2025-01-01'
END_DATE = '2025-02-04'
WHITELISTED_USERS = {'david_alvero', 'fengtality', 'carlitohummingbotfoundation', 'hbminerfan', 'w0jak'}
WHITELISTED_REACTIONS = {'1️⃣', '2️⃣', '3️⃣'}
REACTION_POINTS = {'1️⃣': 1, '2️⃣': 2, '3️⃣': 3}
excel_path = 'data/discord_messages.xlsx'

# Define the start date of week #71
WEEK_71_START_DATE = datetime.strptime('2024-04-16', '%Y-%m-%d')

# Discord bot setup
intents = discord.Intents.default()
intents.messages = True
client = discord.Client(intents=intents)
TOKEN = os.environ.get('DISCORD_TOKEN')
CHANNEL_IDS = [int(cid) for cid in os.environ.get('CHANNEL_IDS', "").split(',')]

logging.basicConfig(level=logging.INFO)

async def fetch_messages(channel_id):
    messages_info = []
    target_channel = client.get_channel(channel_id)
    start_date = datetime.strptime(START_DATE, '%Y-%m-%d')
    end_date = datetime.strptime(END_DATE, '%Y-%m-%d') + timedelta(days=1)
    
    if target_channel:
        after_date = start_date
        before_date = start_date + timedelta(days=1)
        
        # Fetch regular messages in the channel
        while after_date < end_date:
            logging.info(f"Fetching messages from channel {target_channel.name} from {after_date} to {before_date}")
            async for msg in target_channel.history(limit=1000, before=before_date, after=after_date):
                messages_info.extend(await process_message(msg, channel_id))
            after_date = before_date
            before_date += timedelta(days=1)

        # Fetch messages from threads within the date range
        for thread in target_channel.threads:
            logging.info(f"Fetching messages from thread {thread.name} in channel {target_channel.name}")
            async for msg in thread.history(limit=1000):
                if start_date <= msg.created_at.replace(tzinfo=None) < end_date:
                    messages_info.extend(await process_message(msg, thread.id))
        
    return messages_info



async def process_message(msg, channel_id):
    messages_info = []
    for reaction in msg.reactions:
        if str(reaction.emoji) in WHITELISTED_REACTIONS:
            users = [user async for user in reaction.users()]
            evaluator_names = [user.name for user in users if user.name in WHITELISTED_USERS]
            if evaluator_names:
                points = sum(REACTION_POINTS[str(reaction.emoji)] for user in users if user.name in WHITELISTED_USERS)
                message_date = msg.created_at.replace(tzinfo=None)  # Make message_date offset-naive
                # Calculate week number
                days_diff = (message_date - WEEK_71_START_DATE).days
                week_no = 71 + days_diff // 7
                messages_info.append({
                    'id': msg.id,
                    'date': msg.created_at.strftime('%d-%b-%y'),
                    'Week No.': week_no,
                    'channel': msg.channel.name,
                    'Participants Discord Handle': msg.author.name,
                    'message': msg.content,
                    'reaction': str(reaction.emoji),
                    'points': points,
                    'Evaluator': ', '.join(evaluator_names),
                    'url': f"https://discord.com/channels/{msg.guild.id}/{channel_id}/{msg.id}"
                })
    return messages_info

def read_existing_data(path):
    try:
        return pd.read_excel(path)
    except FileNotFoundError:
        return pd.DataFrame()

def save_to_excel(data, path):
    if not data.empty:
        data.to_excel(path, index=False)

def append_new_messages(existing_df, new_data):
    new_df = pd.DataFrame(new_data)
    updated_df = pd.concat([existing_df, new_df]).drop_duplicates(subset='id', keep='last').reset_index(drop=True)
    return updated_df

@client.event
async def on_ready():
    print(f'{client.user} has connected to Discord!')
    existing_df = read_existing_data(excel_path)
    all_messages = []
    for channel_id in CHANNEL_IDS:
        messages_data = await fetch_messages(channel_id)
        all_messages.extend(messages_data)
    updated_df = append_new_messages(existing_df, all_messages)
    save_to_excel(updated_df, excel_path)
    await client.close()

client.run(TOKEN)
