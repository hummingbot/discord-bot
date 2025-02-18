import os
import asyncio
import logging
from datetime import datetime, timedelta

import discord
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

# -------------------- Environment Variables --------------------
# Main bot credentials and channels
MAIN_DISCORD_TOKEN = os.getenv('MAIN_DISCORD_TOKEN')
MAIN_CHANNEL_IDS = [int(cid) for cid in os.getenv('MAIN_CHANNEL_IDS', "").split(',') if cid.strip()]

# Fetch bot credentials and channels
FETCH_DISCORD_TOKEN = os.getenv('FETCH_DISCORD_TOKEN')
FETCH_CHANNEL_IDS = [int(cid) for cid in os.getenv('FETCH_CHANNEL_IDS', "").split(',') if cid.strip()]

# Other configuration
WHITELISTED_USERS = set(os.getenv('WHITELISTED_USERS', '').split(','))
WHITELISTED_REACTIONS = set(os.getenv('WHITELISTED_REACTIONS', '1️⃣,2️⃣,3️⃣').split(','))
REACTION_POINTS = {
    reaction: int(points) 
    for reaction, points in [
        item.split(':') 
        for item in os.getenv('REACTION_POINTS', '1️⃣:1,2️⃣:2,3️⃣:3').split(',')
    ]
}
EXCEL_PATH = os.getenv('EXCEL_PATH', 'data/discord_messages.xlsx')
WEEK_71_START_DATE = datetime.strptime('2024-04-16', '%Y-%m-%d')

# -------------------- Main Bot Setup --------------------
intents = discord.Intents.default()
intents.message_content = True  # Needed for on_message
bot = commands.Bot(command_prefix="!", intents=intents)

# -------------------- Helper Functions (for Fetching & Saving) --------------------
async def fetch_messages(client: discord.Client, channel_id: int, start_date: datetime, end_date: datetime):
    """Fetch messages in a channel (or its threads) between start_date and end_date (inclusive)."""
    messages_info = []
    target_channel = client.get_channel(channel_id)
    actual_end_date = end_date + timedelta(days=1)  # include the end date fully

    if target_channel:
        after_date = start_date
        before_date = start_date + timedelta(days=1)
        while after_date < actual_end_date:
            logging.info(f"Fetching messages from channel {target_channel.name} from {after_date} to {before_date}")
            async for msg in target_channel.history(limit=1000, before=before_date, after=after_date):
                messages_info.extend(await process_message(msg, channel_id))
            after_date = before_date
            before_date += timedelta(days=1)

        # Also fetch messages from threads in the channel
        for thread in target_channel.threads:
            logging.info(f"Fetching messages from thread {thread.name} in channel {target_channel.name}")
            async for msg in thread.history(limit=1000):
                if start_date <= msg.created_at.replace(tzinfo=None) < actual_end_date:
                    messages_info.extend(await process_message(msg, thread.id))
    return messages_info

async def process_message(msg, channel_id: int):
    """Process a message by checking for whitelisted reactions and extract data if applicable."""
    messages_info = []
    for reaction in msg.reactions:
        if str(reaction.emoji) in WHITELISTED_REACTIONS:
            users = [user async for user in reaction.users()]
            evaluator_names = [user.name for user in users if user.name in WHITELISTED_USERS]
            if evaluator_names:
                points = sum(REACTION_POINTS[str(reaction.emoji)] for user in users if user.name in WHITELISTED_USERS)
                message_date = msg.created_at.replace(tzinfo=None)
                days_diff = (message_date - WEEK_71_START_DATE).days
                week_no = 71 + days_diff // 7
                messages_info.append({
                    'Date': msg.created_at.strftime('%d-%b-%y'),
                    'Week No.': week_no,
                    'Participants Discord Handle': msg.author.name,
                    'Points': points,
                    'Evaluator': ', '.join(evaluator_names),
                    'url': f"https://discord.com/channels/{msg.guild.id}/{channel_id}/{msg.id}"
                })
    return messages_info

def read_existing_data(path: str) -> pd.DataFrame:
    try:
        return pd.read_excel(path)
    except FileNotFoundError:
        return pd.DataFrame()

def save_to_excel(data: pd.DataFrame, path: str):
    if not data.empty:
        data.to_excel(path, index=False)

def append_new_messages(existing_df: pd.DataFrame, new_data: list) -> pd.DataFrame:
    new_df = pd.DataFrame(new_data)
    updated_df = pd.concat([existing_df, new_df]).drop_duplicates(subset='url', keep='last').reset_index(drop=True)
    return updated_df

# -------------------- Slash Command: /fetch_and_save --------------------
@bot.tree.command(
    name="fetch_and_save",
    description="Fetch messages between dates and save to Excel. Format: YYYY-MM-DD"
)
async def fetch_and_save(interaction: discord.Interaction, start_date: str, end_date: str):
    # Permission check: only users with the 'hummingbot-admin' role may run this command.
    if interaction.guild is None or not any(role.name == "hummingbot-admin" for role in interaction.user.roles):
        await interaction.response.send_message("Error: You do not have permission to run this command.", ephemeral=True)
        return

    # Parse the provided start and end dates
    try:
        start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        await interaction.response.send_message("Error: Please use the date format YYYY-MM-DD for both start_date and end_date.", ephemeral=True)
        return

    # Defer and then update the original response with a start message
    await interaction.response.defer(ephemeral=True)
    await interaction.edit_original_response(content="Fetching messages. Please wait...")

    try:
        # Run the fetch bot process to get messages from FETCH_CHANNEL_IDS within the specified date range
        all_messages = await run_fetch_bot(start_date_dt, end_date_dt)
        # Read, update, and save Excel file
        existing_df = read_existing_data(EXCEL_PATH)
        updated_df = append_new_messages(existing_df, all_messages)
        save_to_excel(updated_df, EXCEL_PATH)
        await interaction.edit_original_response(content="Fetch and save completed successfully.")
    except Exception as e:
        logging.exception("An error occurred while fetching and saving messages.")
        await interaction.edit_original_response(content=f"An error occurred: {e}")

# -------------------- Temporary Fetch Bot Function --------------------
async def run_fetch_bot(start_date: datetime, end_date: datetime):
    """Creates a temporary client (fetch bot) to log in with FETCH_DISCORD_TOKEN and fetch messages within the given date range."""
    fetch_intents = discord.Intents.default()
    fetch_intents.messages = True
    fetch_client = discord.Client(intents=fetch_intents)

    fetched_messages = []

    @fetch_client.event
    async def on_ready():
        logging.info(f"{fetch_client.user} has connected as the fetch bot!")
        for channel_id in FETCH_CHANNEL_IDS:
            messages_data = await fetch_messages(fetch_client, channel_id, start_date, end_date)
            fetched_messages.extend(messages_data)
        await fetch_client.close()

    await fetch_client.start(FETCH_DISCORD_TOKEN)
    return fetched_messages

# -------------------- Slash Command: /download_excel --------------------

@bot.tree.command(
    name="download_excel",
    description="Download the Excel file with the fetched data."
)
async def download_excel(interaction: discord.Interaction):
    # Optional: restrict access to certain roles
    if interaction.guild is None or not any(role.name == "hummingbot-admin" for role in interaction.user.roles):
        await interaction.response.send_message("Error: You do not have permission to run this command.", ephemeral=True)
        return

    if not os.path.exists(EXCEL_PATH):
        await interaction.response.send_message("Error: Excel file not found. Please run /fetch_and_save first.", ephemeral=True)
        return

    # Send the file as an attachment (ephemeral responses cannot include attachments)
    await interaction.response.send_message("Here is the Excel file:", file=discord.File(EXCEL_PATH))


# -------------------- on_ready and on_message for Main Bot --------------------
@bot.event
async def on_ready():
    try:
        await bot.tree.sync()
        logging.info("Slash commands synced successfully.")
    except Exception as e:
        logging.error(f"Error syncing commands: {e}")
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    print("------")

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return
    if message.channel.id in MAIN_CHANNEL_IDS:
        response_text = f"Hello {message.author.mention}"
        if message.content:
            response_text += f", I got your message: {message.content}"
        if message.attachments:
            response_text += "\nI also received these attachments:"
            for attachment in message.attachments:
                if attachment.content_type and attachment.content_type.startswith('image/'):
                    response_text += f"\n- Image: {attachment.filename}"
                else:
                    response_text += f"\n- File: {attachment.filename}"
        await message.channel.send(response_text)
    await bot.process_commands(message)

# -------------------- Start the Main Bot --------------------
bot.run(MAIN_DISCORD_TOKEN)
