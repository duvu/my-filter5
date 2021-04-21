# dbot.py
import asyncio
import os

import discord
from dotenv import load_dotenv

from stock import Stock

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')
CHANNEL_ID = os.getenv('DISCORD_CHANNEL')

# client = discord.Client()
# client = commands.Bot(command_prefix=".")


class MyClient(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # create the background task and run it in the background
        self.bg_task = self.loop.create_task(self.my_background_task())

    async def on_ready(self):
        print(f'Logged in as {self.user} (ID: {self.user.id})')
        print('------')

    async def my_background_task(self):
        await self.wait_until_ready()
        counter = 0
        channel = self.get_channel(int(CHANNEL_ID))  # channel ID goes here
        print(f'preparing to send a message ...')
        while not self.is_closed():
            s = Stock('FPT')
            s.consensus_day.evaluate_ichimoku()
            s_score = s.consensus_day.score()
            buy_agreement = s_score['buy_agreement'].iloc[-1]
            buy_disagreement = s_score['buy_disagreement'].iloc[-1]

            sell_agreement = s_score['sell_agreement'].iloc[-1]
            sell_disagreement = s_score['sell_disagreement'].iloc[-1]
            message = f'#buy_agreement: ' + str(buy_agreement)
            counter += 1
            await channel.send(str(counter) + message)
            await asyncio.sleep(10)  # task runs every 60 seconds


client = MyClient()
client.run(TOKEN)
