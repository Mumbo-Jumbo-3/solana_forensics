import asyncio
from solana.rpc.api import Client
from solders.pubkey import Pubkey
import os

async def test():
    url = "https://pro-api.solscan.io/v2.0/token/price"

asyncio.run(test())