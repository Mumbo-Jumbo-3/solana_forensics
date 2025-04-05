import asyncio
from solana.rpc.api import Client
from solders.pubkey import Pubkey
import os

async def test():
    client = Client(f"https://mainnet.helius-rpc.com/?api-key={os.getenv('HELIUS_API_KEY')}")
    signatures = client.get_signatures_for_address(
        Pubkey.from_string("2RJHVBfeN7Yptbg83VX97UVVeD6UrU9ga11TMwbveQiu"),
        limit=1,
        before=None
    )
    print(signatures)

asyncio.run(test())