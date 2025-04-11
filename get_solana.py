import asyncpg
import os
import aiohttp
import json
from typing import Dict, Any
from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)

rpc_url = "https://mainnet.helius-rpc.com/?api-key=" + os.getenv("HELIUS_API_KEY")
# flipside = Flipside(api_key=os.getenv("FLIPSIDE_API_KEY"))

async def fetch_account_metadata(account_address: str, db: asyncpg.Connection) -> Dict[str, Any]:
    try:
        db_result = await db.fetchrow(
            """
            SELECT label, tags, img_url, type
            FROM accounts
            WHERE pubkey = $1
            """,
            account_address
        )
        if db_result:
            return {
                'pubkey': account_address,
                'label': db_result['label'],
                'tags': db_result['tags'],
                'type': db_result['type'],
                'img_url': db_result['img_url']
            }
        else:
            url = f'https://pro-api.solscan.io/v2.0/account/metadata?address={account_address}'
            headers = {
                'token': os.getenv('SOLSCAN_API_KEY')
            }
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        raise HTTPException(status_code=resp.status, detail="Failed to fetch account metadata")
                    json_data = await resp.json()
                    data = json_data['data']
                    tags = data.get('account_tags', [])
                    tags_str = ', '.join(tags)
                    await db.execute(
                        """
                        INSERT INTO accounts (pubkey, label, tags, type, img_url)
                        VALUES ($1, $2, $3, $4, $5)
                        """,
                        account_address,
                        data.get('account_label', ''),
                        tags_str,
                        data.get('account_type', ''),
                        data.get('account_icon', '')
                    )
                    return {
                        'pubkey': account_address,
                        'label': data.get('account_label', ''),
                        'tags': tags,
                        'type': data.get('account_type', ''),
                        'img_url': data.get('account_icon', '')
                    }
    except Exception as e:
        logger.error(f"Unexpected error fetching account metadata: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

async def fetch_transaction(tx_signature: str) -> Dict[str, Any]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(rpc_url, json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTransaction",
                "params": [
                    tx_signature,
                    {
                        "encoding": "jsonParsed",
                        "maxSupportedTransactionVersion": 0
                    }
                ]
            }) as resp:
                if resp.status != 200:
                    raise HTTPException(status_code=resp.status, detail="Failed to fetch transaction data")
                json_data = await resp.json()
                with open(f"tx_{tx_signature}.json", "w") as f:
                    json.dump(json_data, f, indent=2)
                return json_data
    except aiohttp.ClientError as e:
        raise HTTPException(status_code=500, detail=f"RPC request failed: {str(e)}")
    
async def fetch_account_flows(
    account_address,
    direction: str = "in",
    sort: str = "asc",
    limit: int = 10,
    page: int = 1,
) -> list[Dict[str, Any]]:
    try:
        url = (
            f"https://pro-api.solscan.io/v2.0/account/transfer"
            f"?address={account_address}"
            f"&flow={direction}"
            f"&page={page}"
            f"&page_size={limit}"
            f"&sort_by=block_time"
            f"&sort_order={sort}"
        )
        headers = {
            'token': os.getenv("SOLSCAN_API_KEY")
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    print('resp.text', await resp.text())
                    raise HTTPException(status_code=resp.status, detail="Failed to fetch transaction data")
                json_data = await resp.json()
                if json_data.get('success') != True:
                    print('json_data', json_data)
                    raise HTTPException(status_code=400, detail="Failed to fetch transaction data")
                return json_data['data']

    except Exception as e:
        logger.error(f"Unexpected error fetching account inflow txs: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    