from flipside import Flipside
import os
import aiohttp
import json
from typing import Dict, Any
from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)

rpc_url = "https://mainnet.helius-rpc.com/?api-key=" + os.getenv("HELIUS_API_KEY")
flipside = Flipside(api_key=os.getenv("FLIPSIDE_API_KEY"))

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
                # with open(f"tx_{tx_signature}.json", "w") as f:
                #     json.dump(json_data, f, indent=2)
                return json_data
    except aiohttp.ClientError as e:
        raise HTTPException(status_code=500, detail=f"RPC request failed: {str(e)}")
    
async def fetch_account_inflows(
    account_address
) -> list[Dict[str, Any]]:
    # if mint in ['So11111111111111111111111111111111111111111', 'So11111111111111111111111111111111111111112']:
    #     mint = ['So11111111111111111111111111111111111111111', 'So11111111111111111111111111111111111111112']
    # try:
    #     query = f"""
    #         select
    #             block_timestamp,
    #             tx_id,
    #             tx_from,
    #             tx_to,
    #             amount
    #         from solana.core.fact_transfers
    #         where
    #             tx_to = '{account_address}'
    #             and mint{" = " + mint if type(mint) == str else " in (" + ", ".join(mint) + ")"}
    #         limit 10
    #     """
    #     result = flipside.query(query)
    #     records = result['records']
    #     print(records)
    #     return records
    try:
        query = f"""
            select
                block_timestamp,
                tx_id,
                tx_from,
                tx_to,
                mint,
                amount
            from solana.core.fact_transfers
            where
                tx_to = '{account_address}'
                and amount > 0.000000001
            order by block_timestamp asc
            limit 10
        """
        result = flipside.query(query)
        records = result.records
        print(records)
        return records
    except Exception as e:
        logger.error(f"Unexpected error fetching account balance inflow txs: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    
async def fetch_account_balances(account_address: str) -> Dict[str, Any]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(rpc_url, json={
                "jsonrpc": "2.0",
                "id": "text",
                "method": "getAssetsByOwner",
                "params": {
                    "ownerAddress": account_address,
                    "page": 1,
                    "limit": 100,
                    "sortBy": {
                        "sortBy": "recent_action",
                        "sortOrder": "desc"
                    },
                    "options": {
                        "showFungible": True,
                        "showNativeBalance": True,
                    }
                }
            }) as resp:
                if resp.status != 200:
                    raise HTTPException(status_code=resp.status, detail="Failed to fetch transaction data")
                json_data = await resp.json()
                return json_data
            
    except aiohttp.ClientError as e:
        raise HTTPException(status_code=500, detail=f"RPC request failed: {str(e)}")
    