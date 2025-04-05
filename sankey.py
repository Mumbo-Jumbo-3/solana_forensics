# Parse transaction and build Sankey data
from typing import Any, Dict
import aiohttp
from fastapi import HTTPException

async def build_sankey_data(tx_data: Dict[str, Any], rpc_url: str) -> Dict[str, Any]:
    try:
        nodes = []
        links = []

        result = tx_data["result"]
        meta = result["meta"]
        transaction = result["transaction"]
        accounts = [account["pubkey"] for account in transaction["message"]["accountKeys"]]

        # PROCESS FEES
        total_fee = meta["fee"]
        base_fee = len(transaction['signatures']) * 5000  # Base fee calculation
        priority_fee = total_fee - base_fee

        fee_payer = accounts[0]
        nodes.append({
            "pubkey": fee_payer,
            "tag": "Fee Payer"
        })

        nodes.append({
            "pubkey": "Burn",
            "tag": "Burn"
        })
        links.append({
            "source": fee_payer,
            "target": "Burn",
            "value": base_fee / 2,
            "type": "fee",
            "mint": "So11111111111111111111111111111111111111112",
            "tag": "Base Fee"
        })

        nodes.append({
            "pubkey": "Validator",
            "tag": "Validator"
        })
        links.append({
            "source": fee_payer,
            "target": "Validator",
            "value": base_fee / 2,
            "type": "fee",
            "mint": "So11111111111111111111111111111111111111112",
            "tag": "Base Fee"
        })
        
        if priority_fee > 0:
            links.append({
                "source": fee_payer,
                "target": "Validator",
                "value": priority_fee,
                "type": "fee",
                "mint": "So11111111111111111111111111111111111111112",
                "tag": "Priority Fee"
            })

        # PROCESS INNER INSTRUCTIONS
        ata_to_mint = {}
        ata_to_owner = {}
        for balance in meta["preTokenBalances"]:
            ata_pubkey = accounts[balance['accountIndex']]
            ata_to_mint[ata_pubkey] = balance['mint']
            ata_to_owner[ata_pubkey] = balance['owner']
        
        current_program_id = None
        if meta["innerInstructions"]:
            for ix in meta["innerInstructions"][0]["instructions"]:
                if "parsed" in ix:
                    if ix["parsed"].get("type") == "transfer" and ix.get("programId") == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA":
                        info = ix["parsed"]["info"]
                        info["mint"] = ata_to_mint[info["source"]]
                        info["dest_owner"] = ata_to_owner[info["destination"]]

                        source_node = {
                            "pubkey": info["authority"],
                            "tag": current_program_id
                        }
                        if not any(node["pubkey"] == source_node["pubkey"] for node in nodes):
                            nodes.append(source_node)
                        
                        dest_node = {
                            "pubkey": info["dest_owner"],
                            "tag": current_program_id
                        }
                        if not any(node["pubkey"] == dest_node["pubkey"] for node in nodes):
                            nodes.append(dest_node)
                        
                        link = {
                            "source": info["authority"],
                            "target": info["dest_owner"],
                            "value": float(info["amount"]),
                            "type": "transfer",
                            "mint": info["mint"],
                            "tag": current_program_id
                        }
                        links.append(link)
                else:
                    current_program_id = ix["programId"]

        # PROCESS INSTRUCTIONS
        for ix in transaction["message"]["instructions"]:
            if "parsed" in ix:
                if ix["parsed"].get("type") == "transfer" and ix["programId"] == "11111111111111111111111111111111":
                    info = ix["parsed"]["info"]
                    
                    source_node = {
                        "pubkey": info["source"],
                        "tag": current_program_id
                    }
                    if source_node not in nodes:
                        nodes.append(source_node)
                    
                    dest_node = {
                        "pubkey": info["destination"],
                        "tag": current_program_id
                    }
                    if dest_node not in nodes:
                        nodes.append(dest_node)
                    
                    link = {
                        "source": info["source"],
                        "target": info["destination"],
                        "value": float(info["lamports"]),
                        "type": "transfer",
                        "mint": "So11111111111111111111111111111111111111112",
                    }
                    links.append(link)

        token_addresses = {
            link["mint"] for link in links if "mint" in link and "mint" != "So11111111111111111111111111111111111111112"
        }
        token_tickers = {}
        token_decimals = {}

        for token_address in token_addresses:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(rpc_url, json={
                        "jsonrpc": "2.0",
                        "id": "test",
                        "method": "getAsset",
                        "params": {
                            "id": token_address
                        }
                    }) as resp:
                        if resp.status != 200:
                            raise HTTPException(status_code=resp.status, detail="Failed to fetch transaction data")
                        data = await resp.json()
                        ticker = data['result']['content']['metadata']['symbol']
                        decimals = data['result']['token_info']['decimals']
                        print(ticker)
                        token_tickers[token_address] = ticker
                        token_decimals[token_address] = decimals
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"RPC request failed: {str(e)}")
            
        for link in links:
            if "mint" in link:
                if link["mint"] == "So11111111111111111111111111111111111111112":
                    link["ticker"] = "SOL"
                    link["value"] = float(link["value"]) / 10 ** 9
                else:
                    link["ticker"] = token_tickers[link["mint"]]
                    link["value"] = float(link["value"]) / 10 ** token_decimals[link["mint"]]
        
        # token_prices = {}
        # headers = {
        #     "accept": "application/json",
        #     "x-chain": "solana",
        #     "X-API-KEY": os.getenv("BIRDEYE_API_KEY")
        # }
        # for token_address in token_addresses:
        #     url = f"https://public-api.birdeye.so/defi/historical_price_unix?address={token_address}&unixtime={result['blockTime']}"
        #     async with aiohttp.ClientSession() as session:
        #         async with session.get(url, headers=headers) as resp:
        #             if resp.status != 200:
        #                 raise HTTPException(status_code=resp.status, detail="Failed to fetch token price data")
        #             data = await resp.json()
        #             token_prices[token_address] = data.get('data', {}).get('value', 0)
        
        # # Update links with USD values
        # for link in links:
        #     if link["type"] == "SOL":
        #         # SOL transfers (using wrapped SOL price)
        #         sol_price = token_prices.get('So11111111111111111111111111111111111111112', 0)
        #         link["usd_value"] = link["value"] * sol_price
        #     elif link["type"] == "TOKEN":
        #         # Token transfers
        #         token_price = token_prices.get(link["mint"], 0)
        #         link["usd_value"] = link["value"] * token_price

        return {"nodes": nodes, "links": links}
    except KeyError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid transaction data structure: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )