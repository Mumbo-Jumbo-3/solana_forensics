# Parse transaction and build Network data
import os
from typing import Any, Dict
import aiohttp
from fastapi import HTTPException
from account_tags import tags

async def build_tx_flows_network(tx_data: Dict[str, Any], rpc_url: str) -> Dict[str, Any]:
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

        ata_to_mint = {}
        ata_to_owner = {}
        
        for balance in meta.get("preTokenBalances", []):
            account_index = balance['accountIndex']
            ata_pubkey = accounts[account_index]
            ata_to_mint[ata_pubkey] = balance['mint']
            ata_to_owner[ata_pubkey] = balance['owner']
        
        for balance in meta.get("postTokenBalances", []):
            account_index = balance['accountIndex']
            ata_pubkey = accounts[account_index]
            if ata_pubkey not in ata_to_mint:
                ata_to_mint[ata_pubkey] = balance['mint']
                ata_to_owner[ata_pubkey] = balance['owner']

        print(ata_to_mint)
        print(ata_to_owner)

        current_program_id = None

        # PROCESS INSTRUCTIONS
        for ix in transaction["message"]["instructions"]:
            if "parsed" in ix:
                if ix["parsed"].get("type") == "transfer" and ix["programId"] == "11111111111111111111111111111111":
                    info = ix["parsed"]["info"]
                    
                    source_node = {
                        "pubkey": info["source"],
                        "tag": tags.get(current_program_id, None)
                    }
                    if source_node not in nodes:
                        nodes.append(source_node)
                    
                    dest_node = {
                        "pubkey": info["destination"],
                        "tag": tags.get(current_program_id, None)
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

                    for inner_ix_group in meta.get("innerInstructions", []):
                        for inner_ix in inner_ix_group["instructions"]:
                            if (
                                "parsed" in inner_ix and 
                                inner_ix["parsed"].get("type") == "initializeAccount3" and
                                inner_ix["programId"] == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" and
                                inner_ix["parsed"]["info"]["account"] == info["destination"] and
                                inner_ix["parsed"]["info"]["mint"] == "So11111111111111111111111111111111111111112"
                            ):
                                # tag node where pubkey matches info["destination"] as "Wrap SOL"
                                for node in nodes:
                                    if node["pubkey"] == info["destination"]:
                                        node["tag"] = "Wrap SOL"
                                        break
                                
                                link = {
                                    "source": info["destination"],
                                    "target": info["source"],
                                    "value": float(info["lamports"]),
                                    "type": "transfer",
                                    "mint": "So11111111111111111111111111111111111111112",
                                    "tag": "Wrap SOL"
                                }
                                links.append(link)

                
                elif ix["parsed"].get("type") == "createIdempotent" and ix["programId"] == "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL":
                    info = ix["parsed"]["info"]
                    ata_pubkey = info["account"]
                    ata_to_mint[ata_pubkey] = info["mint"]
                    ata_to_owner[ata_pubkey] = info["wallet"]
        
        # PROCESS INNER INSTRUCTIONS
        current_program_id = None
        if meta["innerInstructions"]:
            for ix_group in meta["innerInstructions"]:
                for ix in ix_group["instructions"]:
                    if "parsed" in ix:
                        if ix["parsed"].get("type") == "initializeAccount3" and ix.get("programId") == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA":
                            info = ix["parsed"]["info"]
                            ata_pubkey = info["account"]
                            if ata_pubkey not in ata_to_mint:
                                ata_to_mint[ata_pubkey] = info["mint"]
                                ata_to_owner[ata_pubkey] = info["owner"]

                        if ix["parsed"].get("type") in ["transfer", "transferChecked"] and ix.get("programId") == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA":
                            info = ix["parsed"]["info"]
                            info["mint"] = ata_to_mint[info["source"]]
                            info["dest_owner"] = ata_to_owner[info["destination"]]

                            source_node = {
                                "pubkey": info["authority"],
                                "tag": tags.get(info["authority"], None)
                            }
                            if not any(node["pubkey"] == source_node["pubkey"] for node in nodes):
                                nodes.append(source_node)
                            
                            dest_node = {
                                "pubkey": info["dest_owner"],
                                "tag": tags.get(info["dest_owner"], None)
                            }
                            if not any(node["pubkey"] == dest_node["pubkey"] for node in nodes):
                                nodes.append(dest_node)
                            
                            link = {
                                "source": info["authority"],
                                "target": info["dest_owner"],
                                "value": float(info["amount"] if "amount" in info else info["tokenAmount"]["amount"]),
                                "type": "transfer",
                                "mint": info["mint"],
                                "program_id": current_program_id,
                                "tag": tags.get(current_program_id, None)
                            }
                            links.append(link)

                    else:
                        current_program_id = ix["programId"]

        
        # GET TICKERS AND DECIMALS FOR WHOLE AMOUNTS
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

        # # GET ACCOUNT TAGS
        # pubkeys_to_check = set(node["pubkey"] for node in nodes) | {
        #     link["program_id"] for link in links if "program_id" in link
        # }
        
        # headers = {"token": os.getenv('SOLSCAN_API_KEY')}
        # for pubkey in pubkeys_to_check:
        #     solscan_metadata_url = "https://pro-api.solscan.io/v2.0/account/metadata"
        #     try:
        #         async with aiohttp.ClientSession() as session:
        #             async with session.get(solscan_metadata_url, headers=headers) as resp:
        #                 if resp.status != 200:
        #                     raise HTTPException(status_code=resp.status, detail="Failed to fetch transaction data")
        #                 data = await resp.json()
        #                 ticker = data['result']['content']['metadata']['symbol']
        #                 decimals = data['result']['token_info']['decimals']
        #                 print(ticker)
        #                 token_tickers[token_address] = ticker
        #                 token_decimals[token_address] = decimals
        #     except Exception as e:
        #         raise HTTPException(status_code=500, detail=f"RPC request failed: {str(e)}")
        
        # GET TOKEN PRICES
        # token_prices = {}
        # headers = {
        #     "accept": "application/json",
        #     "x-chain": "solana",
        #     "X-API-KEY": "9c907c681def4d6bae59328649208009"
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
    
async def build_account_balances_network(
    account_balances: list[Dict[str, Any]],
) -> Dict[str, Any]:
    try:
        nodes = []
        links = []

        for balance in account_balances:
            node = {
                "pubkey": balance["owner"],
                "tag": tags.get(balance["mint"], None)
            }
            if node not in nodes:
                nodes.append(node)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
        
    
async def build_account_inflows_network(
    inflows_data: list[Dict[str, Any]],
    account_address: str,
    rpc_url: str
) -> Dict[str, Any]:
    try:
        nodes = []
        links = []

        dest_node = {
            "pubkey": account_address,
        }
        nodes.append(dest_node)

        for inflow in inflows_data:
            source_node = {
                "pubkey": inflow["tx_from"],
                "tag": tags.get(inflow["tx_from"], None)
            }
            if source_node not in nodes:
                nodes.append(source_node)

            link = {
                "source": source_node["pubkey"],
                "target": account_address,
                "value": inflow["amount"],
                "mint": inflow["mint"]
            }
            links.append(link)

        token_addresses = {
            link["mint"] for link in links if "mint" in link and "mint" not in ["So11111111111111111111111111111111111111112", "So11111111111111111111111111111111111111111"]
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
                        print(data)
                        ticker = data['result']['content']['metadata']['symbol']
                        decimals = data['result']['token_info']['decimals']
                        print(ticker)
                        token_tickers[token_address] = ticker
                        token_decimals[token_address] = decimals
            except Exception as e:
                #raise HTTPException(status_code=500, detail=f"RPC request failed: {str(e)}")
                token_tickers[token_address] = "UNKNOWN"
                token_decimals[token_address] = 0  # default to 9 decimals
        
        
        for link in links:
            if "mint" in link:
                if link["mint"] in ["So11111111111111111111111111111111111111112", "So11111111111111111111111111111111111111111"]:
                    link["ticker"] = "SOL"
                else:
                    link["ticker"] = token_tickers[link["mint"]]

        return {"nodes": nodes, "links": links}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
