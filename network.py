# Parse transaction and build Network data
import asyncio
from datetime import datetime
import os
from typing import Any, Dict
import aiohttp
import asyncpg
from fastapi import HTTPException
from account_tags import tags

async def add_accounts_metadata(
    nodes: list[Dict[str, Any]],
    existing_node_pubkeys: list = [],
    db: asyncpg.Connection = None
):
    if not db:
        return nodes
    
    nodes_dict = {node["pubkey"]: node for node in nodes}
    new_pubkeys = {node["pubkey"] for node in nodes
                   if node["pubkey"] not in existing_node_pubkeys
                   and node["pubkey"] not in ['Validator', 'Burn']}
    
    if new_pubkeys:
        try:
            db_results = await db.fetch(
                """
                SELECT pubkey, label, tags, type, img_url
                FROM accounts
                WHERE pubkey = ANY($1)
                """,
                list(new_pubkeys)
            )
            
            db_found_pubkeys = set()
            for result in db_results:
                pubkey = result['pubkey']
                db_found_pubkeys.add(pubkey)
                nodes_dict[pubkey].update({
                    "label": result['label'],
                    "tags": result['tags'].split(',') if result['tags'] else [],
                    "type": result['type'],
                    "img_url": result['img_url']
                })

            missing_pubkeys = new_pubkeys - db_found_pubkeys
            print('missing_pubkeys', missing_pubkeys)
            if missing_pubkeys:
                async with aiohttp.ClientSession() as session:
                    tasks = []
                    headers = {'token': os.getenv('SOLSCAN_API_KEY')}
                    for pubkey in missing_pubkeys:
                        url = f'https://pro-api.solscan.io/v2.0/account/metadata?address={pubkey}'
                        print('account metadata url', url)
                        tasks.append(asyncio.create_task(session.get(url, headers=headers)))
                    
                    responses = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    insert_values = []
                    for pubkey, resp in zip(missing_pubkeys, responses):
                        try:
                            if isinstance(resp, Exception):
                                raise resp
                                
                            if resp.status == 200:
                                data = (await resp.json())['data']
                                account_data = {
                                    "label": data.get('account_label', ''),
                                    "tags": data.get('account_tags', []),
                                    "type": data.get('account_type', ''),
                                    "img_url": data.get('account_icon', '')
                                }
                                
                                nodes_dict[pubkey].update(account_data)
                                
                                insert_values.append((
                                    pubkey,
                                    account_data["label"],
                                    ','.join(account_data["tags"]),
                                    account_data["type"],
                                    account_data["img_url"]
                                ))
                        except Exception as e:
                            print(f"Error fetching metadata for {pubkey}: {e}")
                            nodes_dict[pubkey].update({
                                "label": "",
                                "tags": [],
                                "type": "",
                                "img_url": ""
                            })

                    if insert_values:
                        await db.executemany(
                            """
                            INSERT INTO accounts (pubkey, label, tags, type, img_url)
                            VALUES ($1, $2, $3, $4, $5)
                            ON CONFLICT (pubkey) DO NOTHING
                            """,
                            insert_values
                        )

        except Exception as e:
            print(f"Error in metadata processing: {e}")
            for pubkey in new_pubkeys:
                nodes_dict[pubkey].update({
                    "label": "",
                    "tags": [],
                    "type": "",
                    "img_url": ""
                })

    return list(nodes_dict.values())

async def build_tx_flows_network(
    tx_data: Dict[str, Any],
    rpc_url: str,
    db: asyncpg.Connection = None,
    existing_node_pubkeys: list = [],
    existing_edge_ids: list = []
) -> Dict[str, Any]:
    try:
        nodes = []
        edges = []

        def add_edge_if_new(edge):
            edge_id = f"{edge['txId']}-{edge['source']}-{edge['target']}-{edge['mint']}-{edge['amount']}"
            if edge_id not in existing_edge_ids:
                edges.append(edge)

        result = tx_data["result"]
        meta = result["meta"]
        transaction = result["transaction"]
        accounts = [account["pubkey"] for account in transaction["message"]["accountKeys"]]

        token_days = set()

        # PROCESS FEES
        total_fee = meta["fee"]
        base_fee = len(transaction['signatures']) * 5000  # Base fee calculation
        priority_fee = total_fee - base_fee

        fee_payer = accounts[0]
        nodes.append({
            "pubkey": fee_payer,
            "label": "Fee Payer",
        })

        nodes.append({
            "pubkey": "Burn",
            "label": "Burn"
        })
        edges.append({
            "source": fee_payer,
            "target": "Burn",
            "amount": base_fee / 2,
            "type": "fee",
            "mint": "So11111111111111111111111111111111111111112",
            "label": "Base Fee"
        })

        nodes.append({
            "pubkey": "Validator",
            "label": "Validator",
        })
        edges.append({
            "source": fee_payer,
            "target": "Validator",
            "amount": base_fee / 2,
            "type": "fee",
            "mint": "So11111111111111111111111111111111111111112",
            "label": "Base Fee"
        })
        
        if priority_fee > 0:
            edges.append({
                "source": fee_payer,
                "target": "Validator",
                "amount": priority_fee,
                "type": "fee",
                "mint": "So11111111111111111111111111111111111111112",
                "label": "Priority Fee"
            })

        token_days.add(("So11111111111111111111111111111111111111112", datetime.fromtimestamp(result['blockTime']).strftime('%Y%m%d')))

        ata_to_mint = {}
        ata_to_owner = {}
        
        for balance in meta.get("preTokenBalances", []):
            account_index = balance['accountIndex']
            ata_pubkey = accounts[account_index]
            ata_to_mint[ata_pubkey] = balance['mint']
            ata_to_owner[ata_pubkey] = balance['owner']
            token_days.add((balance['mint'], datetime.fromtimestamp(result['blockTime']).strftime('%Y%m%d')))
        
        for balance in meta.get("postTokenBalances", []):
            account_index = balance['accountIndex']
            ata_pubkey = accounts[account_index]
            if ata_pubkey not in ata_to_mint:
                ata_to_mint[ata_pubkey] = balance['mint']
                ata_to_owner[ata_pubkey] = balance['owner']

        print(ata_to_mint)
        print(ata_to_owner)

        prices_map = await get_prices(token_days, db)
        print(prices_map)

        current_program_id = None

        # PROCESS INSTRUCTIONS
        for ix in transaction["message"]["instructions"]:
            if "parsed" in ix:
                if ix["parsed"].get("type") == "transfer" and ix["programId"] == "11111111111111111111111111111111":
                    info = ix["parsed"]["info"]
                    
                    source_node = {
                        "pubkey": info["source"]
                    }
                    if source_node not in nodes:
                        nodes.append(source_node)
                    
                    dest_node = {
                        "pubkey": info["destination"]
                    }
                    if dest_node not in nodes:
                        nodes.append(dest_node)
                    
                    edge = {
                        "source": info["source"],
                        "target": info["destination"],
                        "amount": float(info["lamports"]),
                        "type": "transfer",
                        "mint": "So11111111111111111111111111111111111111112",
                        "txId": transaction["signatures"][0]
                    }
                    add_edge_if_new(edge)

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
                                        node["label"] = "Wrap SOL"
                                        break
                                        
                                edge = {
                                    "source": info["destination"],
                                    "target": info["source"],
                                    "amount": info["lamports"],
                                    "value": info["lamports"],
                                    "type": "transfer",
                                    "mint": "So11111111111111111111111111111111111111112",
                                    "tag": "Wrap SOL",
                                    "txId": transaction["signatures"][0]
                                }
                                add_edge_if_new(edge)

                
                elif ix["parsed"].get("type") == "createIdempotent" and ix["programId"] == "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL":
                    info = ix["parsed"]["info"]
                    ata_pubkey = info["account"]
                    ata_to_mint[ata_pubkey] = info["mint"]
                    ata_to_owner[ata_pubkey] = info["wallet"]

                elif ix["parsed"].get("type") == "createAccount" and ix["parsed"].get("info").get("owner") == "Stake11111111111111111111111111111111111111":
                    print(ix)
                    info = ix["parsed"]["info"]
                    lamports = float(info["lamports"])
                    new_account = info["newAccount"]
                    source = info["source"]

                    source_node = {
                        "pubkey": source
                    }
                    if source_node not in nodes:
                        nodes.append(source_node)

                    stake_account_node = {
                        "pubkey": new_account
                    }
                    if stake_account_node not in nodes:
                        nodes.append(stake_account_node)

                    edge = {
                        "source": source,
                        "target": new_account,
                        "amount": lamports,
                        "type": "stake",
                        "mint": "So11111111111111111111111111111111111111112",
                        "txId": transaction["signatures"][0]
                    }
                    add_edge_if_new(edge)
                
                elif ix["parsed"].get("type") == "delegate" and ix.get("programId") == "Stake11111111111111111111111111111111111111":
                    info = ix["parsed"]["info"]
                    stake_account = info["stakeAccount"]
                    vote_account = info["voteAccount"]
                    
                    stake_account_node = {
                        "pubkey": stake_account
                    }
                    if stake_account_node not in nodes:
                        nodes.append(stake_account_node)
                    
                    vote_account_node = {
                        "pubkey": vote_account
                    }
                    if vote_account_node not in nodes:
                        nodes.append(vote_account_node)
                        
                    edge = {
                        "source": stake_account,
                        "target": vote_account,
                        "amount": 1,
                        "type": "delegate",
                        "mint": "So11111111111111111111111111111111111111112",
                        "txId": transaction["signatures"][0]
                    }
                    add_edge_if_new(edge)
            
        
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
                                "pubkey": info["authority"]
                            }
                            if not any(node["pubkey"] == source_node["pubkey"] for node in nodes):
                                nodes.append(source_node)
                            
                            dest_node = {
                                "pubkey": info["dest_owner"]
                            }
                            if not any(node["pubkey"] == dest_node["pubkey"] for node in nodes):
                                nodes.append(dest_node)
                            
                            edge = {
                                "source": info["authority"],
                                "target": info["dest_owner"],
                                "amount": float(info["amount"] if "amount" in info else info["tokenAmount"]["amount"]),
                                "type": "transfer",
                                "mint": info["mint"],
                                "programId": current_program_id,
                                "txId": transaction["signatures"][0]
                            }
                            add_edge_if_new(edge)

                    else:
                        current_program_id = ix["programId"]

        
        # ADD TRANSFER METADATA
        token_addresses = {
            edge["mint"] for edge in edges if "mint" in edge and "mint" not in ["So11111111111111111111111111111111111111112", "So11111111111111111111111111111111111111111"]
        }
        token_tickers = {}
        token_decimals = {}
        token_img_urls = {}

        for token_address in token_addresses:
            try:
                db_result = await db.fetchrow(
                    """
                    SELECT ticker, decimals, img_url
                    FROM tokens
                    WHERE mint = $1
                    """,
                    token_address
                )
                if db_result:
                    token_tickers[token_address] = db_result['ticker']
                    token_decimals[token_address] = db_result['decimals']
                    token_img_urls[token_address] = db_result['img_url']
                else:
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
                            img_url = data['result']['content']['links']['image']

                            await db.execute(
                                """
                                INSERT INTO tokens (mint, ticker, decimals, img_url)
                                VALUES ($1, $2, $3, $4)
                                """,
                                token_address, ticker, decimals, img_url
                            )

                            token_tickers[token_address] = ticker
                            token_decimals[token_address] = decimals
                            token_img_urls[token_address] = img_url
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"RPC request failed: {str(e)}")
        
        sol_price = prices_map[("So11111111111111111111111111111111111111112", datetime.fromtimestamp(result['blockTime']).strftime('%Y%m%d'))]
        for edge in edges:
            if "mint" in edge and edge["type"] != "delegate":
                if edge["mint"] in ["So11111111111111111111111111111111111111112", "So11111111111111111111111111111111111111111"]:
                    sol_amount = float(edge["amount"]) / 10 ** 9
                    edge["ticker"] = "SOL"
                    edge["tokenImage"] = "https://assets.coingecko.com/coins/images/4128/standard/solana.png?1718769756"
                    edge["amount"] = sol_amount
                    edge["value"] = sol_amount * sol_price
                else:
                    edge["ticker"] = token_tickers[edge["mint"]]
                    edge["tokenImage"] = token_img_urls[edge["mint"]]
                    edge["amount"] = float(edge["amount"]) / 10 ** token_decimals[edge["mint"]]

        # ADD ACCOUNT METADATA
        nodes = await add_accounts_metadata(nodes, existing_node_pubkeys, db)
        print(nodes)
        print(edges)
        
        return {"nodes": nodes, "edges": edges}
    
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
    
async def build_account_flows_network(
    flows_data: list[Dict[str, Any]],
    rpc_url: str,
    db: asyncpg.Connection = None,
    limit: int = 10,
    existing_node_pubkeys: list = [],
    existing_edge_ids: list = []
) -> Dict[str, Any]:
    try:
        nodes = []
        edges = []

        def add_edge_if_new(edge):
            edge_id = f"{edge['txId']}-{edge['source']}-{edge['target']}-{edge['mint']}-{edge['amount']}"
            if edge_id not in existing_edge_ids:
                edges.append(edge)

        token_days = [(flow['token_address'], datetime.fromtimestamp(flow['block_time']).strftime('%Y%m%d')) for flow in flows_data]
        unique_token_days = list(set(token_days))
        prices_map = await get_prices(unique_token_days, db)

        for flow in flows_data:
            if not flow["from_address"] or not flow["to_address"]:
                print(flow)
                continue
            
            source_node = {
                "pubkey": flow["from_address"],
            }
            if not any(node["pubkey"] == source_node["pubkey"] for node in nodes):
                nodes.append(source_node)
            
            dest_node = {
                "pubkey": flow["to_address"]
            }
            if not any(node["pubkey"] == dest_node["pubkey"] for node in nodes):
                nodes.append(dest_node)

            whole_amount = flow['amount'] / 10 ** flow['token_decimals']
            date_str = datetime.fromtimestamp(flow['block_time']).strftime('%Y%m%d')
            price = prices_map[(flow['token_address'], date_str)]
                
            edge = {
                'source': flow['from_address'],
                'target': flow['to_address'],
                'amount': whole_amount,
                'value': price * whole_amount if price else None,
                'mint': flow['token_address'],
                'txId': flow['trans_id'],
                'blockTime': flow['block_time'],
                'type': flow['activity_type']
            }
            add_edge_if_new(edge)

        # ADD TRANSFER METADATA
        token_addresses = {
            edge["mint"] for edge in edges if "mint" in edge and "mint" not in ["So11111111111111111111111111111111111111112", "So11111111111111111111111111111111111111111"]
        }
        token_tickers = {}
        token_img_urls = {}

        for token_address in token_addresses:
            try:
                db_result = await db.fetchrow(
                    """
                    SELECT ticker, decimals, img_url
                    FROM tokens
                    WHERE mint = $1
                    """,
                    token_address
                )
                if db_result:
                    token_tickers[token_address] = db_result['ticker']
                    token_img_urls[token_address] = db_result['img_url']
                else:
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
                            img_url = data['result']['content']['links']['image']

                            await db.execute(
                                """
                                INSERT INTO tokens (mint, ticker, decimals, img_url)
                                VALUES ($1, $2, $3, $4)
                                """,
                                token_address, ticker, decimals, img_url
                            )

                            token_tickers[token_address] = ticker
                            token_img_urls[token_address] = img_url
                            
            except Exception as e:
                #raise HTTPException(status_code=500, detail=f"RPC request failed: {str(e)}")
                token_tickers[token_address] = ""
                token_img_urls[token_address] = ""
        
        for edge in edges:
            if "mint" in edge:
                if edge["mint"] in ["So11111111111111111111111111111111111111112", "So11111111111111111111111111111111111111111"]:
                    edge["ticker"] = "SOL"
                    edge['tokenImage'] = "https://assets.coingecko.com/coins/images/4128/standard/solana.png?1718769756"
                else:
                    edge["ticker"] = token_tickers[edge["mint"]]
                    edge['tokenImage'] = token_img_urls[edge["mint"]]

        # ADD ACCOUNT METADATA
        nodes = await add_accounts_metadata(nodes, existing_node_pubkeys, db)
        
        if limit == len(flows_data):
            return {"nodes": nodes, "edges": edges, "hasMore": True}
        elif limit > len(flows_data):
            return {"nodes": nodes, "edges": edges, "hasMore": False}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

async def get_prices(token_days, db: asyncpg.Connection = None):
    prices_map = {}
    if not token_days:
        return prices_map
    
    try:
        tokens = [pair[0] for pair in token_days]
        days = [pair[1] for pair in token_days]

        db_results = await db.fetch(
            """
            SELECT mint, day, price
            FROM prices_daily
            WHERE (mint, day) IN (
                SELECT * FROM unnest($1::text[], $2::text[])
            )
            """,
            tokens, days
        )

        for result in db_results:
            prices_map[(result['mint'], result['day'])] = float(result['price']) if result['price'] else None

        missing_pairs = [pair for pair in token_days if pair not in prices_map]
            
        if missing_pairs:
            token_to_days = {}
            for token, day in missing_pairs:
                if token not in token_to_days:
                    token_to_days[token] = []
                token_to_days[token].append(day)
            
            headers = {'token': os.getenv('SOLSCAN_API_KEY')}
            async with aiohttp.ClientSession() as session:
                tasks = []
                
                for token, days in token_to_days.items():
                    days.sort()
                    from_time = min(days)
                    to_time = max(days)
                    
                    url = f"https://pro-api.solscan.io/v2.0/token/price?address={token}&from_time={from_time}&to_time={to_time}"
                    tasks.append(asyncio.create_task(session.get(url, headers=headers)))
                
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                
                insert_values = []
                for (token, _), resp in zip(token_to_days.items(), responses):
                    if isinstance(resp, Exception):
                        print(f"Error fetching price for {token}: {resp}")
                        for day in token_to_days[token]:
                            prices_map[(token, day)] = None
                            insert_values.append((token, day, None))
                        continue
                        
                    if resp.status == 200:
                        json_data = await resp.json()
                        days_with_prices = set()

                        if json_data.get('data'):
                            for price_data in json_data['data']:
                                day = price_data.get('date')
                                price = price_data.get('price')
                                
                                if day:
                                    days_with_prices.add(day)
                                    prices_map[(token, day)] = float(price)
                                    insert_values.append((token, str(day), float(price)))
                        
                        for day in token_to_days[token]:
                            if day not in days_with_prices:
                                prices_map[(token, day)] = None
                                insert_values.append((token, str(day), None))
                
                # Bulk insert new prices
                if insert_values:
                    await db.executemany(
                        """
                        INSERT INTO prices_daily (mint, day, price)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (mint, day) DO NOTHING
                        """,
                        insert_values
                    )
        
        return prices_map
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")