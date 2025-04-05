from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from solana.rpc.api import Client
import aiohttp
import asyncio
import os
from typing import Dict, Any
import logging
import json
from flipside import Flipside

from get_solana import fetch_account_balances, fetch_account_inflows, fetch_transaction
from network import build_account_balances_network, build_account_inflows_network, build_tx_flows_network
from sankey import build_sankey_data

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
load_dotenv()

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Add your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

rpc_url = "https://mainnet.helius-rpc.com/?api-key=" + os.getenv("HELIUS_API_KEY")     

@app.get("/transaction/sankey/{tx_signature}")
async def get_transaction_flow(tx_signature: str):
    try:
        logger.info(f"Fetching transaction data for signature: {tx_signature}")
        tx_data = await fetch_transaction(tx_signature)
        
        logger.info("Building Sankey data from transaction")
        sankey_data = await build_sankey_data(tx_data, rpc_url)
        
        if not sankey_data["links"]:
            logger.warning(f"No valid transfers found in transaction: {tx_signature}")
            raise HTTPException(
                status_code=404,
                detail="No valid transfers found in this transaction"
            )
        
        logger.info(f"Successfully processed transaction with {len(sankey_data['links'])} transfers")
        return sankey_data
    except HTTPException as he:
        logger.error(f"HTTP Exception: {str(he.detail)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing transaction: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/transaction/network/{tx_signature}")
async def get_transaction_flow(tx_signature: str):
    try:
        logger.info(f"Fetching transaction data for signature: {tx_signature}")
        tx_data = await fetch_transaction(tx_signature)
        
        logger.info("Building Sankey data from transaction")
        network_data = await build_tx_flows_network(tx_data, rpc_url)
        
        if not network_data["links"]:
            logger.warning(f"No valid transfers found in transaction: {tx_signature}")
            raise HTTPException(
                status_code=404,
                detail="No valid transfers found in this transaction"
            )
        
        logger.info(f"Successfully processed transaction with {len(network_data['links'])} transfers")
        return network_data
    except HTTPException as he:
        logger.error(f"HTTP Exception: {str(he.detail)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing transaction: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    
@app.get("/account_inflows/{account_address}")
async def get_account_inflows(account_address: str):
    try:
        logger.info(f"Fetching transaction data for signature: {account_address}")
        inflows_data = await fetch_account_inflows(account_address)
        
        logger.info("Building network data from inflows")
        network_data = await build_account_inflows_network(inflows_data, account_address, rpc_url)
        
        if not network_data["links"]:
            logger.warning(f"No valid inflows found for account: {account_address}")
            raise HTTPException(
                status_code=404,
                detail="No valid inflows found for this account"
            )
        
        logger.info(f"Successfully processed inflows for account: {account_address}")
        return network_data
    except HTTPException as he:
        logger.error(f"HTTP Exception: {str(he.detail)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing transaction: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/account_balances/{account_address}")
async def get_account_balances(account_address: str):
    try:
        logger.info(f"Fetching transaction data for signature: {account_address}")
        account_balances = await fetch_account_balances(account_address)
        
        logger.info("Building account balances network")
        network_data = await build_account_balances_network(account_balances)
        
        if not network_data["nodes"]:
            logger.warning(f"No valid balances found for account: {account_address}")
            raise HTTPException(
                status_code=404,
                detail="No valid balances found for this account"
            )
        
        logger.info(f"Successfully processed account balances with {len(network_data['nodes'])} nodes")
        return network_data
    except HTTPException as he:
        logger.error(f"HTTP Exception: {str(he.detail)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing transaction: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")