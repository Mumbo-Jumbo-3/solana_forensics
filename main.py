from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from solana.rpc.api import Client
import aiohttp
import asyncio
import os
from typing import Dict, Any
import logging
import json
import asyncpg
import warnings
warnings.filterwarnings("always", category=UserWarning)

from get_solana import fetch_account_flows, fetch_account_metadata, fetch_transaction
from network import build_account_flows_network, build_tx_flows_network
from sankey import build_sankey_data

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
load_dotenv()

rpc_url = "https://mainnet.helius-rpc.com/?api-key=" + os.getenv("HELIUS_API_KEY")     

db_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Database connection pool setup
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(
            os.getenv("DATABASE_URL")
        )
        logger.info("Database connection pool created")
        yield
    finally:
        if db_pool:
            await db_pool.close()
            logger.info("Database connection pool closed")

app = FastAPI(lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://solfor.io"],  # Add your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ExistingNetworkData(BaseModel):
    existingNodes: list[str] = []
    existingEdges: list[str] = []

async def get_db():
    async with db_pool.acquire() as connection:
        yield connection

@app.get("/account/{account_address}")
async def get_account(
    account_address: str,
    db: asyncpg.Connection = Depends(get_db)
):
    try:
        return await fetch_account_metadata(account_address, db=db)
    except HTTPException as he:
        logger.error(f"HTTP Exception: {str(he.detail)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing account: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/transaction_flows/{tx_signature}")
async def get_transaction_flows(
    tx_signature: str,
    existing_network_data: ExistingNetworkData,
    db: asyncpg.Connection = Depends(get_db)
):
    try:
        logger.info(f"Fetching transaction data for signature: {tx_signature}")
        tx_data = await fetch_transaction(tx_signature)
        
        logger.info("Building network data from transaction")
        network_data = await build_tx_flows_network(
            tx_data,
            rpc_url,
            db=db,
            existing_node_pubkeys=existing_network_data.existingNodes,
            existing_edge_ids=existing_network_data.existingEdges
        )
        
        if not network_data["edges"]:
            logger.warning(f"No valid transfers found in transaction: {tx_signature}")
            raise HTTPException(
                status_code=404,
                detail="No valid transfers found in this transaction"
            )
        
        logger.info(f"Successfully processed transaction with {len(network_data['edges'])} transfers")
        return network_data
    except HTTPException as he:
        logger.error(f"HTTP Exception: {str(he.detail)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing transaction: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    
@app.post("/account_flows/{account_address}")
async def get_account_flows(
    account_address: str,
    existing_network_data: ExistingNetworkData,
    direction: str = Query(default="in"),
    sort: str = Query(default="asc"),
    limit: int = Query(default=100),
    page: int = Query(default=1),
    db: asyncpg.Connection = Depends(get_db)
):
    try:
        flows_data = await fetch_account_flows(
            account_address,
            direction=direction,
            sort=sort,
            limit=limit,
            page=page
        )
        
        logger.info("Building network data from flows")
        network_data = await build_account_flows_network(
            flows_data,
            rpc_url=rpc_url,
            db=db,
            existing_node_pubkeys=existing_network_data.existingNodes,
            existing_edge_ids=existing_network_data.existingEdges
        )

        if not network_data["edges"]:
            logger.warning(f"No valid flows found for account: {account_address}")
            raise HTTPException(
                status_code=404,
                detail="No valid flows found for this account"
            )
        
        logger.info(f"Successfully processed inflows for account: {account_address}")
        return network_data
    except HTTPException as he:
        logger.error(f"HTTP Exception: {str(he.detail)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing transaction: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")