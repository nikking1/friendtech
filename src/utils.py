import binascii
import logging
from itertools import chain
import random
import asyncio
from typing import List

from db.client import Database
from db.models import Trade, Share
from db.operations import get_last_block, insert_trades, get_all_share_addresses, get_shares_missing_twitter, update_shares, insert_shares
from src.scanner import Scanner
from src.friendtech import FriendTech
from src.twitterscore import TwitterScore
from src.contract import Contract

logger = logging.getLogger(__name__)


async def scan_blockchain(db: Database, scanner: Scanner, semaphore: asyncio.Semaphore, batch_size: int):
    """Scan the blockchain for new data."""
    start_block = await get_last_block(db)
    last_block = await scanner.get_last_block_number()

    # Check if the block was already scanned
    if start_block >= last_block:
        logger.info("No new blocks to process. Exiting.")
        return
    
    async def bounded_handle_block_range(*args, **kwargs):
        async with semaphore:
            return await handle_block_range(*args, **kwargs)
            
    batch_tasks = []
    for i in range(start_block + 1, last_block + 1, batch_size):
        end_block = min(i + batch_size - 1, last_block)
        percentage = (i - start_block) / (last_block - start_block) * 100
        logger.info(f"Fetching {i - start_block}/{last_block - start_block}...........{percentage:.2f}%")
        task = bounded_handle_block_range(scanner, db, i, end_block)
        batch_tasks.append(task)

    await asyncio.gather(*batch_tasks)


async def handle_block_range(scanner: Scanner, db: Database, start: int, end: int) -> None:
    """Fetch trades for a range of blocks and insert them into the database"""

    logger.info(f"Handling block range from {start} to {end}")
    block_tasks = []
    concurrent_tasks = asyncio.Semaphore(5)

    async def fetch_and_handle_trades(block_num):
        async with concurrent_tasks:
            return await scanner.get_trades(block_num)
    
    block_tasks = [fetch_and_handle_trades(block_num) for block_num in range(start, end)]

    trades = await asyncio.gather(*block_tasks, return_exceptions=True)

    valid_trades = []
    for trade in trades:
        if trade and not isinstance(trade, Exception):
            valid_trades.append(trade)
    flat_trades = list(chain.from_iterable(valid_trades))

    # Insert trades and shares into the database.
    if flat_trades:
        try:
            await asyncio.gather(
                insert_trades(db, flat_trades),
                process_trades_to_shares(db, scanner, flat_trades), 
                return_exceptions=True
            )
            logger.info(f"Successfully inserted {len(flat_trades)} trades from blocks {start} to {end}")
        except Exception as e:
            logger.exception(f"Error inserting trades: {e}")


async def process_trades_to_shares(db: Database, scanner: Scanner, trades: List[Trade]) -> None:
    """Fetch shares for a range of trades and insert them into the database"""

    logger.info(f"Processing {len(trades)} trades to convert to shares.")
    # Retrive Unique and most resent trades for each address
    most_recent_trades = {}
    for trade in trades:
        subject = trade.subject
        if subject in most_recent_trades:
            if trade.timestamp > most_recent_trades[subject].timestamp:
                most_recent_trades[subject] = trade
        else:
            most_recent_trades[subject] = trade
    unique_most_recent_trades = list(most_recent_trades.values())

    logger.info(f"Identified {len(unique_most_recent_trades)} unique recent trades for share processing.")

    contract = Contract()
    existing_shares = await get_all_share_addresses(db)
    shares_to_update, shares_to_create = [], []
    for trade in unique_most_recent_trades:
        address_str = "0x" + binascii.hexlify(trade.subject).decode()
        balance = await scanner.get_balance(address_str)

        share_data = {
            'address': trade.subject,
            'last_transaction': trade.timestamp,
            'balance': balance,
            'buy_price': contract.calc_buy_price_after_fee(trade.supply, 1),
            'sell_price': contract.calc_sell_price_after_fee(trade.supply, 1),
            'supply': trade.supply
        }
        if trade.subject in existing_shares:
            shares_to_update.append(share_data)
        else:
            share_data['registered'] = trade.timestamp
            shares_to_create.append(share_data)

    logger.info(f"Updating {len(shares_to_update)} existing shares, and creating {len(shares_to_create)} new shares...")

    try:
        if shares_to_update:
            valid_shares = [Share(**share) for share in shares_to_update if isinstance(share, dict)]
            await update_shares(db, valid_shares)
            logger.info(f"Updated {len(valid_shares)} shares.")

        if shares_to_create:
            valid_shares = [Share(**share) for share in shares_to_create if isinstance(share, dict)]
            await insert_shares(db, valid_shares)
            logger.info(f"Inserted {len(valid_shares)} new shares.")
    except Exception as e:
        logger.error(f"An error occurred while processing shares: {e}", exc_info=True)


async def update_twitter_info(db: Database, max_attempts: int = 10):
    """Fetch shares twitter information"""
    
    friend_tech = FriendTech()
    twitter_score = TwitterScore()

    shares = await get_shares_missing_twitter(db, 30)
    if not shares:
        logger.info("No shares missing Twitter data at this time.")
        return
    
    updated_shares = []
    for share in shares:
        address_str = "0x" + binascii.hexlify(share.address).decode()
        attempt = 0
        success = False
        while attempt < max_attempts:
            try:
                friendtech_data = await friend_tech.get_info_from_address(address_str)
                if friendtech_data is not None:
                    twitter_data = await twitter_score.get_twitter_score(friendtech_data.get('twitterUsername'))
                    share.twitter_username = friendtech_data.get('twitterUsername')
                    share.twitter_name = friendtech_data.get('twitterName')
                    share.rank = friendtech_data.get('rank')
                    share.twitter_score = twitter_data.get('twitter_score') if twitter_data and twitter_data['success'] else 0
                    updated_shares.append(share)
                    success = True
                    break
                else:
                    attempt += 1
                    logger.warning(f"Attempt {attempt} failed, FriendTech data is None for address {address_str}. Retrying...")
                    await asyncio.sleep(random.uniform(0.5, 1.5))
            except Exception as e:
                attempt += 1
                logger.error(f"Attempt {attempt} failed due to exception: {str(e)}")
                await asyncio.sleep(random.uniform(0.5, 1.5))
        if not success:
            logger.info(f"All attempts failed for address {address_str}, using default data.")
            share.twitter_username = "not_found"
            share.twitter_name = "Not Found"
            share.rank = 0
            share.twitter_score = 0
            updated_shares.append(share)

    if updated_shares:
        await update_shares(db, updated_shares)
        logger.info(f"Successfully updated Twitter information for {len(updated_shares)} shares.")
    else:
        logger.info("No shares were updated with new Twitter information.")