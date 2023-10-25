import asyncio
import logging
from conf.db import DATABASE_URL
from conf.base import BASE_RPC_LIST
from db.client import Database
from src.scanner import Scanner
from src.utils import scan_blockchain, update_twitter_info

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
httpx_logger = logging.getLogger('httpx')
httpx_logger.setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


async def periodic_blockchain_scan(db: Database, scanner: Scanner, interval: int, semaphore: asyncio.Semaphore, batch_size: int):
    """Periodically scan the blockchain for new data."""
    while True:
        try:
            logger.info("Starting blockchain scan...")
            await scan_blockchain(db, scanner, semaphore, batch_size)
            logger.info("Blockchain scan completed")
        except Exception as e:
            logger.exception(f"An error occurred during blockchain scannings: {e}")
        await asyncio.sleep(interval)


async def periodic_twitter_info_update(db: Database, interval: int):
    """Periodically update Twitter information."""
    while True:
        try:
            logger.info("Starting Twitter info update..")
            await update_twitter_info(db)
            logger.info("Twitter info update completed.")
        except Exception as e:
            logger.exception(f"An error occurred during twitter info update: {e}")
        await asyncio.sleep(interval)


async def main():
    try:
        db = Database(DATABASE_URL)
        await db.connect()

        scanner = Scanner(BASE_RPC_LIST)
        semaphore = asyncio.Semaphore(5)
        batch_size = 50

        # Start periodic tasks
        block_interval, twitter_interval = 5, 5
        blockchain_task = asyncio.create_task(
            periodic_blockchain_scan(db, scanner, block_interval, semaphore, batch_size)
        )
        twitter_task = asyncio.create_task(
            periodic_twitter_info_update(db, twitter_interval)
        )

        await asyncio.gather(blockchain_task, twitter_task)

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())