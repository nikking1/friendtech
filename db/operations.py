from typing import List, Union
from db.models import Trade, Share
from .client import Database


async def get_last_block(db: Database) -> Union[int, None]:
    """Retrieve the last block number from the trades table"""

    query = """
        SELECT MAX(block_number) FROM trades;
    """
    last_block = await db.fetch_val(query)
    return last_block if last_block is not None else 0


async def insert_trades(db: Database, trades_data: List[Trade]):
    """Insert multiple trades into the database"""

    fields = [field for field in Trade.__fields__]
    placeholders = ', '.join(f'${i+1}' for i in range(len(fields)))
    field_names = ', '.join(fields)
    
    query = f"""
        INSERT INTO trades ({field_names})
        VALUES ({placeholders})
        ON CONFLICT (transaction_hash)
        DO NOTHING;
    """

    query = f"""
        INSERT INTO trades ({field_names})
        VALUES ({placeholders})
    """
    values = [tuple(getattr(trade, field) for field in fields) for trade in trades_data]
    await db.execute_many(query, values)


async def get_all_shares(db: Database) -> List[Share]:
    """Retrieve all Share instances from the database."""

    query = """
        SELECT * FROM shares;
    """
    rows = await db.fetch_all(query)
    return [Share(**row) for row in rows]


async def get_all_share_addresses(db: Database) -> List[bytes]:
    """Retrieve all share addresses from the database."""

    query = """
        SELECT address FROM shares;
    """
    rows = await db.fetch_all(query)
    return {row['address'] for row in rows}

async def get_shares_missing_twitter(db: Database, limit: int) -> List[Share]:
    """Retrieve the top shares without a twitter_username, ordered by balance."""
    
    query = """
        SELECT * FROM shares 
        WHERE twitter_username IS NULL
        ORDER BY balance DESC 
        LIMIT $1;
    """
    rows = await db.fetch_all(query, limit)
    return [Share(**row) for row in rows]

async def update_shares(db: Database, shares: List[Share]):
    """Update multiple shares in the database"""

    if not shares:
        return
    
    update_queries = []
    for share in shares:
        fields_to_update = {field: value for field, value in share.model_dump().items() if value is not None and field != 'address'}
        if not fields_to_update:
            continue
        
        setters = ', '.join(f"{field} = ${i + 2}" for i, field in enumerate(fields_to_update))
        query = f"""
            UPDATE shares
            SET {setters}
            WHERE address = $1;
        """
        values = [share.address] + list(fields_to_update.values())
        update_queries.append((query, values))

    async with db.transaction() as transaction:
        for query, values in update_queries:
            await transaction.execute(query, *values)


async def insert_shares(db: Database, shares: List[Share]):
    """Insert multiple shares into the database"""

    if not shares:
        return

    fields = [field for field in Share.__fields__]
    placeholders = ', '.join(f'${i+1}' for i in range(len(fields)))
    field_names = ', '.join(fields)

    query = f"""
        INSERT INTO shares ({field_names})
        VALUES ({placeholders})
        ON CONFLICT (address)
        DO NOTHING;
    """

    values = [tuple(getattr(share, field) for field in fields) for share in shares]
    await db.execute_many(query, values)
