import asyncpg
from typing import Optional, AsyncGenerator
from asyncpg import Connection, Pool
from contextlib import asynccontextmanager


class Database:
    """PostgreSQL Database initializer using asyncpg"""

    def __init__(self, database_url: str) -> None:
        self._conn_str = database_url
        self.pool: Optional[Pool] = None
    
    async def connect(self) -> None:
        """Establish a connection to the database"""
        if self.pool is None:
            try:
                self.pool = await asyncpg.create_pool(dsn=self._conn_str)
                print("Database connection established")
            except Exception as e:
                print(f"Unable to connect to the database: {e}")
                raise e
    
    async def close(self) -> None:
        """Close the connections in the pool"""
        if self.pool:
            await self.pool.close()
            print("Database connection closed")
    
    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[Connection, None]:
        """Async context manager to get a database connection from the pool"""
        assert self.pool is not None, "Connection pool is not established"
        connection: Connection = await self.pool.acquire()
        try:
            yield connection
        finally:
            await self.pool.release(connection)

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[Connection, None]:
        """Provide a transaction context for atomic operations."""
        # async with db.transaction() as transaction_conn:
        #     await transaction_conn.execute(query1)
        #     await transaction_conn.execute(query2)
        async with self.get_connection() as conn:
            tx = conn.transaction()
            await tx.start()
            try:
                yield conn
            except Exception as e:
                await tx.rollback()
                print(f"Transaction failed and rolled back: {e}")
                raise
            else:
                await tx.commit()

    async def _execute(self, operation: str, query: str, *args, **kwargs):
        """Private method to execute database operations."""
        async with self.get_connection() as conn:
            try:
                operation_func = getattr(conn, operation)
                return await operation_func(query, *args, **kwargs)
            except Exception as e:
                print(f"Error during database operation ({operation}): {e}")
                raise
    
    async def fetch_all(self, query: str, *args):
        """Fetch multiple rows from the database."""
        return await self._execute('fetch', query, *args)

    async def fetch_row(self, query: str, *args):
        """Fetch a single row from the database."""
        return await self._execute('fetchrow', query, *args)

    async def fetch_val(self, query: str, *args):
        """Fetch a single value from the database."""
        return await self._execute('fetchval', query, *args)
    
    async def execute_query(self, query: str, *args):
        """Execute a single query against the database."""
        return await self._execute('execute', query, *args)

    async def execute_many(self, query: str, records):
        """Execute a statement for multiple records."""
        return await self._execute('executemany', query, records)