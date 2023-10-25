import json
import asyncio
import backoff
import logging
from time import time
from collections import defaultdict
from pydantic import ValidationError
from web3 import AsyncWeb3
from web3.middleware import async_geth_poa_middleware
from eth_utils import to_hex
from db.models import Trade
from conf.base import CONTRACT_ADDRESS, EVENT_SIGNUTARE

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Scanner:
    def __init__(self, rpcs:list[str], contract_address: str = CONTRACT_ADDRESS, event_signature_hash: str = EVENT_SIGNUTARE, sem: int = 5) -> None:
        self.contract_address = contract_address
        self.event_signature_hash = event_signature_hash
        self.contract = None
        self.semaphore = asyncio.Semaphore(sem) 
        self.rpcs = rpcs
        self.backoff_times = defaultdict(list)
        self.requests_counter = {}
        self.last_selected = {rpc: 0 for rpc in rpcs}
        with open("abi/friendTechAbi.json", 'r') as file:
            self.contract_abi = json.load(file)

    async def _get_w3(self) -> tuple[AsyncWeb3, str]:
        current_time = time()

        def recent_backoffs(rpc):
            return sum(1 for timestamp in self.backoff_times[rpc] if current_time - timestamp < 600)
    
        # Calculate the backoff ratio for each rpc
        def backoff_ratio(rpc):
            return recent_backoffs(rpc) / (self.requests_counter.get(rpc, 0) + 0.000001)

        sorted_rpcs = sorted(
            self.rpcs,
            key=lambda rpc: (
                self.last_selected[rpc] - current_time, 
                backoff_ratio(rpc), 
                self.requests_counter.get(rpc, 0)
            )
        )

        # Pick the first RPC from the sorted list
        rpc = sorted_rpcs[0]
        self.requests_counter[rpc] = self.requests_counter.get(rpc, 0) + 1

        # Update the last selected time
        self.last_selected[rpc] = current_time

        web3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(rpc))
        web3.middleware_onion.inject(async_geth_poa_middleware, layer=0)
        return web3, rpc

    
    @backoff.on_exception(backoff.constant, Exception, interval=1, max_tries=1000000)
    async def _filter_transactions(self, block_number: int):
        web3, rpc = await self._get_w3()
        try:
            block = await web3.eth.get_block(block_number, full_transactions=True)
            timestamp = block['timestamp']
            relevant_transactions = []
            for tx in block['transactions']:
                if tx['to'] and tx['to'].lower() == self.contract_address.lower():
                    relevant_transactions.append(tx)
            return relevant_transactions, timestamp
        except Exception as e:
            logger.error(f"Failed to filter transactions for block {block_number}: {e}")
            self.backoff_times[rpc].append(time())
            raise e

    @backoff.on_exception(backoff.constant, Exception, interval=1, max_tries=1000000)
    async def _decode_trade_events(self, transaction, timestamp):
        web3, rpc = await self._get_w3()
        try:
            trades = []
            tx_receipt = await web3.eth.get_transaction_receipt(transaction['hash'])
            contract = web3.eth.contract(address=self.contract_address, abi=self.contract_abi)
            if tx_receipt.status == 1:
                for log in tx_receipt['logs']:
                    if to_hex(log['topics'][0]) == self.event_signature_hash:
                        decoded_event = contract.events.Trade().process_log(log)
                        trade_data = {
                            'trader': decoded_event['args']['trader'],
                            'subject': decoded_event['args']['subject'],
                            'is_buy': decoded_event['args']['isBuy'],
                            'share_amount': decoded_event['args']['shareAmount'],
                            'eth_amount': decoded_event['args']['ethAmount'],
                            'protocol_eth_amount': decoded_event['args']['protocolEthAmount'],
                            'subject_eth_amount': decoded_event['args']['subjectEthAmount'],
                            'supply': decoded_event['args']['supply'],
                            'transaction_hash': decoded_event['transactionHash'],
                            'block_number': decoded_event['blockNumber'],
                            'timestamp': timestamp
                        }
                        try:
                            # Validate and create a trade model instance
                            trade = Trade.model_validate(trade_data)
                            trades.append(trade)
                        except ValidationError as e:
                            logger.error(f"Data validation error for trade: {e.json()}")
                            raise e
            return trades
        except Exception as e:
            logger.error(f"Failed to decode trade events for transaction {transaction['hash']}: {e}")
            self.backoff_times[rpc].append(time())
            raise e

    async def get_trades(self, block_number: int):
        """Scan a block for transactions, returning the decoded trades"""
        trades = []
        relevant_transactions, timestamp = await self._filter_transactions(block_number)
        async with self.semaphore:
            decode_tasks = [self._decode_trade_events(tx, timestamp) for tx in relevant_transactions]
            results = await asyncio.gather(*decode_tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    logger.error(f"Error occurred during trade decoding: {res}")
                else:
                    trades.extend(res)

        return trades
    
    async def get_last_block_number(self):
        """Get last block number"""
        web3, rpc = await self._get_w3()
        try:
            last_block = await web3.eth.get_block_number()
            return last_block
        except Exception as e:
            logger.error(f"Failed to get last block number: {e}")
            self.backoff_times[rpc].append(time())
            raise e

    async def get_balance(self, address: str):
        """Get wallet current balance"""
        web3, rpc = await self._get_w3()
        try:
            checksum_address = web3.to_checksum_address(address)
            balance = await web3.eth.get_balance(checksum_address)
            return balance
        except Exception as e:
            logger.error(f"Failed to get {address} balance: {e}")
            self.backoff_times[rpc].append(time())
            raise e
