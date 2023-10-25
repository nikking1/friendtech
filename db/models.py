from typing import Optional
from pydantic import BaseModel, validator
from decimal import Decimal
from hexbytes import HexBytes


class Share(BaseModel):
    address: bytes
    twitter_username: Optional[str] = None
    twitter_name: Optional[str] = None
    twitter_score: Optional[float] = None
    registered: Optional[int] = None
    last_transaction: int
    balance: Decimal
    buy_price: Decimal
    sell_price: Decimal
    supply: int
    rank: Optional[int] = None

    class Config:
        validate_assignment = True

    @validator('address', pre=True)
    def validate_bytes(cls, v):
        return convert_to_bytes(v)
        
    @validator('registered', 'last_transaction', 'supply', 'rank', pre=True)
    def validate_int(cls, v):
        if v is not None:
            return convert_to_int(v)
        return v
    
    @validator('balance', 'buy_price', 'sell_price', pre=True)
    def validate_decimal(cls, v):
        return convert_to_decimal(v)
    

class Trade(BaseModel):
    trader: bytes
    subject: bytes
    is_buy: bool
    share_amount: int
    eth_amount: Decimal
    protocol_eth_amount: Decimal
    subject_eth_amount: Decimal
    supply: int
    transaction_hash: bytes
    block_number: int
    timestamp: int

    @validator('trader', 'subject', 'transaction_hash', pre=True)
    def validate_bytes(cls, v):
        return convert_to_bytes(v)

    @validator('share_amount', 'supply', 'block_number', 'timestamp', pre=True)
    def validate_int(cls, v):
        return convert_to_int(v)
    
    @validator('eth_amount', 'protocol_eth_amount', 'subject_eth_amount', pre=True)
    def validate_decimal(cls, v):
        return convert_to_decimal(v)

def convert_to_bytes(value):
    if isinstance(value, str) and value.startswith("0x"):
        return bytes.fromhex(value[2:])
    elif isinstance(value, HexBytes):
        return bytes(value)
    elif isinstance(value, bytes):
        return value
    else:
        raise ValueError(f'Expected a hex string, HexBytes, or bytes, got: {type(value).__name__}')

def convert_to_int(value):
    if isinstance(value, int):
        return value
    elif isinstance(value, str):
        if value.isdigit():
            return int(value)
        elif value.startswith("0x"):
            return int(value, 16)
    elif isinstance(value, float):
        return int(value)
    else:
        raise ValueError(f'Expected a numerical value, got: {type(value).__name__}')

def convert_to_decimal(value):
    if isinstance(value, Decimal):
        return value
    elif isinstance(value, (str, float, int)):
        return Decimal(value)
    return value