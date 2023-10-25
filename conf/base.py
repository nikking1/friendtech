import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

BASE_MAINNET = os.getenv('BASE_MAINNET')
BASE_RPC_LIST = os.getenv('BASE_RPC_LIST', '').split(',')
CONTRACT_ADDRESS = os.getenv('CONTRACT_ADDRESS')
EVENT_SIGNUTARE = os.getenv('EVENT_SIGNUTARE')