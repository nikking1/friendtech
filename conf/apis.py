import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

FRIENDTECH_BACKEND = os.getenv('FRIENDTECH_BACKEND')
TWITTERSCORE_KEY = os.getenv('TWITTERSCORE_KEY')