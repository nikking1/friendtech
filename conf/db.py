import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# get database configurations
DATABASE = {
    'USERNAME': os.getenv('POSTGRES_USER'),
    'PASSWORD': os.getenv('POSTGRES_PASSWORD'),
    'HOST': os.getenv('POSTGRES_HOST'),
    'PORT': os.getenv('POSTGRES_PORT'),
    'NAME': os.getenv('POSTGRES_DB'),
}
DATABASE_URL=f"postgresql://{DATABASE['USERNAME']}:{DATABASE['PASSWORD']}@{DATABASE['HOST']}:{DATABASE['PORT']}/{DATABASE['NAME']}"