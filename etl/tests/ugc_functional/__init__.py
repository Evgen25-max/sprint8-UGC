import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent.parent / '.env')

SERVICE_URL = os.getenv('SERVICE_URL', 'http://127.0.0.1:80')
CH_URL = os.getenv('CH_URL', 'http://127.0.0.1:8123')
CH_USER = os.getenv('CLICKHOUSE_USER', 'ugc')
CH_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
JWT_SECRET_KEY = os.environ['JWT_SECRET_KEY']