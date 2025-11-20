import os
from dotenv import load_dotenv
from supabase import create_client, Client
import json

load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_API_KEY")

if not url or not key:
    print("Supabase URL and API Key must be set in the .env file.")
    exit()

supabase: Client = create_client(url, key)

try:
    # Fetch one row where trades is not null
    response = supabase.table("snapshots").select("trades").neq("trades", "null").limit(1).execute()
    if response.data:
        trades_data = response.data[0]['trades']
        print("Trades Data Type:", type(trades_data))
        print("Trades Data Sample (First item):")
        if isinstance(trades_data, list) and len(trades_data) > 0:
             print(json.dumps(trades_data[0], indent=2))
        else:
             print(json.dumps(trades_data, indent=2)[:1000])
    else:
        print("No trades data found.")
except Exception as e:
    print(f"Error fetching trades data: {e}")
