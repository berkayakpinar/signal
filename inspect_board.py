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
    # Fetch one row where board is not null
    response = supabase.table("snapshots").select("board").neq("board", "null").limit(1).execute()
    if response.data:
        board_data = response.data[0]['board']
        print("Board Data Type:", type(board_data))
        print("Board Data Sample:")
        print(json.dumps(board_data, indent=2)[:1000]) # Print first 1000 chars
    else:
        print("No board data found.")
except Exception as e:
    print(f"Error fetching board data: {e}")
