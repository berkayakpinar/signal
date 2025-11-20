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
    response = supabase.table("snapshots").select("*").limit(1).execute()
    if response.data:
        print("Columns in 'snapshots' table:")
        keys = list(response.data[0].keys())
        keys.sort()
        for k in keys:
            print(f"- {k}")
            
        print("\nSample Data (first 5 keys):")
        # Print a few sample values to guess types
        for k in keys[:5]:
             print(f"{k}: {response.data[0][k]}")
    else:
        print("Table 'snapshots' is empty or does not exist.")
except Exception as e:
    print(f"Error fetching from snapshots: {e}")
