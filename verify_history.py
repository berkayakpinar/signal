import os
from dotenv import load_dotenv
from supabase import create_client, Client
import pandas as pd
import json

load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_API_KEY")

if not url or not key:
    print("Supabase URL and API Key must be set in the .env file.")
    exit()

supabase: Client = create_client(url, key)

def fetch_history_contracts():
    try:
        print("Fetching contracts...")
        response = supabase.table("snapshots").select("contract, snapshot_minute").order("snapshot_minute", desc=True).limit(5000).execute()
        if response.data:
            df = pd.DataFrame(response.data)
            unique_contracts = df['contract'].unique().tolist()
            return unique_contracts[:30]
    except Exception as e:
        print(f"Error fetching history contracts: {e}")
    return []

def fetch_snapshot_history(contract):
    try:
        print(f"Fetching history for {contract}...")
        response = supabase.table("snapshots").select("snapshot_minute, board").eq("contract", contract).order("snapshot_minute", desc=False).execute()
        if response.data:
            data = []
            for row in response.data:
                snapshot_time = row['snapshot_minute']
                board = row.get('board', {})
                if board:
                    price = board.get('lastPrice')
                    if price is not None:
                        data.append({
                            'snapshot_minute': snapshot_time,
                            'price': price
                        })
            
            df = pd.DataFrame(data)
            return df
    except Exception as e:
        print(f"Error fetching snapshot history for {contract}: {e}")
    return pd.DataFrame()

contracts = fetch_history_contracts()
print(f"Found {len(contracts)} contracts.")
if contracts:
    print(f"Top 5: {contracts[:5]}")
    contract = contracts[0]
    df = fetch_snapshot_history(contract)
    print(f"History for {contract}: {len(df)} rows.")
    if not df.empty:
        print(df.head())
else:
    print("No contracts found.")
