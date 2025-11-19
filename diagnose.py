import functions
import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_API_KEY")
supabase = create_client(url, key)

r = functions.connect_to_redis()
active_contracts = functions.get_active_contracts(r)
print(f"Active Contracts in Redis ({len(active_contracts)}): {active_contracts}")

# Fetch one row to see columns
response = supabase.table("signals").select("*").limit(1).execute()
if response.data:
    print(f"Columns in 'signals' table: {list(response.data[0].keys())}")
else:
    print("No data in 'signals' table.")

# Check specifically for contracts 14-18
missing_contracts = ['PH25111914', 'PH25111915', 'PH25111916', 'PH25111917', 'PH25111918']
response = supabase.table("signals").select("contract").in_("contract", missing_contracts).limit(100).execute()
found_missing = set(row['contract'] for row in response.data)
print(f"Found data for missing contracts: {found_missing}")
print(f"Actually missing from Supabase: {set(missing_contracts) - found_missing}")
