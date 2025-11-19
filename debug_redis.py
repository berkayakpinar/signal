import functions
import json

try:
    r = functions.connect_to_redis()
    print("Connected to Redis")
    
    # Check keys matching 'board'
    cursor = '0'
    all_keys = []
    while cursor != 0:
        cursor, keys = r.scan(cursor=cursor, match='board', count=100)
        all_keys.extend(keys)
    
    print(f"Keys found matching 'board': {all_keys}")
    
    if 'board' in all_keys:
        key_type = r.type('board')
        print(f"Type of 'board' key: {key_type}")
        
        if key_type == 'ReJSON-RL':
            data = r.json().get('board', '.')
            print(f"Keys in 'board' JSON: {list(data.keys())}")
            print(f"Total contracts in Redis: {len(data.keys())}")
        else:
            print("Key 'board' is not ReJSON-RL")

except Exception as e:
    print(f"Error: {e}")
