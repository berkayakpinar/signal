import redis
import os
from dotenv import load_dotenv

load_dotenv()

def connect_to_redis():
    redis_host = os.getenv("REDIS_HOST")
    redis_port = os.getenv("REDIS_PORT")
    redis_password = os.getenv("REDIS_PASSWORD")
    
    if not redis_host or not redis_port or not redis_password:
        raise ValueError("Redis credentials must be set in .env file")

    # Bağlantı havuzu oluştur
    pool = redis.ConnectionPool(
        host=redis_host, 
        port=int(redis_port), 
        password=redis_password, 
        decode_responses=True,
        max_connections=10  # Maksimum 10 bağlantı
    )

    # Redis nesnesini havuz ile başlat
    r = redis.Redis(connection_pool=pool)
    return r


def get_board_data(r):
    board_data = {}

    # Performans için SCAN kullan
    cursor = '0'
    while cursor != 0:
        cursor, keys = r.scan(cursor=cursor, match='board', count=100)  # 100 anahtar al
    
        for key in keys:
            key_type = r.type(key)  # decode() kaldırıldı
    
            if key_type == 'ReJSON-RL':  # ReJSON veri tipi
                value = r.json().get(key, '.')
                board_data[key] = value
            elif key_type == 'stream':  # Stream veri tipi
                value = r.xrange(key, count=10)  # Son 10 kaydı al
import redis

def connect_to_redis():
    redis_host = "34.89.222.23"
    redis_port = 6379
    redis_password = "RKr3d1s!"

    # Bağlantı havuzu oluştur
    pool = redis.ConnectionPool(
        host=redis_host, 
        port=redis_port, 
        password=redis_password, 
        decode_responses=True,
        max_connections=10  # Maksimum 10 bağlantı
    )

    # Redis nesnesini havuz ile başlat
    r = redis.Redis(connection_pool=pool)
    return r


def get_board_data(r):
    board_data = {}

    # Performans için SCAN kullan
    cursor = '0'
    while cursor != 0:
        cursor, keys = r.scan(cursor=cursor, match='board', count=100)  # 100 anahtar al
    
        for key in keys:
            key_type = r.type(key)  # decode() kaldırıldı
    
            if key_type == 'ReJSON-RL':  # ReJSON veri tipi
                value = r.json().get(key, '.')
                board_data[key] = value
            elif key_type == 'stream':  # Stream veri tipi
                value = r.xrange(key, count=10)  # Son 10 kaydı al
                board_data[key] = value
            else:
                board_data[key] = f"Unsupported type: {key_type}"

    return board_data["board"]

def get_active_contracts(r):
    board = get_board_data(r)
    active_contracts = list(board.keys())
    return active_contracts