import os
import pymysql
import hashlib
import logging
from elasticsearch import Elasticsearch
from datetime import datetime
from dotenv import load_dotenv # .env 로드용

# 1. 환경 변수 로드
load_dotenv(dotenv_path="../.env") # 상위 폴더의 .env 찾기

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# 설정값 가져오기
DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}
ES_HOST = os.getenv("ES_HOST")
ES_INDEX = os.getenv("ES_INDEX")

def get_sql_query():
    """외부 SQL 파일을 읽어오는 함수"""
    # 현재 파일(collector.py)의 위치를 기준으로 SQL 파일 경로 찾기
    base_dir = os.path.dirname(os.path.abspath(__file__))
    sql_path = os.path.join(base_dir, "../sql/metadata_query.sql")
    
    with open(sql_path, "r", encoding="utf-8") as f:
        return f.read()

def generate_doc_id(source, db, table):
    unique_key = f"{source}_{db}_{table}"
    return hashlib.md5(unique_key.encode('utf-8')).hexdigest()

def run():
    es = Elasticsearch(ES_HOST)
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            # SQL 파일 읽기
            sql = get_sql_query()
            
            logger.info("📡 Fetching metadata from DB...")
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            # ... (이하 데이터 가공 및 적재 로직은 이전과 동일) ...
            # 코드 길이를 줄이기 위해 중복 로직은 생략했지만, 
            # 이전 답변의 'extract_and_index' 내부 로직을 여기에 넣으면 돼.
            
            logger.info(f"✅ Processing {len(rows)} columns...")
            # (여기에 for loop 로직 붙여넣기)

    except FileNotFoundError:
        logger.error("❌ SQL 파일을 찾을 수 없어! 경로를 확인해.")
    except Exception as e:
        logger.error(f"🔥 Error: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    run()