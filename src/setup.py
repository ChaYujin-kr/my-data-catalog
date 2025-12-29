import os
import json
import logging
import pymysql
import pymysql.cursors
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv

# 1. 프록시 강제 해제 (필수)
os.environ["NO_PROXY"] = "localhost,127.0.0.1"
if "HTTP_PROXY" in os.environ: del os.environ["HTTP_PROXY"]
if "HTTPS_PROXY" in os.environ: del os.environ["HTTPS_PROXY"]

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# 환경변수 로드
current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, "..", ".env")
load_dotenv(dotenv_path=env_path)

# 설정값
DB_CONFIG = {
    'host': os.getenv("DB_HOST", "localhost"),
    'port': int(os.getenv("DB_PORT", 3306)),
    'user': os.getenv("DB_USER", "root"),
    'password': os.getenv("DB_PASSWORD", "root"),
    'db': 'information_schema', # 메타데이터는 여기서 수집
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "data-catalog")
TARGET_DB = "my_service_db" # 수집할 대상 DB 이름

def collect_and_index():
    # 1. Elasticsearch 연결
    es = Elasticsearch(ES_HOST, request_timeout=30, verify_certs=False)
    if not es.ping(): # ping 대신 info()가 더 정확하지만 일단 ping 시도
        try:
            es.info()
        except Exception as e:
            logger.error(f"🔥 ES 연결 실패: {e}")
            return

    # 2. MySQL 연결 및 데이터 조회
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            # information_schema에서 우리가 만든 'my_service_db'의 테이블/컬럼 정보 조회
            sql = f"""
                SELECT 
                    t.TABLE_NAME, 
                    t.TABLE_COMMENT, 
                    c.COLUMN_NAME, 
                    c.COLUMN_TYPE, 
                    c.COLUMN_COMMENT
                FROM TABLES t
                JOIN COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
                WHERE t.TABLE_SCHEMA = '{TARGET_DB}'
                ORDER BY t.TABLE_NAME
            """
            logger.info("📡 DB에서 메타데이터 조회 중...")
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            if not rows:
                logger.warning(f"⚠️  '{TARGET_DB}' 데이터베이스에서 테이블을 하나도 못 찾았어! (DBeaver에서 데이터 넣은거 확실해?)")
                return

            logger.info(f"✅ 총 {len(rows)}개의 컬럼 정보를 가져왔어. 이제 조립 시작!")

    except Exception as e:
        logger.error(f"🔥 MySQL 접속 실패: {e}")
        return
    finally:
        conn.close()

    # 3. 데이터 가공 (Row -> Document)
    # MySQL은 컬럼 단위로 주니까, 이걸 '테이블' 단위로 묶어야 해.
    tables = {}
    for row in rows:
        tb_name = row['TABLE_NAME']
        if tb_name not in tables:
            tables[tb_name] = {
                "source": "mysql",
                "database": TARGET_DB,
                "table_name": tb_name,
                "description": row['TABLE_COMMENT'] or "설명 없음",
                "owner": "admin", # 임시 값
                "last_updated": "2025-12-29", # 임시 값 (원래는 datetime.now())
                "columns": []
            }
        
        tables[tb_name]["columns"].append({
            "name": row['COLUMN_NAME'],
            "type": row['COLUMN_TYPE'],
            "comment": row['COLUMN_COMMENT'] or ""
        })

    logger.info(f"🔨 조립 완료! 총 {len(tables)}개의 테이블 문서를 만들었어.")

    # 4. Elasticsearch 적재 (Bulk Insert)
    actions = []
    for tb_name, doc in tables.items():
        action = {
            "_index": ES_INDEX,
            "_id": f"{TARGET_DB}_{tb_name}", # 고유 ID 생성
            "_source": doc
        }
        actions.append(action)

    if actions:
        try:
            logger.info(f"🚀 Elasticsearch로 {len(actions)}건 전송 시작...")
            success, failed = helpers.bulk(es, actions)
            logger.info(f"🎉 전송 완료! 성공: {success}건, 실패: {failed}")
        except Exception as e:
            logger.error(f"🔥 ES 전송 중 에러 발생: {e}")
    else:
        logger.warning("🤔 전송할 데이터가 없네?")

if __name__ == "__main__":
    collect_and_index()