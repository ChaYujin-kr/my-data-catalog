# 📚 My Data Catalog

MySQL의 테이블 메타데이터를 수집하여 Elasticsearch에 적재하고, Kibana Vega로 시각화/검색하는 데이터 카탈로그 프로젝트입니다.

## 🛠 Tech Stack
- **Database:** MySQL 8.0
- **Search Engine:** Elasticsearch 8.11
- **Visualization:** Kibana (Vega-Lite)
- **Collector:** Python 3.11 (PyMySQL, Elasticsearch client)
- **Infra:** Docker Compose

## 🚀 How to Run
1. `docker-compose up -d`
2. `python src/setup.py`
3. `python src/collector.py`
