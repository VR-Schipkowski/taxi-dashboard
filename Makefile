test:
	DATA_DIR=./provider/data/test/taxi-data docker compose up

release:
	DATA_DIR=./provider/data/release/taxi_log_2008_by_id docker compose up

down:
	docker compose down
clean:
	docker compose down && docker compose prune -f