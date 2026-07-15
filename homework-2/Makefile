TOPIC ?= test-topic
MAX_MSGS ?= 100
P ?= 1
C ?= 1
PROD ?= 1
NAME ?= Custom Run

.PHONY: setup up down clean build run exp-h exp-i exp-j exp-k exp-l exp-m exp-n exp-o plot

build:
	docker compose build

up:
	docker compose up -d redpanda-0 redpanda-1
	@echo "Waiting 10 seconds for Redpanda cluster to initialize..."
	@sleep 10

setup: build up
	@echo "Downloading and setting up dataset..."
	docker compose run --rm processor python src/setup_data.py

down:
	docker-compose down

clean: down
	@echo "Cleaning up all logs, reports, and containers..."
	rm -rf logs/*.csv report/*.csv report/*.png

run:
	@echo "\n=== Starting Experiment: $(NAME) ==="
	@rm -f logs/consumer_*_logs.csv
	@echo "Recreating topic '$(TOPIC)' with $(P) partitions..."
	@docker compose exec -T redpanda-0 rpk topic delete $(TOPIC) || true
	@docker compose exec -T redpanda-0 rpk topic create $(TOPIC) -p $(P) -r 1
	@echo "Scaling consumers to $(C)..."
	@docker compose up -d --scale processor=$(C) processor
	@echo "Waiting 10 seconds for consumers to balance partitions..."
	@sleep 10
	@echo "Firing up $(PROD) producer(s) in parallel..."
	@for i in $$(seq 1 $(PROD)); do \
		docker compose run --rm processor python src/generator.py --topic $(TOPIC) --max $(MAX_MSGS) & \
	done; \
	wait
	@echo "Producers finished sending."
	@WAIT_TIME=$$(( ($(MAX_MSGS) * $(PROD) / $(C)) + 15 )); \
	echo "Waiting $$WAIT_TIME seconds for consumers to process the queue..."; \
	sleep $$WAIT_TIME
	@echo "Stopping consumers..."
	@docker compose stop processor
	@docker compose rm -f -s processor
	@docker compose run --rm processor python src/aggregator.py --name "$(NAME)"

exp-h:
	$(MAKE) run P=1 C=1 PROD=1 NAME="h (1P/1C)"

exp-i:
	$(MAKE) run P=1 C=2 PROD=1 NAME="i (1P/2C)"

exp-j:
	$(MAKE) run P=2 C=2 PROD=1 NAME="j (2P/2C)"

exp-k:
	$(MAKE) run P=5 C=5 PROD=1 NAME="k (5P/5C)"

exp-l:
	$(MAKE) run P=10 C=1 PROD=1 NAME="l (10P/1C)"

exp-m:
	$(MAKE) run P=10 C=5 PROD=1 NAME="m (10P/5C)"

exp-n:
	$(MAKE) run P=10 C=10 PROD=1 NAME="n (10P/10C)"

exp-o:
	$(MAKE) run P=10 C=10 PROD=2 NAME="o (10P/10C 2 Prod)"

plot:
	@echo "Generating performance graphs..."
	@docker compose run --rm processor python src/plot_results.py
