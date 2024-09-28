current_branch = 1.0.0



build_notebook:
	@docker build -t ice-lakehouse-notebook:$(current_branch) ./docker/notebook

build_spark:
	@docker build -t ice-lakehouse-spark:$(current_branch) ./docker/spark


deploy_services:
	@docker compose -f services/lakehouse_services.yml down
	@docker compose -f services/lakehouse_services.yml up -d --build

stop_services:
	@docker compose -f services/lakehouse_services.yml down

watch_services:
	@watch docker compose -f services/lakehouse_services.yml ps