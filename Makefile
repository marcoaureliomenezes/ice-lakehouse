current_branch = 1.0.0


deploy_services:
	@docker-compose -f services/lakehouse_services.yml down
	@docker-compose -f services/lakehouse_services.yml up -d --build

stop_services:
	@docker-compose -f services/lakehouse_services.yml down

watch_logs:
	@watch docker-compose -f services/lakehouse_services.yml ps