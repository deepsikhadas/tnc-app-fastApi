to start on a new system:
install docker-compose>1.29.2
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose up airflow-init

and you're ready to go
