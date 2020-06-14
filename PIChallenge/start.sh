#!/bin/bash
#Este script obtiene y lanza las imagenes docker de Airflow y MSSQL. Además restaura el backup Testing_ETL.bak y crea un usuario de aplicación
date
echo 'Levantando imagenes de AirFlow y MSSQL'
docker-compose up -d
date
echo 'Instalando librerias faltantes'
docker exec -it pichallenge_webserver_1  pip install pymssql --upgrade
date
echo 'Restaurando backup Testing_ETL.bak'
sleep 15
docker exec -it sql-server-db /opt/mssql-tools/bin/sqlcmd -S 127.0.0.1 -U SA -P "AdminPiChallenge1" -Q "RESTORE DATABASE Testing_ETL FROM DISK = '/var/opt/mssql/backup/Testing_ETL.bak' WITH MOVE 'Testing_ETL' TO '/var/opt/mssql/data/Testing_ETL.mdf', MOVE 'Testing_ETL_log' TO '/var/opt/mssql/data/Testing_ETL.ldf'"
date
echo 'Creando group login y usuario'
docker exec -it sql-server-db /opt/mssql-tools/bin/sqlcmd -S 127.0.0.1 -U SA -P "AdminPiChallenge1" -i "/var/opt/mssql/backup/create_user.sql"
date
echo 'Creando Conexión  airflow-mssql'
#Obtengo mi ip
export my_ip=$(hostname)
echo my_ip
#Creo conexión
docker-compose -f docker-compose.yml run --rm webserver airflow connections --add --conn_id 'mssql_pi' --conn_login 'pi' --conn_password 'AdminPI!' --conn_host '192.168.1.236' --conn_type 'mssql' --conn_port '1433' --conn_schema 'TESTING_ETL'
date
echo 'Fin del despliegue'
echo 'Airflow URL: http://localhost:8080/admin/'




