#!/bin/bash
#Este script detiene los contenedores levantados
date
echo 'Bajando servicios'
docker-compose down
date
echo 'Los servicios se bajaron correctamente'


