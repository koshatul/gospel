docker network create \
    streakdb \
    --driver overlay \
    --attachable

docker volume create streakdb

docker service create \
    --name streakdb-mariadb \
    --env MYSQL_ROOT_PASSWORD=root \
    --env MYSQL_USER=streakdb \
    --env MYSQL_PASSWORD=streakdb \
    --env MYSQL_DATABASE=streakdb \
    --publish 3306:3306 \
    --network streakdb \
    --mount type=volume,source=streakdb,target=/var/lib/mysql \
    --detach \
    mariadb:10
