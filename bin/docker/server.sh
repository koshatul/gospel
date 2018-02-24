docker network create \
    gospel \
    --driver overlay \
    --attachable

docker volume create gospel

docker service create \
    --name gospel-mariadb \
    --env MYSQL_ROOT_PASSWORD=root \
    --env MYSQL_USER=gospel \
    --env MYSQL_PASSWORD=gospel \
    --env MYSQL_DATABASE=gospel \
    --publish 3306:3306 \
    --network gospel \
    --mount type=volume,source=gospel,target=/var/lib/mysql \
    --detach \
    mariadb:10
