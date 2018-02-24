docker run \
    --rm \
    -it \
    --network gospel \
    mariadb:10 \
    mysql -h gospel-mariadb -u root --password=root --database=gospel
