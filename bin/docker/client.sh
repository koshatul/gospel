docker run \
    --rm \
    -it \
    --network streakdb \
    mariadb:10 \
    mysql -h streakdb-mariadb -u root --password=root --database=streakdb
