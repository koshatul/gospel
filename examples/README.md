# Gospel Examples

## Prerequisites

- This requires Go 1.6 or later.
- MariaDB 10.3 is running and has a database and user ready ([quick start MariaDB in docker](#Quick-Start-MariaDB-in-Docker)).

## Example Installation

```
go get -u github.com/jmalloc/gospel
```

Install vendored libraries
```
cd $GOPATH/src/github.com/jmalloc/gospel
make vendor
```

## Try it

- Listen for new events and read them

  ```
  $ go run examples/read/main.go
  ```

- Append events as fast as possible

  ```
  $ go run examples/append/main.go
  ```

- Append a single event
  ```
  $ go run examples/append_single/main.go
  ```

- Append unchecked events

  ```
  $ go run examples/append_unchecked/main.go
  ```

## Appendix

## Quick Start MariaDB in Docker

```
docker run \
 --rm \
 -d \
 --name gospel_mariadb \
 -p 127.0.0.1:3306:3306 \
 --env MYSQL_RANDOM_ROOT_PASSWORD=true \
 --env MYSQL_DATABASE=gospel \
 --env MYSQL_USER=gospel \
 --env MYSQL_PASSWORD=gospel \
 mariadb:10.3
```