ifeq ($(GOSPEL_MARIADB_DSN),)
export GOSPEL_MARIADB_DSN := gospel:gospel@tcp(127.0.0.1:3306)/gospel
endif

REQ += artifacts/mariadb/schema.go

-include artifacts/make/go/Makefile

artifacts/make/%/Makefile:
	curl -sf https://jmalloc.github.io/makefiles/fetch | bash /dev/stdin $*

artifacts/mariadb/schema.go: $(shell find src/gospelmaria/schema -name '*.sql')
	@mkdir -p $(@D)
	echo 'package schema' > "$@"
	echo 'var Statements = `' >> "$@"
	cat $^ >> "$@"
	echo '`' >> "$@"
