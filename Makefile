REQ += artifacts/mariadb/schema.go

-include artifacts/make/go/Makefile

artifacts/make/%/Makefile:
	curl -sf https://jmalloc.github.io/makefiles/fetch | bash /dev/stdin $*

artifacts/mariadb/schema.go: $(shell find src/driver/mariadb/schema -name '*.sql')
	@mkdir -p $(@D)
	echo 'package schema' > "$@"
	echo 'var Statements = `' >> "$@"
	cat $^ >> "$@"
	echo '`' >> "$@"
