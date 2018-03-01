MATRIX_OS := darwin linux windows

REQ += artifacts/mariadb/schema.go

-include artifacts/make/go/Makefile

.PHONY: run
run: artifacts/build/debug/$(GOOS)/$(GOARCH)/gospel
	"$<" $(RUN_ARGS)

artifacts/make/%/Makefile:
	curl -sf https://jmalloc.github.io/makefiles/fetch | bash /dev/stdin $*

artifacts/mariadb/schema.go: $(shell find src/gospelmaria/schema -name '*.sql' | sort)
	@mkdir -p $(@D)
	echo 'package schema' > "$@"
	echo 'var Statements = `' >> "$@"
	cat $^ >> "$@"
	echo '`' >> "$@"
