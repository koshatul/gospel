REQ += src/gospelmaria/schema/schema.gen.go

-include artifacts/make/go/Makefile

artifacts/make/%/Makefile:
	curl -sf https://jmalloc.github.io/makefiles/fetch | bash /dev/stdin $*

src/gospelmaria/schema/schema.gen.go: $(shell find src/gospelmaria/schema -name '*.sql' | sort)
	@mkdir -p $(@D)
	echo 'package schema' > "$@"
	echo >> "$@"
	echo 'var statements = `' >> "$@"
	cat $^ >> "$@"
	echo '`' >> "$@"
