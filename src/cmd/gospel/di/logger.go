package di

import (
	"io/ioutil"
	"log"

	"github.com/jmalloc/twelf/src/twelf"
)

func init() {
	Container.Define(
		func() twelf.Logger {
			return &twelf.StandardLogger{
				Target: log.New(ioutil.Discard, "", 0),
			}
		},
	)
}
