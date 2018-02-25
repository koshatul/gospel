package mariadb

import "bytes"

// escapeString returns a quoted and escaped representation of s.
// Neither the Go 'sql' package, nor the mysql driver currently expose string
// escaping functions. See https://github.com/golang/go/issues/18478.
func escapeString(s string) string {
	var buf bytes.Buffer

	buf.WriteRune('\'')

	for _, r := range s {
		switch r {
		case '\x00':
			buf.WriteString(`\0`)
		case '\x1a':
			buf.WriteString(`\Z`)
		case '\r':
			buf.WriteString(`\r`)
		case '\n':
			buf.WriteString(`\n`)
		case '\b':
			buf.WriteString(`\b`)
		case '\t':
			buf.WriteString(`\t`)
		case '\\':
			buf.WriteString(`\\`)
		case '\'':
			buf.WriteString(`\'`)
		default:
			buf.WriteRune(r)
		}
	}

	buf.WriteRune('\'')

	return buf.String()
}

// escapeStrings escapes a slice of strings.
func escapeStrings(s []string) []string {
	escaped := make([]string, len(s))

	for i, v := range s {
		escaped[i] = escapeString(v)
	}

	return escaped
}
