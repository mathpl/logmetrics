package logmetrics

import (
	"bytes"
	"log"
	"os"

	"github.com/mathpl/golang-pkg-pcre/src/pkg/pcre"
	"github.com/metakeule/replacer"
)

type transform struct {
	replace_only_one   bool
	log_default_assign bool

	ops []interface{}
}

type replace struct {
	str      string
	repl     []byte
	matcher  *pcre.Regexp
	replacer replacer.Replacer
}

type match_or_default struct {
	str         string
	default_val string
	matcher     *pcre.Regexp
}

func (r *replace) init(regexp string, template string) {
	matcher := pcre.MustCompile(regexp, 0)
	r.matcher = &matcher

	r.replacer = replacer.New()
	r.replacer.Parse([]byte(template))
}

func (m *match_or_default) init(regexp string, default_val string) {
	matcher := pcre.MustCompile(regexp, 0)
	m.matcher = &matcher
	m.default_val = default_val
}

func (t *transform) apply(data string) string {
	for _, operation := range t.ops {
		got_match := false
		switch op := operation.(type) {
		case replace:
			if (t.replace_only_one && !got_match) || !t.replace_only_one {
				m := op.matcher.MatcherString(data, 0)
				got_match = m.Matches()
				if got_match {
					var buf bytes.Buffer

					replace_map := build_replace_map(m.ExtractString())
					op.replacer.Replace(&buf, replace_map)
					data = buf.String()
				}
			}
		case match_or_default:
			m := op.matcher.Matcher([]byte(data), 0)
			if !m.Matches() {
				if t.log_default_assign {
					log.Printf("Assigning default value to: %s", data)
				}
				data = op.default_val
			}
		}
	}

	return data
}

func parseTransform(conf map[interface{}]interface{}) map[int]transform {
	transforms := make(map[int]transform)

	for position, setting := range conf {
		switch s := setting.(type) {
		case map[interface{}]interface{}:
			var transform transform

			var ok bool
			if transform.replace_only_one, ok = s["replace_only_one"].(bool); !ok {
				transform.replace_only_one = false
			}
			if transform.log_default_assign, ok = s["log_default_assign"].(bool); !ok {
				transform.log_default_assign = false
			}

			var operations []interface{}
			if operations, ok = s["operations"].([]interface{}); ok {
				for _, args := range operations {

					var str_args []string
					// Convert to []string
					for _, arg := range args.([]interface{}) {
						str_args = append(str_args, arg.(string))
					}

					switch str_args[0] {
					case "replace":
						var r replace
						r.init(str_args[1], str_args[2])
						transform.ops = append(transform.ops, r)
					case "match_or_default":
						var m match_or_default
						m.init(str_args[1], str_args[2])
						transform.ops = append(transform.ops, m)
					}
				}
			} else {
				log.Print("No operation under tranform group.")
				os.Exit(1)
			}

			transforms[position.(int)] = transform
		}
	}

	return transforms
}
