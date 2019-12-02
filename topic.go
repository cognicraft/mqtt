package mqtt

import "strings"

const (
	separator       = "/"
	wildMultiLevel  = "#"
	wildSingleLevel = "+"
)

type Topic string

func (t Topic) Parts() []string {
	return strings.Split(string(t), separator)
}

func (t Topic) IsValid() bool {
	return !strings.ContainsAny(string(t), "#+")
}

func (t Topic) IsFilter() bool {
	if t.IsValid() {
		return true
	}
	mlw := strings.Index(string(t), wildMultiLevel)
	if mlw < 0 {
		return true
	}
	return mlw == len(t)-1
}

func (t Topic) Accept(o Topic) bool {
	if !t.IsFilter() || !o.IsValid() {
		return false
	}
	if t == o {
		return true
	}
	as := t.Parts()
	bs := o.Parts()
	if len(as) != len(bs) && as[len(as)-1] != wildMultiLevel {
		return false
	}
	// both have same number of parts
	for i := 0; i < len(as); i++ {
		a := as[i]
		b := bs[i]
		switch {
		case a == wildMultiLevel:
			return true
		case a == wildSingleLevel:
			continue
		case a == b:
			continue
		case a != b:
			return false
		}
	}
	return true
}
