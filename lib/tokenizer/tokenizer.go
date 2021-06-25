package tokenizer

import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Tokenizer interface {
	Tokenize(text string) []string
}

func NewTokenizer() Tokenizer {
	return &RegexTokenizer{}
}

type tokenizer struct{}

func (t *tokenizer) Tokenize(text string) []*Token {
	res := make([]*Token, 0)

	s := sanitizer.Replace(text)

	start, index := 0, 0

	isSpace := false

	cache := make(map[string][]*Token, 0)

	for index < len(s) {
		r, size := utf8.DecodeRuneInString(s[index:])
		if size == 0 {
			break
		}
		if index == 0 {
			isSpace = unicode.IsSpace(r)
		}

		if unicode.IsSpace(r) != isSpace {
			if start < index {
				span := s[start:index]
				if v, ok := cache[span]; ok {
					res = append(res, v...)
				} else {
					tokens := split(span)
					cache[span] = tokens
					res = append(res, tokens...)
				}
			}

			start = index
			if unicode.IsSpace(r) {
				start++
			}

			isSpace = !isSpace
		}

		index += size
	}

	if start < index {
		res = append(res, split(s[start:index])...)
	}

	return res
}

type Tokens []*Token

func (t Tokens) Len() int           { return len(t) }
func (t Tokens) Less(i, j int) bool { return t[i].Text < t[j].Text }
func (t Tokens) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

type Token struct {
	Pos  string // Part of speech
	Iob  string // IOB
	Text string
}

var sanitizer = strings.NewReplacer(
	"\u201c", `"`,
	"\u201d", `"`,
	"\u2018", "'",
	"\u2019", "'",
	"&rsquo;", "'",
)

func split(token string) []*Token {
	res := make([]*Token, 0)

	// TODO: do we really need it?
	suffs := make([]*Token, 0)

	last := 0

	for len(token) != 0 && utf8.RuneCountInString(token) != last {
		if isSpecial(token) {
			res = append(res, &Token{
				Pos:  "",
				Iob:  "",
				Text: token, // TODO: remove spaces
			})

			break
		}

		last = utf8.RuneCountInString(token)
		lower := strings.ToLower(token)

		if hasPrefix(token, prefixes) {
			res = append(res, &Token{
				Pos:  "",
				Iob:  "",
				Text: token, // TODO: remove spaces
			})

			token = token[1:] // TODO: use len(prefix) instead of 1
		} else

		// Handle "they'll", "I'll", etc.
		// Handle "Don't", "won't", etc.
		// TODO: won't is ["will", "n't"] not ["wo", "n't"]
		if idx := hasAnyIndex(lower, []string{"'ll", "'s", "'re", "'m", "n't"}); idx > -1 {
			res = append(res, &Token{
				Pos:  "",
				Iob:  "",
				Text: token, // TODO: remove spaces
			})

			token = token[idx:]
		} else if hasSuffix(token, suffixes) {
			suffs = append([]*Token{
				{
					Text: string(token[len(token)-1]),
				},
			}, suffs...)

			token = token[:len(token)-1]
		} else {
			res = append(res, &Token{
				Pos:  "",
				Iob:  "",
				Text: token, // TODO: remove spaces
			})
		}
	}

	res = append(res, suffs...)

	return res
}

func isSpecial(token string) bool {
	_, ok := emoticons[token]
	return ok || internalRE.MatchString(token)
}

func hasPrefix(token string, prefixes []string) bool {
	for _, v := range prefixes {
		if len(token) > len(v) && strings.HasPrefix(token, v) {
			return true
		}
	}

	return false
}

func hasSuffix(token string, prefixes []string) bool {
	for _, v := range prefixes {
		if len(token) > len(v) && strings.HasSuffix(token, v) {
			return true
		}
	}

	return false
}

func hasAnyIndex(s string, suffixes []string) int {
	for _, suffix := range suffixes {
		idx := strings.Index(s, suffix)
		if idx >= 0 && len(s) > len(suffix) {
			return idx
		}
	}

	return -1
}

var internalRE = regexp.MustCompile(`^(?:[A-Za-z]\.){2,}$|^[A-Z][a-z]{1,2}\.$`)
var prefixes = []string{"$", "(", `"`, "["}
var suffixes = []string{",", ")", `"`, "]", "!", ";", ".", "?", ":", "'"}
var emoticons = map[string]struct{}{
	"(-8":       {},
	"(-;":       {},
	"(-_-)":     {},
	"(._.)":     {},
	"(:":        {},
	"(=":        {},
	"(o:":       {},
	"(Â¬_Â¬)":   {},
	"(à² _à² )": {},
	"(â•¯Â°â–¡Â°ï¼‰â•¯ï¸µâ”»â”â”»": {},
	"-__-":         {},
	"8-)":          {},
	"8-D":          {},
	"8D":           {},
	":(":           {},
	":((":          {},
	":(((":         {},
	":()":          {},
	":)))":         {},
	":-)":          {},
	":-))":         {},
	":-)))":        {},
	":-*":          {},
	":-/":          {},
	":-X":          {},
	":-]":          {},
	":-o":          {},
	":-p":          {},
	":-x":          {},
	":-|":          {},
	":-}":          {},
	":0":           {},
	":3":           {},
	":P":           {},
	":]":           {},
	":`(":          {},
	":`)":          {},
	":`-(":         {},
	":o":           {},
	":o)":          {},
	"=(":           {},
	"=)":           {},
	"=D":           {},
	"=|":           {},
	"@_@":          {},
	"O.o":          {},
	"O_o":          {},
	"V_V":          {},
	"XDD":          {},
	"[-:":          {},
	"^___^":        {},
	"o_0":          {},
	"o_O":          {},
	"o_o":          {},
	"v_v":          {},
	"xD":           {},
	"xDD":          {},
	"Â¯\\(ãƒ„)/Â¯": {},
}

var (
	tokenRegex = regexp.MustCompile(
		`(([а-я\w\d]$|\*)|([а-я\w\d][a-я\w\d\-_']*\d+\.\d+)|([а-я\w\d][a-я\w\d\-_']*[а-я\w\d+#]+))`,
	)
)

type RegexTokenizer struct{}

func (t *RegexTokenizer) Tokenize(text string) []string {
	text = strings.ToLower(text)
	tokens := tokenRegex.FindAllString(text, -1)
	return tokens
}
