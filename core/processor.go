package core

import (
	"bitbucket.org/entrlcom/indexer/lib/numerator"
	"bitbucket.org/entrlcom/indexer/lib/tokenizer"
	"github.com/aaaton/golem/v4"
	"github.com/aaaton/golem/v4/dicts/en"
)

type alphabetType int

const (
	alphabetUnknown = iota
	alphabetLatin
	alphabetCyrillic
)

func (t alphabetType) String() string {
	switch t {
	case alphabetLatin:
		return "en"
	case alphabetCyrillic:
		return "ru"
	default:
		return "unknown"
	}
}

func (t alphabetType) Cardinality() byte {
	switch t {
	case alphabetLatin:
		return 26
	case alphabetCyrillic:
		return 33
	default:
		return 0
	}
}

func (t alphabetType) FirstLetter() rune {
	switch t {
	case alphabetLatin:
		return 'a'
	case alphabetCyrillic:
		return 'а'
	default:
		return '0'
	}
}

func (t alphabetType) OffsetBySymbol(r rune) byte {
	return t.Cardinality() + offsetsOfOtherSymbols[r]
}

func (t alphabetType) SymbolByOffset(offset byte) rune {
	return otherSymbolsByOffsets[offset-t.Cardinality()]
}

type FilterData struct {
	Value string
	DocId int
}

type TermData struct {
	Term     string
	TermInfo DocInfo
}

type DocInfo struct {
	DocId     int
	Positions []int
}

func (i *DocInfo) Equal(other *DocInfo) bool {
	if i.DocId != other.DocId {
		return false
	}
	if len(i.Positions) != len(other.Positions) {
		return false
	}
	for i, pos := range i.Positions {
		if pos != other.Positions[i] {
			return false
		}
	}
	return true
}

type TokenData struct {
	Token string
	Info  DocInfo
}

type TextProcessor struct {
	numerator   numerator.Numerator
	tokenizer   tokenizer.Tokenizer
	lemmatizers map[alphabetType]*golem.Lemmatizer
}

func NewDocumentProcessor() *TextProcessor {
	lemmatizers := make(map[alphabetType]*golem.Lemmatizer)
	// TODO: добавить кириллицу
	lemmatizers[alphabetLatin], _ = golem.New(en.New())

	return &TextProcessor{
		numerator:   numerator.NewNumerator(),
		tokenizer:   tokenizer.NewTokenizer(),
		lemmatizers: lemmatizers,
	}
}

func (p *TextProcessor) getLemmatizer(text string) (*golem.Lemmatizer, bool) {
	alphabet := getAlphabet([]rune(text))
	if alphabet == alphabetUnknown {
		return nil, false
	}
	lemmatizer, ok := p.lemmatizers[alphabet]
	return lemmatizer, ok
}

func (p *TextProcessor) ProcessDocumentValue(docId int, docText string) ([]*TokenData, error) {
	result := make([]*TokenData, 0)
	tokens := p.tokenizer.Tokenize(docText)
	tokenStats := p.numerator.Numerate(tokens)
	// TODO: получение корректного лемматайзера
	lemmatizer, _ := p.lemmatizers[alphabetLatin]
	for token, positions := range tokenStats {
		// lemmatizer, ok := p.getLemmatizer(token.Text)
		// if !ok {
		// 	continue
		// }
		tokenData := &TokenData{
			Token: lemmatizer.Lemma(token),
			Info: DocInfo{
				DocId:     docId,
				Positions: positions,
			},
		}
		result = append(result, tokenData)
	}

	return result, nil
}

func (p *TextProcessor) ProcessQuery(query string) (terms []string, err error) {
	tokens := p.tokenizer.Tokenize(query)
	terms = make([]string, len(tokens))

	lemmatizer, _ := p.lemmatizers[alphabetLatin]
	//lemmatizer, ok := p.getLemmatizer(query)
	//if !ok {
	//	return nil, fmt.Errorf("can't get lemmatizer: unknown alphabet")
	//}
	for i, token := range tokens {
		terms[i] = lemmatizer.Lemma(token)
	}

	return terms, nil
}
