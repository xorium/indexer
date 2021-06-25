package core

import (
	cm "bitbucket.org/entrlcom/genproto/gen/go/course-manager/v1"
	si "bitbucket.org/entrlcom/genproto/gen/go/search/indexer/v1"
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"strings"
	"unicode"
)

var otherSymbolsRegex = regexp.MustCompile(`[\s\d\-_#&*+/'|"]+`)

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func RecreateFile(filePath string) error {
	_ = os.Remove(filePath)
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	_ = file.Close()
	return nil
}

func CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}

	out, err := os.Create(dst)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		return err
	}

	if err := in.Close(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}

	return nil
}

func getAlphabet(s []rune) alphabetType {
	if len(s) == 0 {
		return alphabetUnknown
	}
	r := s[0]
	switch {
	case unicode.Is(unicode.Cyrillic, r):
		return alphabetCyrillic
	case unicode.Is(unicode.Latin, r), otherSymbolsRegex.Match([]byte(string(s))):
		return alphabetLatin
	default:
		return alphabetUnknown
	}
}

func castInt(i interface{}) int {
	switch i.(type) {
	case int:
		return i.(int)
	case float64:
		return int(i.(float64))
	}
	return 0
}

func uintPow(base, exp int) uint64 {
	return uint64(math.Pow(float64(base), float64(exp)))
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// runesToBytes конвертирует слайс рун в слайс позиций каждой руны в ее алфавите.
func runesToBytes(runes []rune) ([]byte, error) {
	alphabet := getAlphabet(runes)
	firstLetter := alphabet.FirstLetter()
	cardinality := alphabet.Cardinality()

	lettersPositions := make([]byte, len(runes))
	for i, r := range runes {
		if firstLetter <= r && r <= firstLetter+rune(cardinality) {
			lettersPositions[i] = byte(r-firstLetter) + 1
		} else {
			lettersPositions[i] = alphabet.OffsetBySymbol(r)
		}
	}

	return lettersPositions, nil
}

// bytesToRunes конвертирует слайс позиций каждой руны в алфавите alpha в слайс
// рун.
func bytesToRunes(positions []byte, alphabet alphabetType) ([]rune, error) {
	firstLetter := alphabet.FirstLetter()
	cardinality := alphabet.Cardinality()

	end := bytes.IndexByte(positions, 0)
	if end > 0 {
		positions = positions[:end]
	}

	runes := make([]rune, len(positions))
	for i, pos := range positions {
		if pos > cardinality {
			runes[i] = alphabet.SymbolByOffset(pos)
		} else {
			runes[i] = firstLetter + rune(pos-1)
		}
	}

	return runes, nil
}

// runesToIntegers конвертирует слайс рун в слайс из 64-битных беззнаковых чисел.
// Нужно для сжатия + более быстрого сравнения (чем сравнивать каждую руну).
func runesToIntegers(runes []rune) ([]uint64, error) {
	var (
		cardinality int
		num         uint64
		firstLetter rune
	)
	switch getAlphabet(runes) {
	case alphabetLatin:
		cardinality = 26
		firstLetter = 'a'
	case alphabetCyrillic:
		cardinality = 33
		firstLetter = 'а'
	default:
		return nil, fmt.Errorf("unknown alphabet for %v", runes)
	}

	lettersPositions := make([]uint64, len(runes))
	for i, r := range runes {
		lettersPositions[i] = uint64(r - firstLetter)
	}

	stepSize := int(math.Floor(64 / math.Log2(float64(cardinality))))
	resultLen := int(math.Ceil(float64(len(lettersPositions)) / float64(stepSize)))
	res := make([]uint64, resultLen)
	for i := 0; i < len(lettersPositions); i += stepSize {
		num = 0
		upperBound := minInt(i+stepSize, len(lettersPositions))
		for j := i; j < upperBound; j++ {
			num += lettersPositions[j] + uintPow(cardinality, i+stepSize-j-1)
		}
		res[i/stepSize] = num
	}

	return res, nil
}

func PrepareFieldName(fieldName string) string {
	return strings.ToLower(fieldName)
}

const (
	fieldNameLevel           = "level"
	fieldNamePace            = "pace"
	fieldNameStatus          = "status"
	fieldNameVideoDuration   = "video_duration"
	fieldNameLanguages       = "languages"
	fieldNameSubtitles       = "subtitles"
	fieldNameTitle           = "title"
	fieldNameHeadline        = "headline"
	fieldNameDesc            = "description"
	fieldNameProviderName    = "provider.name"
	fieldNamePartnersName    = "partners.name"
	fieldNameInstructorsName = "instructors.name"
	fieldNameTopicsName      = "topics.name"
)

func GetCourseDurationLabel(c *cm.Course) string {
	type interval struct {
		from  int
		to    int
		label string
	}
	intervals := []interval{
		{0, 3, "0-3 hr"},
		{3, 7, "3-7 hr"},
		{7, 15, "7-15 hr"},
		{15, math.MaxInt32, "15+ hr"},
	}
	courseHoursDur := int(c.VideoDurationS) / 3600
	for _, interval := range intervals {
		if interval.from <= courseHoursDur && courseHoursDur < interval.to {
			return interval.label
		}
	}
	return ""
}

func GetCourseDocument(course *cm.Course) *si.Document {
	doc := &si.Document{Id: course.Id}
	doc.Fields = append(doc.Fields, &si.DocumentField{
		Name:  fieldNameLevel,
		Value: fmt.Sprintf("%v", course.Level),
		Type:  si.DocumentField_STRING,
	})
	doc.Fields = append(doc.Fields, &si.DocumentField{
		Name:  fieldNamePace,
		Value: fmt.Sprintf("%v", course.Pace),
		Type:  si.DocumentField_STRING,
	})
	doc.Fields = append(doc.Fields, &si.DocumentField{
		Name:  fieldNameStatus,
		Value: fmt.Sprintf("%v", course.Status),
		Type:  si.DocumentField_STRING,
	})
	doc.Fields = append(doc.Fields, &si.DocumentField{
		Name:  fieldNameVideoDuration,
		Value: GetCourseDurationLabel(course),
		Type:  si.DocumentField_STRING,
	})
	if course.Title != "" {
		doc.Fields = append(doc.Fields, &si.DocumentField{
			Name:  fieldNameTitle,
			Value: course.Title,
			Type:  si.DocumentField_STRING,
		})
	}
	if course.Headline != "" {
		doc.Fields = append(doc.Fields, &si.DocumentField{
			Name:  fieldNameHeadline,
			Value: course.Headline,
			Type:  si.DocumentField_STRING,
		})
	}
	if course.Description != "" {
		doc.Fields = append(doc.Fields, &si.DocumentField{
			Name:  fieldNameDesc,
			Value: course.Description,
			Type:  si.DocumentField_STRING,
		})
	}
	if course.Provider != nil {
		doc.Fields = append(doc.Fields, &si.DocumentField{
			Name:  fieldNameProviderName,
			Value: course.Provider.Name,
			Type:  si.DocumentField_STRING,
		})
	}

	for _, lang := range course.Languages {
		doc.Fields = append(doc.Fields, &si.DocumentField{
			Name:  fieldNameLanguages,
			Value: fmt.Sprintf("%v", lang),
			Type:  si.DocumentField_STRING,
		})
	}
	for _, sub := range course.Subtitles {
		doc.Fields = append(doc.Fields, &si.DocumentField{
			Name:  fieldNameSubtitles,
			Value: fmt.Sprintf("%v", sub),
			Type:  si.DocumentField_STRING,
		})
	}
	for _, instructor := range course.Instructors {
		if instructor == nil {
			continue
		}
		doc.Fields = append(doc.Fields, &si.DocumentField{
			Name:  fieldNameInstructorsName,
			Value: instructor.Name,
			Type:  si.DocumentField_STRING,
		})
	}
	for _, partner := range course.Partners {
		if partner == nil {
			continue
		}
		doc.Fields = append(doc.Fields, &si.DocumentField{
			Name:  fieldNamePartnersName,
			Value: partner.Name,
			Type:  si.DocumentField_STRING,
		})
	}
	for _, topic := range course.Topics {
		if topic == nil {
			continue
		}
		doc.Fields = append(doc.Fields, &si.DocumentField{
			Name:  fieldNameTopicsName,
			Value: topic.Name,
			Type:  si.DocumentField_STRING,
		})
	}

	return doc
}
