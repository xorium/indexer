package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

var DefaultDocInfoMarshaller Marshaller = NewDocInfoMarshaller()

type DocInfoMarshaller struct{}

func NewDocInfoMarshaller() *DocInfoMarshaller {
	return &DocInfoMarshaller{}
}

func (d *DocInfoMarshaller) ToBytes(docInfoItem interface{}) ([]byte, error) {
	termData, ok := docInfoItem.(TermData)
	if !ok {
		return nil, fmt.Errorf("can't cast item to *core.DocInfo")
	}
	termInfo := termData.TermInfo
	// Doc ID будет занимать 4 байта.
	docId := make([]byte, 4)
	binary.BigEndian.PutUint32(docId, uint32(termInfo.DocId))

	// Количество позиций будет занимать 2 байта.
	posNum := make([]byte, 2)
	binary.BigEndian.PutUint16(posNum, uint16(len(termInfo.Positions)))

	// Каждая позиция будет занимать 2 байта (закодирована как uint16).
	positions := make([]byte, 2*len(termInfo.Positions))
	for i, pos := range termInfo.Positions {
		if pos > math.MaxUint16 {
			return nil, fmt.Errorf("position %d is to big for uint16", pos)
		}
		binary.BigEndian.PutUint16(positions[i*2:], uint16(pos))
	}

	return bytes.Join([][]byte{docId, posNum, positions}, []byte{}), nil
}

func (d *DocInfoMarshaller) Parse(
	buf []byte,
) (items []interface{}, unparsed []byte, err error) {
	i := 0
	for i < len(buf)-4 {
		docId := int(binary.BigEndian.Uint32(buf[i:]))
		posNum := int(binary.BigEndian.Uint16(buf[i+4:]))
		currInfo := &DocInfo{
			DocId:     docId,
			Positions: make([]int, posNum),
		}

		end := i + 6 + posNum*2
		// Если за текущий вызов не получается полностью распрасить данные для
		// последней структуры, то возвращает уже распаршенные и остальные байты.
		if end > len(buf) {
			break
		}

		i += 6
		for j := i; j < end; j += 2 {
			currInfo.Positions[(j-i)/2] = int(binary.BigEndian.Uint16(buf[j:]))
		}
		items = append(items, currInfo)
		i = end
	}

	return items, buf[i:], nil
}

// SortKey возвращает ключ сортировки в формате {term}{docId}, чтобы данные
// индексе были отсортированы по term'ам и по ID документов.
func (d *DocInfoMarshaller) SortKey(itemKey interface{}) ([]byte, error) {
	termData, ok := itemKey.(TermData)
	if !ok {
		return nil, fmt.Errorf("can't cast itemKey to TermData")
	}
	termDocKey := make([]byte, 54)
	copy(termDocKey, termData.Term)
	binary.BigEndian.PutUint32(termDocKey[50:], uint32(termData.TermInfo.DocId))

	return termDocKey, nil
}

// Key возвращает term в строковом формате - в индексе будут храниться данные
// позиций в и документов, сгруппированные по term'ам.
func (d *DocInfoMarshaller) Key(sortKey []byte) (key string, err error) {
	endPos := bytes.IndexRune(sortKey, 0)
	if endPos != -1 {
		key = string(sortKey[:endPos])
	} else {
		key = string(sortKey)
	}
	return key, nil
}

type FilterMarshaller struct{}

func NewFilterMarshaller() *FilterMarshaller {
	return &FilterMarshaller{}
}

func (d *FilterMarshaller) ToBytes(filterItem interface{}) ([]byte, error) {
	filter, ok := filterItem.(FilterData)
	if !ok {
		return nil, fmt.Errorf("can't cast filterItem to FilterData")
	}

	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, uint32(filter.DocId))

	return data, nil
}

func (d *FilterMarshaller) Parse(
	buf []byte,
) (items []interface{}, unparsed []byte, err error) {
	for i := 0; i < len(buf); i += 4 {
		docId := int(binary.BigEndian.Uint32(buf[i:]))
		items = append(items, docId)
	}
	lastIndex := len(buf) - 1 - len(buf)%4
	return items, buf[lastIndex:], nil
}

// SortKey возвращает ключ сортировки в формате {term}{docId}, чтобы данные
// индексе были отсортированы по term'ам и по ID документов.
func (d *FilterMarshaller) SortKey(itemKey interface{}) ([]byte, error) {
	filterData, ok := itemKey.(FilterData)
	if !ok {
		return nil, fmt.Errorf("can't cast itemKey to FilterData")
	}
	filterKey := make([]byte, 104)
	copy(filterKey, filterData.Value)
	binary.BigEndian.PutUint32(filterKey[100:], uint32(filterData.DocId))

	return filterKey, nil
}

// Key возвращает ключ в строковом формате - в индексе будут храниться данные
// позиций в и документов, сгруппированные по этим ключам.
func (d *FilterMarshaller) Key(sortKey []byte) (key string, err error) {
	endPos := bytes.IndexRune(sortKey, 0)
	if endPos != -1 {
		key = string(sortKey[:endPos])
	} else {
		key = string(sortKey)
	}
	return key, nil
}

type Reader struct {
	dataSource io.ReadCloser
	prevBytes  []byte
	sizeToRead int
	totalRead  int
	chunkSize  int
	marshaller Marshaller
}

func NewReader(dataSource io.ReadSeekCloser, size int, m Marshaller) *Reader {
	return &Reader{
		dataSource: dataSource,
		sizeToRead: size,
		totalRead:  0,
		prevBytes:  make([]byte, 0),
		chunkSize:  512 * 1024,
		marshaller: m,
	}
}

func (d *Reader) WithChunkSize(chunkSize int) *Reader {
	d.chunkSize = chunkSize
	return d
}

func (d *Reader) Read() ([]interface{}, error) {
	// Размер следующего считывающегося куска данных должен быть таким, чтобы
	// суммарное количество считанных данных не превосходило d.sizeToRead.
	// По умолчанию этот размер равен максимальному размеру данных, разрешенных
	// ядром для чтения за 1 операцию.
	chunkSize := minInt(d.chunkSize, d.sizeToRead-d.totalRead)
	// Если считывать больше не нужно, то возвращаем ошибку-индикатор окончания.
	if chunkSize == 0 {
		_ = d.dataSource.Close()
		return nil, io.EOF
	}
	// В текущий буфер должны поместиться, оставшиеся с предыдущего вызова данные
	// и текущие считанные.
	buf := make([]byte, len(d.prevBytes)+chunkSize)
	// Сначала буфер заполняется данными, оставшимися с предыдущего вызова.
	copy(buf, d.prevBytes)
	n, err := d.dataSource.Read(buf[len(d.prevBytes):])
	if errors.Is(err, io.EOF) {
		_ = d.dataSource.Close()
		return nil, io.EOF
	}
	if n == 0 {
		return nil, fmt.Errorf("can't read dataSource data")
	}
	// Если считалось меньше данных, чем chunkSize, то обрезаем буфер.
	if chunkSize < n {
		buf = buf[:len(d.prevBytes)+n]
	}
	// Учитывается количество считанных байтов в общем счетчике.
	d.totalRead += n

	result, unparsed, err := d.marshaller.Parse(buf)
	if err != nil {
		_ = d.dataSource.Close()
		return nil, err
	}
	// Нераспершенные байты сохраняются для интерпретации при следующем вызове.
	d.prevBytes = unparsed

	return result, nil
}

type Iterator struct {
	reader    *Reader
	currItems []interface{}
	currIndex int
	validFlag bool
}

func NewIterator(r *Reader) *Iterator {
	return &Iterator{
		reader:    r,
		currItems: make([]interface{}, 0),
		currIndex: 0,
	}
}

func (i *Iterator) Next() bool {
	var err error
	if i.currIndex >= len(i.currItems) {
		i.currIndex = 0
		i.currItems = nil
		for err == nil && len(i.currItems) == 0 {
			i.currItems, err = i.reader.Read()
		}
		if errors.Is(err, io.EOF) || len(i.currItems) == 0 {
			return false
		}
	}
	return i.currIndex < len(i.currItems)
}

func (i *Iterator) Value() interface{} {
	item := i.currItems[i.currIndex]
	i.currIndex++
	return item
}
