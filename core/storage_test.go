package core

import (
	"errors"
	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"io"
	"math/rand"
	"testing"
	"time"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func BenchmarkFileSystemStorage_FinishCreatingIndex(t *testing.B) {
	rand.Seed(time.Now().UnixNano())
	indexName := "test_index"
	s := NewFileSystemStorage()

	opts := []StorageOpt{
		OptCompressKey(true), OptMaxKeyAlphaPower(33),
		OptMaxKeySize(24),
	}
	err := s.StartCreatingIndex(indexName, opts...)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 130_000; i++ {
		err := s.UpdateIndex(indexName, TermData{
			RandStringRunes(10),
			DocInfo{
				DocId:     rand.Intn(100),
				Positions: []int{rand.Intn(10), rand.Intn(10), rand.Intn(10)},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := s.FinishCreatingIndex(indexName); err != nil {
		t.Fatal(err)
	}
}

func TestFileSystemStorage_StartCreatingIndex(t *testing.T) {
	indexName := "test_index"
	s := NewFileSystemStorage()

	opts := []StorageOpt{
		OptCompressKey(true), OptMaxKeyAlphaPower(33),
		OptMaxKeySize(24),
	}
	err := s.StartCreatingIndex(indexName, opts...)
	if err != nil {
		t.Fatal(err)
	}
	_, ok := s.indexDbs[indexName]
	assert.True(t, ok)
}

func TestFileSystemStorage_UpdateIndex(t *testing.T) {
	indexName := "test_index"
	s := NewFileSystemStorage()

	opts := []StorageOpt{
		OptCompressKey(true), OptMaxKeyAlphaPower(33),
		OptMaxKeySize(24),
	}
	err := s.StartCreatingIndex(indexName, opts...)
	if err != nil {
		t.Fatal(err)
	}
	term := "term1"
	docInfo := DocInfo{
		DocId:     1,
		Positions: []int{2, 4, 6},
	}
	termData := TermData{term, docInfo}
	err = s.UpdateIndex(indexName, termData)
	if err != nil {
		t.Fatal(err)
	}

	db := s.indexDbs[indexName]
	docTermKey, _ := DefaultDocInfoMarshaller.SortKey(termData)
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(docTermKey)
		if err != nil {
			return err
		}
		assert.True(t, item.String() != "")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestFileSystemStorage_FinishCreatingIndex(t *testing.T) {
	indexName := "test_index"
	s := NewFileSystemStorage()

	opts := []StorageOpt{
		OptCompressKey(true), OptMaxKeyAlphaPower(33),
		OptMaxKeySize(24),
	}
	err := s.StartCreatingIndex(indexName, opts...)
	if err != nil {
		t.Fatal(err)
	}

	termData := TermData{
		"term1",
		DocInfo{
			DocId:     1,
			Positions: []int{2, 4, 6},
		},
	}
	err = s.UpdateIndex(indexName, termData)
	if err != nil {
		t.Fatal(err)
	}
	err = s.UpdateIndex(
		indexName,
		TermData{
			"term2",
			DocInfo{
				DocId:     1,
				Positions: []int{1, 3, 5},
			},
		})
	if err != nil {
		t.Fatal(err)
	}
	err = s.UpdateIndex(
		indexName,
		TermData{
			"very_long_term_abc123",
			DocInfo{
				DocId:     2,
				Positions: []int{1, 2},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if err = s.FinishCreatingIndex(indexName); err != nil {
		t.Fatal(err)
	}
}

func TestFastAccessTable_AddTermOffset(t *testing.T) {
	opts := []StorageOpt{
		OptCompressKey(true), OptMaxKeyAlphaPower(33),
		OptMaxKeySize(24),
	}
	fat := NewFastAccessTable(opts...)

	err := fat.AddKeyOffset([]byte("term1"), 12, 34)
	if err != nil {
		t.Fatal(err)
	}
	err = fat.AddKeyOffset([]byte("term2"), 46, 56)
	if err != nil {
		t.Fatal(err)
	}
	fat.PreloadKeysIntoMap()

	assert.True(t, fat.Buffers[alphabetLatin].Len() > 0)
}

func TestFastAccessTable_GetTermOffsetSize(t *testing.T) {
	opts := []StorageOpt{
		OptCompressKey(true), OptMaxKeyAlphaPower(33),
		OptMaxKeySize(24),
	}
	fat := NewFastAccessTable(opts...)

	err := fat.AddKeyOffset([]byte("ab"), 125, 19)
	if err != nil {
		t.Fatal(err)
	}
	err = fat.AddKeyOffset([]byte("cd"), 144, 32)
	if err != nil {
		t.Fatal(err)
	}
	err = fat.AddKeyOffset([]byte("term1"), 12, 34)
	if err != nil {
		t.Fatal(err)
	}
	err = fat.AddKeyOffset([]byte("term2"), 46, 56)
	if err != nil {
		t.Fatal(err)
	}
	err = fat.AddKeyOffset([]byte("very_long_term_abc123"), 102, 23)
	if err != nil {
		t.Fatal(err)
	}

	err = fat.AddKeyOffset([]byte("тест123"), 12, 34)
	if err != nil {
		t.Fatal(err)
	}

	fat.PreloadKeysIntoMap()

	offset, size := fat.GetOffsetSizeByKey([]byte("term2"))
	assert.Equal(t, 46, int(offset))
	assert.Equal(t, 56, int(size))

	offset, size = fat.GetOffsetSizeByKey([]byte("doesnt_exist"))
	assert.Equal(t, 0, int(offset))
	assert.Equal(t, 0, int(size))

	offset, size = fat.GetOffsetSizeByKey([]byte("тест123"))
	assert.Equal(t, 12, int(offset))
	assert.Equal(t, 34, int(size))

	offset, size = fat.GetOffsetSizeByKey([]byte("very_long_term_abc123"))
	assert.Equal(t, 102, int(offset))
	assert.Equal(t, 23, int(size))
}

func TestFastAccessTable_Iterator(t *testing.T) {
	opts := []StorageOpt{
		OptCompressKey(false), OptMaxKeyAlphaPower(33),
		OptMaxKeySize(24),
	}
	fat := NewFastAccessTable(opts...)

	err := fat.AddKeyOffset([]byte("test"), 125, 19)
	if err != nil {
		t.Fatal(err)
	}
	err = fat.AddKeyOffset([]byte("тест"), 144, 32)
	if err != nil {
		t.Fatal(err)
	}
	err = fat.AddKeyOffset([]byte("word123"), 12, 34)
	if err != nil {
		t.Fatal(err)
	}
	err = fat.AddKeyOffset([]byte("слово123"), 46, 56)
	if err != nil {
		t.Fatal(err)
	}
	err = fat.AddKeyOffset([]byte("очень_длинное_слово_123"), 102, 23)
	if err != nil {
		t.Fatal(err)
	}

	fat.PreloadKeysIntoMap()

	fat.IterBegin()
	assert.True(t, fat.IterNext())
	key, offset, size := fat.IterValue()
	assert.Equal(t, "test", key)
	assert.Equal(t, 125, int(offset))
	assert.Equal(t, 19, int(size))

	assert.True(t, fat.IterNext())
	key, offset, size = fat.IterValue()
	assert.Equal(t, "word123", key)
	assert.Equal(t, 12, int(offset))
	assert.Equal(t, 34, int(size))

	assert.True(t, fat.IterNext())
	key, offset, size = fat.IterValue()
	assert.Equal(t, "тест", key)
	assert.Equal(t, 144, int(offset))
	assert.Equal(t, 32, int(size))

	assert.True(t, fat.IterNext())
	key, offset, size = fat.IterValue()
	assert.Equal(t, "слово123", key)
	assert.Equal(t, 46, int(offset))
	assert.Equal(t, 56, int(size))

	assert.True(t, fat.IterNext())
	key, offset, size = fat.IterValue()
	assert.Equal(t, "очень_длинное_слово_123", key)
	assert.Equal(t, 102, int(offset))
	assert.Equal(t, 23, int(size))

	assert.False(t, fat.IterNext())
}

/*
func TestFastAccessTable_GetBytes(t *testing.T) {
	opts := []StorageOpt{
		OptCompressKey(true), OptMaxKeyAlphaPower(33),
		OptMaxKeySize(24),
	}
	fat := NewFastAccessTable(opts...)

	err := fat.AddKeyOffset([]byte("ab"), 125, 19)
	if err != nil {
		t.Fatal(err)
	}
	err = fat.AddKeyOffset([]byte("cd"), 144, 32)
	if err != nil {
		t.Fatal(err)
	}
	err = fat.AddKeyOffset([]byte("term1"), 12, 34)
	if err != nil {
		t.Fatal(err)
	}
	err = fat.AddKeyOffset([]byte("term2"), 46, 56)
	if err != nil {
		t.Fatal(err)
	}
	err = fat.AddKeyOffset([]byte("very_long_term_abc123"), 102, 23)
	if err != nil {
		t.Fatal(err)
	}

	err = fat.AddKeyOffset([]byte("тест123"), 12, 34)
	if err != nil {
		t.Fatal(err)
	}

	expected := []byte{
		0, 0, 0, 69,
		91, 123, 34, 110, 97, 109, 101, 34, 58, 50, 44, 34, 118, 97, 108, 117,
		101, 34, 58, 116, 114, 117, 101, 125, 44, 123, 34, 110, 97, 109, 101,
		34, 58, 48, 44, 34, 118, 97, 108, 117, 101, 34, 58, 51, 51, 125, 44,
		123, 34, 110, 97, 109, 101, 34, 58, 48, 44, 34, 118, 97, 108, 117, 101,
		34, 58, 50, 52, 125, 93,

		2, 0, 0, 0, 17, 0, 0, 0, 140, 0, 0, 0, 157, 0, 0, 0, 28, 1, 96, 18, 74,
		18, 116, 184, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 125, 0, 0,
		0, 19, 1, 96, 18, 74, 18, 116, 184, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 144, 0, 0, 0, 32, 1, 96, 151, 206, 223, 191, 87, 4, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 34, 1, 96, 151, 206,
		223, 191, 87, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 46, 0, 0,
		0, 56, 1, 96, 151, 208, 209, 161, 68, 105, 1, 96, 151, 208, 209, 154, 2,
		224, 0, 0, 0, 0, 0, 0, 0, 102, 0, 0, 0, 23, 0, 185, 44, 69, 106, 104, 67,
		70, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 34,
	}

	assert.True(t, bytes.Equal(expected, fat.GetBytes()))
}

func TestFastAccessTable_LoadBytes(t *testing.T) {
	fat := NewFastAccessTable()

	fat.LoadBytes([]byte{
		0, 0, 0, 69,
		91, 123, 34, 110, 97, 109, 101, 34, 58, 50, 44, 34, 118, 97, 108, 117,
		101, 34, 58, 116, 114, 117, 101, 125, 44, 123, 34, 110, 97, 109, 101,
		34, 58, 48, 44, 34, 118, 97, 108, 117, 101, 34, 58, 51, 51, 125, 44,
		123, 34, 110, 97, 109, 101, 34, 58, 48, 44, 34, 118, 97, 108, 117, 101,
		34, 58, 50, 52, 125, 93,

		2, 0, 0, 0, 17, 0, 0, 0, 140, 0, 0, 0, 157, 0, 0, 0, 28, 1, 96, 18, 74,
		18, 116, 184, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 125, 0, 0,
		0, 19, 1, 96, 18, 74, 18, 116, 184, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 144, 0, 0, 0, 32, 1, 96, 151, 206, 223, 191, 87, 4, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 34, 1, 96, 151, 206,
		223, 191, 87, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 46, 0, 0,
		0, 56, 1, 96, 151, 208, 209, 161, 68, 105, 1, 96, 151, 208, 209, 154, 2,
		224, 0, 0, 0, 0, 0, 0, 0, 102, 0, 0, 0, 23, 0, 185, 44, 69, 106, 104, 67,
		70, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 34,
	})

	assert.Len(t, fat.opts, 3)
	assert.Equal(t, fat.opts[0].Value, true)
	assert.Equal(t, fat.opts[1].Value, float64(33))
	assert.Equal(t, fat.opts[2].Value, float64(24))

	offset, size := fat.GetOffsetSizeByKey([]byte("term2"))
	assert.Equal(t, 46, int(offset))
	assert.Equal(t, 56, int(size))

	offset, size = fat.GetOffsetSizeByKey([]byte("doesnt_exist"))
	assert.Equal(t, 0, int(offset))
	assert.Equal(t, 0, int(size))

	offset, size = fat.GetOffsetSizeByKey([]byte("тест123"))
	assert.Equal(t, 12, int(offset))
	assert.Equal(t, 34, int(size))
}
*/

func TestDocInfoReader_Read(t *testing.T) {
	indexName := "test_index"
	s := NewFileSystemStorage()

	opts := []StorageOpt{
		OptCompressKey(true), OptMaxKeyAlphaPower(33),
		OptMaxKeySize(24), OptFilesPath("/tmp/index"),
		OptMarshaller(NewDocInfoMarshaller()), OptStoreInMemory(false),
	}
	err := s.StartCreatingIndex(indexName, opts...)
	if err != nil {
		t.Fatal(err)
	}

	info2 := DocInfo{
		DocId:     2,
		Positions: []int{1, 3, 5, 7},
	}
	err = s.UpdateIndex(indexName, TermData{"term1", info2})
	if err != nil {
		t.Fatal(err)
	}

	info1 := DocInfo{
		DocId:     1,
		Positions: []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	err = s.UpdateIndex(indexName, TermData{"term1", info1})
	if err != nil {
		t.Fatal(err)
	}

	if err = s.FinishCreatingIndex(indexName); err != nil {
		t.Fatal(err)
	}

	fat := s.FAT(indexName)
	offset, size := fat.GetOffsetSizeByKey([]byte("term1"))
	_, indexFile, err := s.getIndexFiles(indexName)
	if err != nil {
		t.Fatal(err)
	}
	_, err = indexFile.Seek(int64(offset), 0)
	if err != nil {
		t.Fatal(err)
	}

	reader := NewReader(indexFile, int(size), DefaultDocInfoMarshaller).WithChunkSize(16)
	infoItems, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	assert.Nil(t, infoItems)

	infoItems, err = reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	// Должны вернуться данные по документу с ID 1, т.к. они должны сортироваться.
	assert.Equal(t, 1, len(infoItems))
	assert.Equal(t, 1, infoItems[0].(*DocInfo).DocId)
	assert.Equal(t, info1.Positions, infoItems[0].(*DocInfo).Positions)

	infoItems, err = reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(infoItems))
	assert.Equal(t, 2, infoItems[0].(*DocInfo).DocId)
	assert.Equal(t, info2.Positions, infoItems[0].(*DocInfo).Positions)

	infoItems, err = reader.Read()
	assert.True(t, errors.Is(err, io.EOF))
}

func TestDocInfoIterator(t *testing.T) {
	indexName := "test_index"
	s := NewFileSystemStorage()

	opts := []StorageOpt{
		OptCompressKey(true), OptMaxKeyAlphaPower(33),
		OptMaxKeySize(24), OptFilesPath("/tmp/index"),
		OptMarshaller(NewDocInfoMarshaller()),
	}
	err := s.StartCreatingIndex(indexName, opts...)
	if err != nil {
		t.Fatal(err)
	}

	info2 := DocInfo{
		DocId:     2,
		Positions: []int{1, 3, 5, 7},
	}
	err = s.UpdateIndex(indexName, TermData{"term1", info2})
	if err != nil {
		t.Fatal(err)
	}
	info3 := DocInfo{
		DocId:     4,
		Positions: []int{2, 4, 6, 8, 18},
	}
	err = s.UpdateIndex(indexName, TermData{"term1", info3})
	if err != nil {
		t.Fatal(err)
	}
	info1 := DocInfo{
		DocId:     1,
		Positions: []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	err = s.UpdateIndex(indexName, TermData{"term1", info1})
	if err != nil {
		t.Fatal(err)
	}
	info4 := DocInfo{
		DocId:     1,
		Positions: []int{1, 2, 3},
	}
	err = s.UpdateIndex(indexName, TermData{"term2", info4})
	if err != nil {
		t.Fatal(err)
	}

	if err = s.FinishCreatingIndex(indexName); err != nil {
		t.Fatal(err)
	}

	fat := s.FAT(indexName)
	offset, size := fat.GetOffsetSizeByKey([]byte("term1"))
	_, indexFile, err := s.getIndexFiles(indexName)
	if err != nil {
		t.Fatal(err)
	}
	_, err = indexFile.Seek(int64(offset), 0)
	if err != nil {
		t.Fatal(err)
	}

	reader := NewReader(indexFile, int(size), DefaultDocInfoMarshaller).WithChunkSize(16)
	iter := NewIterator(reader)
	result := make([]*DocInfo, 0)
	for iter.Next() {
		result = append(result, iter.Value().(*DocInfo))
	}

	assert.Equal(t, 3, len(result))
	assert.True(t, info1.Equal(result[0]))
	assert.True(t, info2.Equal(result[1]))
	assert.True(t, info3.Equal(result[2]))
}

func TestFiltersIterator(t *testing.T) {
	indexName := "test_filters_index"
	s := NewFileSystemStorage()

	opts := []StorageOpt{
		OptCompressKey(false), OptMaxKeyAlphaPower(33),
		OptMaxKeySize(50), OptFilesPath("/tmp/index"),
		OptMarshaller(NewFilterMarshaller()),
	}
	err := s.StartCreatingIndex(indexName, opts...)
	if err != nil {
		t.Fatal(err)
	}

	data1 := FilterData{
		DocId: 1,
		Value: "value2",
	}
	err = s.UpdateIndex(indexName, data1)
	if err != nil {
		t.Fatal(err)
	}
	data3 := FilterData{
		DocId: 3,
		Value: "value1",
	}
	err = s.UpdateIndex(indexName, data3)
	if err != nil {
		t.Fatal(err)
	}
	data2 := FilterData{
		DocId: 2,
		Value: "value2",
	}
	err = s.UpdateIndex(indexName, data2)
	if err != nil {
		t.Fatal(err)
	}
	data4 := FilterData{
		DocId: 2,
		Value: "value1",
	}
	err = s.UpdateIndex(indexName, data4)
	if err != nil {
		t.Fatal(err)
	}
	data5 := FilterData{
		DocId: 1,
		Value: "value3",
	}
	err = s.UpdateIndex(indexName, data5)
	if err != nil {
		t.Fatal(err)
	}

	if err = s.FinishCreatingIndex(indexName); err != nil {
		t.Fatal(err)
	}

	fat := s.FAT(indexName)
	result := make([]FilterData, 0)
	for fat.IterBegin(); fat.IterNext(); {
		filterValue, _, _ := fat.IterValue()
		dataIter, err := s.GetKeyData(indexName, filterValue)
		if err != nil {
			t.Fatal(err)
		}
		for dataIter.Next() {
			result = append(result, FilterData{
				Value: filterValue,
				DocId: dataIter.Value().(int),
			})
		}
	}

	assert.Equal(
		t,
		[]FilterData{
			{"value1", 2}, {"value1", 3},
			{"value2", 1}, {"value2", 2},
			{"value3", 1},
		},
		result,
	)
}
