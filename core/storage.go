package core

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path"
	"strings"
	"sync"
)

const (
	extKeyFile   = ".key"
	extIndexFile = ".index"
	extBackup    = ".bak"
)

var (
	offsetsOfOtherSymbols = map[rune]byte{
		'0': 1, '1': 2, '2': 3, '3': 4, '4': 5, '5': 6, '6': 7, '7': 8, '8': 9,
		'9': 10,
		'_': 11, '#': 12, '-': 13, '+': 14, '/': 15, '*': 16, '&': 17, ' ': 18,
		'\'': 19, '"': 20, '|': 21,
	}
	otherSymbolsByOffsets = map[byte]rune{}
	defaultErrorHandler   = func(err error) { log.Println(err) }
)

func init() {
	for symbol, offset := range offsetsOfOtherSymbols {
		otherSymbolsByOffsets[offset] = symbol
	}
}

type StorageOpt struct {
	Name  int         `json:"name"`
	Value interface{} `json:"value"`
}

const (
	NameOptMaxKeySize = iota
	NameOptMaxKeyAlphaPower
	NameOptCompressKey
	NameOptMarshaller
	NameOptFilesPath
	NameOptStoreInMemory
)

func OptMaxKeySize(value int) StorageOpt {
	if value <= 0 {
		value = 24
	}
	return StorageOpt{Name: NameOptMaxKeySize, Value: value}
}

func OptMaxKeyAlphaPower(value int) StorageOpt {
	if value <= 0 {
		value = 33
	}
	return StorageOpt{Name: NameOptMaxKeySize, Value: value}
}

func OptCompressKey(value bool) StorageOpt {
	return StorageOpt{Name: NameOptCompressKey, Value: value}
}

func OptMarshaller(value Marshaller) StorageOpt {
	return StorageOpt{Name: NameOptMarshaller, Value: value}
}

func OptFilesPath(value string) StorageOpt {
	return StorageOpt{Name: NameOptFilesPath, Value: value}
}

func OptStoreInMemory(value bool) StorageOpt {
	return StorageOpt{Name: NameOptStoreInMemory, Value: value}
}

// Marshaller описывает структуру данных, которая преобразует данные другой
// структуры для ее сохранения в индексе.
type Marshaller interface {
	// Bytes преобразовывает item в бинарный формат для сохранения в файл
	// индекса.
	ToBytes(item interface{}) ([]byte, error)
	// Parse на основе куска данных data возвращает список элементов items
	// остаточные данные unparsed (которые не были полными для парсинга) и ошибку.
	Parse(data []byte) (items []interface{}, unparsed []byte, err error)
	// SortKey возвращает данные ключа сортировки конкретного элемента item.
	// Ключ сортировки - это порядок того, как данные элемента item будут
	// располагаться в индексе.
	SortKey(item interface{}) (sortKey []byte, err error)
	// Key на основе данных ключа сортировки возвращает ключ, по которому будут
	// группироваться данные в индексе, и по которому будет происходить поиск.
	Key(softKey []byte) (key string, err error)
}

// Storage описывает интерфейс хранилища для данных поискового индекса.
type Storage interface {
	// SetErrorHandler устанавливает обработчик внутренних ошибок (которые по
	// определенным причинам не возращаются методами).
	SetErrorHandler(h func(error))
	// SetOptions устанавливает настройки хранения и оперирования индексными
	// файлами для индекса indexName.
	SetOptions(indexName string, opts ...StorageOpt)
	// StartCreatingIndex инициализирует создание индекса name для дальнейшего
	// наполнения через UpdateIndex.
	StartCreatingIndex(name string, opts ...StorageOpt) error
	// UpdateIndex обновляет индекс indexName данными элемента item.
	UpdateIndex(indexName string, item interface{}) error
	// FinishCreatingIndex завершает создание индекса name.
	FinishCreatingIndex(name string) error
	// GetKeyData возвращает итератор по данным для ключа key из индекса indexName.
	GetKeyData(indexName string, key string) (*Iterator, error)
	// DeleteIndex удаляет индекс name.
	DeleteIndex(name string) error
	// FAT возвращает FastAccessTable для индекса indexName.
	FAT(indexName string) *FastAccessTable
}

// storageIndexState хранит состояние индекса для хранилища (маршаллер, путь
// сохранения файлов и пр.).
type storageIndexState struct {
	// Директория, в которой будут храниться файлы индексов и ключей.
	filesPath string
	// Сериализатор/парсер данных для/из индекса.
	marshaller Marshaller
	// Хранить ли индекс в оперативной памяти.
	storeInMemory bool
}

func defaultStorageIndexState() storageIndexState {
	return storageIndexState{
		filesPath:     ".",
		marshaller:    NewDocInfoMarshaller(),
		storeInMemory: false,
	}
}

// FileSystemStorage реализует Storage через хранение данных в файлах на файловой
// системе. Для каждого индекса создается 2 файла - файл ключей и файл самого индекса.
// В файле индекса хранятся данные о позициях различных term'ов в различных документах.
// В файле ключей хранятся смещения (куда "прыгнуть") данных в файле индекса.
type FileSystemStorage struct {
	*sync.RWMutex
	// Курсоры на промежуточные key-value хранилища для временной аггрегации
	// данных о позициях term'ов в документах для разных индексов.
	indexDbs map[string]*badger.DB
	// Карта таблиц смещений в файле индекса для каждого ключа. Каждая таблица
	// формирует структуру для файла ключей и умеет работать с его данными
	// (искать по ним смещение конкретного term'а).
	fats map[string]*FastAccessTable
	// Предзагруженные данные индексов (для которых передавалась опция
	// OptStoreInMemory со значением true.
	indexes map[string][]byte
	// Карта состояний хранилища для разных индексов.
	states     map[string]storageIndexState
	errHandler func(error)
}

func NewFileSystemStorage() *FileSystemStorage {
	return &FileSystemStorage{
		RWMutex:    new(sync.RWMutex),
		indexDbs:   make(map[string]*badger.DB),
		fats:       make(map[string]*FastAccessTable),
		indexes:    make(map[string][]byte),
		states:     make(map[string]storageIndexState),
		errHandler: defaultErrorHandler,
	}
}

func (s *FileSystemStorage) handleError(err error) {
	if s.errHandler != nil {
		s.errHandler(err)
	}
}

func (s *FileSystemStorage) SetErrorHandler(h func(error)) {
	s.Lock()
	s.errHandler = h
	s.Unlock()
}

// prepareIndexName при необходимости корректирует название индекса, чтобы его
// можно было использовать в качестве названия директорий, файлов и тд.
func (s *FileSystemStorage) prepareIndexName(name string) string {
	name = strings.Replace(name, "..", "", -1)
	name = path.Clean(name)
	return name
}

// cleanUp чистит директорию временного хранилища.
func (s *FileSystemStorage) cleanUp(dirPaths ...string) error {
	for _, dirPath := range dirPaths {
		if exists, err := PathExists(dirPath); exists || err != nil {
			if err != nil {
				return err
			}
			if err := os.RemoveAll(dirPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *FileSystemStorage) tempDatabaseDir(indexName string) string {
	indexDbDir := path.Join("/tmp", indexName)
	return indexDbDir
}

func (s *FileSystemStorage) SetOptions(indexName string, opts ...StorageOpt) {
	indexState := defaultStorageIndexState()
	for _, opt := range opts {
		switch opt.Name {
		case NameOptFilesPath:
			filesPath := opt.Value.(string)
			_ = os.MkdirAll(filesPath, os.ModePerm)
			indexState.filesPath = filesPath

		case NameOptMarshaller:
			indexState.marshaller = opt.Value.(Marshaller)
		case NameOptStoreInMemory:
			indexState.storeInMemory = opt.Value.(bool)
		}
	}

	s.Lock()
	s.states[indexName] = indexState
	s.Unlock()
	// Предзагрузка дынных Fast Access Table.
	fat := s.FAT(indexName)
	s.Lock()
	s.fats[indexName] = fat
	s.Unlock()
}

func (s *FileSystemStorage) getIndexState(indexName string) storageIndexState {
	s.RLock()
	state, ok := s.states[indexName]
	s.RUnlock()
	if !ok {
		state = defaultStorageIndexState()
		s.Lock()
		s.states[indexName] = state
		s.Unlock()
	}
	return state
}

func (s *FileSystemStorage) StartCreatingIndex(name string, opts ...StorageOpt) error {
	name = s.prepareIndexName(name)
	s.SetOptions(name, opts...)

	indexDbDir := s.tempDatabaseDir(name)
	if err := s.cleanUp(indexDbDir); err != nil {
		return err
	}
	keyFilePath, indexFilePath := s.getIndexFilesPaths(name)

	if err := s.backupFile(keyFilePath); err != nil {
		return err
	}
	if err := s.backupFile(indexFilePath); err != nil {
		return err
	}

	if err := RecreateFile(keyFilePath); err != nil {
		return err
	}
	if err := RecreateFile(indexFilePath); err != nil {
		return err
	}

	indexDb, err := badger.Open(badger.DefaultOptions(indexDbDir))
	if err != nil {
		return err
	}
	s.Lock()
	s.indexDbs[name] = indexDb
	s.fats[name] = NewFastAccessTable(opts...)
	s.Unlock()

	return nil
}

func (s *FileSystemStorage) DeleteIndex(name string) error {
	name = s.prepareIndexName(name)
	keyFileName, indexFileName := s.getIndexFilesPaths(name)

	s.Lock()
	delete(s.states, name)
	delete(s.fats, name)
	delete(s.indexes, name)
	delete(s.indexDbs, name)
	s.Unlock()

	if err := os.Remove(keyFileName); err != nil {
		return err
	}
	if err := os.Remove(indexFileName); err != nil {
		return err
	}
	return s.cleanUp(s.tempDatabaseDir(name))
}

func (s *FileSystemStorage) UpdateIndex(indexName string, item interface{}) error {
	indexName = s.prepareIndexName(indexName)
	s.RLock()
	tmpDb, ok := s.indexDbs[indexName]
	s.RUnlock()
	if !ok {
		return fmt.Errorf("can't find temporary DB cursor for index %s", indexName)
	}

	state := s.getIndexState(indexName)
	sortKey, err := state.marshaller.SortKey(item)
	if err != nil {
		return err
	}
	itemData, err := state.marshaller.ToBytes(item)
	if err != nil {
		return err
	}

	err = tmpDb.Update(func(txn *badger.Txn) error {
		return txn.Set(sortKey, itemData)
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *FileSystemStorage) backupFile(filePath string) error {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil
	}
	return CopyFile(filePath, filePath+extBackup)
}

func (s *FileSystemStorage) getIndexFilesPaths(indexName string) (string, string) {
	state := s.getIndexState(indexName)
	keyFilePath := path.Join(state.filesPath, indexName+extKeyFile)
	indexFilePath := path.Join(state.filesPath, indexName+extIndexFile)
	return keyFilePath, indexFilePath
}

// getIndexFiles возвращает дескрипторы файлов ключей и индекса.
// Делает backup текущих файлов.
func (s *FileSystemStorage) getIndexFiles(indexName string) (*os.File, *os.File, error) {
	keyFilePath, indexFilePath := s.getIndexFilesPaths(indexName)

	keyFile, err := os.OpenFile(keyFilePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, nil, err
	}
	indexFile, err := os.OpenFile(indexFilePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, nil, err
	}

	return keyFile, indexFile, nil
}

// FAT возвращает Fast Access Table для индекса indexName. Если такой таблицы
// нет в карте таблиц, то она создается, и возвращается созданная.
func (s *FileSystemStorage) FAT(indexName string) *FastAccessTable {
	s.RLock()
	fat, ok := s.fats[indexName]
	s.RUnlock()
	if !ok {
		// Заполнение создаваемой FAT данными из файла ключей для этого индекса.
		keyFilePath, _ := s.getIndexFilesPaths(indexName)
		keyData, _ := ioutil.ReadFile(keyFilePath)
		fat = NewFastAccessTable().LoadBytes(keyData)
		s.Lock()
		s.fats[indexName] = fat
		s.Unlock()
	}
	return fat
}

func (s *FileSystemStorage) saveIndexData(name string, indexFile *os.File) error {
	_, err := indexFile.Seek(0, 0)
	if err != nil {
		return err
	}
	indexData, err := ioutil.ReadAll(indexFile)
	if err != nil {
		return err
	}
	s.Lock()
	s.indexes[name] = indexData
	s.Unlock()
	return nil
}

func (s *FileSystemStorage) FinishCreatingIndex(name string) error {
	name = s.prepareIndexName(name)
	s.RLock()
	tmpDb, ok := s.indexDbs[name]
	s.RUnlock()
	if !ok {
		return fmt.Errorf("can't find key-value DB cursor for index %s", name)
	}

	keyFile, indexFile, err := s.getIndexFiles(name)
	if err != nil {
		return err
	}

	tx := tmpDb.NewTransaction(false)
	it := tx.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	state := s.getIndexState(name)
	// Итерация по данным по данным term'ов и документов и сохранение их во
	// временную БД - по ключам term'ам.
	fat := s.FAT(name)
	totalWritten := 0
	keyOffset := 0
	prevKey := ""
	lastUpdatedKey := ""
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key, _ := state.marshaller.Key(item.Key())
		if prevKey == "" {
			prevKey = key
		}
		err := item.Value(func(docData []byte) error {
			n, err := indexFile.Write(docData)
			if err != nil {
				return err
			}
			// Запись в FAT смещения и размера данных для данного key'а.
			if prevKey != key {
				size := uint32(totalWritten - keyOffset)
				err = fat.AddKeyOffset([]byte(prevKey), uint64(keyOffset), size)
				if err != nil {
					s.handleError(err)
					return nil
				}
				keyOffset = totalWritten
				lastUpdatedKey = prevKey
			}
			totalWritten += n
			prevKey = key
			return nil
		})
		if err != nil {
			return err
		}
	}
	// Запись в FAT смещения и размера данных для последнего term'а.
	if lastUpdatedKey != prevKey {
		size := uint32(totalWritten - keyOffset)
		err = fat.AddKeyOffset([]byte(prevKey), uint64(keyOffset), size)
		if err != nil {
			s.handleError(err)
		}
	}
	fat.PreloadKeysIntoMap()

	// Сохранение данных индекса, если выставлена опция.
	if state.storeInMemory {
		if err := s.saveIndexData(name, indexFile); err != nil {
			return err
		}
	}
	// Сохранение смещений данных из FAT для разных языков в один файл ключей.
	if _, err := keyFile.Write(fat.GetBytes()); err != nil {
		return err
	}
	// Чистка временных файлов key-value хранилища.
	if err := s.cleanUp(s.tempDatabaseDir(name)); err != nil {
		s.handleError(err)
	}
	if err := keyFile.Close(); err != nil {
		return err
	}
	if err := indexFile.Close(); err != nil {
		return err
	}

	return nil
}

type bufferNopCloser struct {
	*bytes.Reader
}

func (b *bufferNopCloser) Close() error { return nil }

// getIndexSourceReader возвращает io.ReadSeeker в зависимости от опцией (либо
// дескриптор файла, либо bytes.Reader, если индекс хранится в памяти).
func (s *FileSystemStorage) getIndexSourceReader(name string) (io.ReadSeekCloser, error) {

	state := s.getIndexState(name)
	if state.storeInMemory {
		s.RLock()
		indexData, ok := s.indexes[name]
		s.RUnlock()
		if ok {
			return &bufferNopCloser{Reader: bytes.NewReader(indexData)}, nil
		}
	}
	_, indexFile, err := s.getIndexFiles(name)
	if err != nil {
		return nil, err
	}

	if state.storeInMemory {
		indexData, err := ioutil.ReadAll(indexFile)
		if err != nil {
			return nil, err
		}
		s.Lock()
		s.indexes[name] = indexData
		s.Unlock()
		return &bufferNopCloser{Reader: bytes.NewReader(indexData)}, nil
	}

	return indexFile, nil
}

func (s *FileSystemStorage) GetKeyData(indexName string, key string) (*Iterator, error) {
	indexName = s.prepareIndexName(indexName)
	fat := s.FAT(indexName)
	offset, size := fat.GetOffsetSizeByKey([]byte(key))

	indexReader, err := s.getIndexSourceReader(indexName)
	if err != nil {
		return nil, err
	}

	_, err = indexReader.Seek(int64(offset), 0)
	if err != nil {
		return nil, err
	}

	state := s.getIndexState(indexName)
	iter := NewIterator(NewReader(indexReader, int(size), state.marshaller))
	return iter, nil
}

type fatItemInfo struct {
	offset uint64
	size   uint32
}

// FastAccessTable реализует методы для формирования, сохранения и оперирования
// данными смещений (в индексном файле).
type FastAccessTable struct {
	Buffers         map[alphabetType]*bytes.Buffer
	keysMap         map[string]fatItemInfo
	opts            []StorageOpt
	maxKeySize      int
	maxKeyBytesSize int
	maxAlphaPower   int
	keyDwordsNum    int
	itemSize        int
	keyCompressing  bool

	alphabets       []alphabetType
	currAlphabetIdx int
	currOffset      int
}

func NewFastAccessTable(opts ...StorageOpt) *FastAccessTable {
	t := &FastAccessTable{
		Buffers: make(map[alphabetType]*bytes.Buffer),
		keysMap: make(map[string]fatItemInfo),
	}
	t.initWithOpts(opts)
	return t
}

// initWithOpts инициализирует внутренние значения таблицы на основе переданных
// опций.
func (t *FastAccessTable) initWithOpts(opts []StorageOpt) {
	t.opts = opts
	t.keyCompressing = true
	for _, opt := range t.opts {
		switch opt.Name {
		case NameOptMaxKeySize:
			t.maxKeySize = castInt(opt.Value)
		case NameOptMaxKeyAlphaPower:
			t.maxAlphaPower = castInt(opt.Value)
		case NameOptCompressKey:
			t.keyCompressing = opt.Value.(bool)
		}
	}

	if t.maxKeySize == 0 {
		t.maxKeySize = 16
	}
	if t.maxAlphaPower == 0 {
		t.maxAlphaPower = 33
	}

	if t.keyCompressing {
		// Количество двойных слов, в которые гарантированно может быть закодированы
		// данные ключа в алфавите мощности t.maxAlphaPower равно:
		// ceil( log_2(t.maxAlphaPower^t.maxKeySize) / 64 )
		keysCount := math.Pow(float64(t.maxAlphaPower), float64(t.maxKeySize))
		t.keyDwordsNum = int(math.Ceil(math.Log2(keysCount) / 64))
		t.maxKeyBytesSize = t.keyDwordsNum * 8

	} else {
		t.maxKeyBytesSize = t.maxKeySize
	}
	// байты на кодированный ключ + 8 байт для смещения к ключу в файле
	// индекса + 4 байта для размера данных по ключу в индексе.
	t.itemSize = t.maxKeyBytesSize + 8 + 4
}

// encodeOpts сериализует опции.
func (t *FastAccessTable) encodeOpts() []byte {
	optsBytes, err := json.Marshal(t.opts)
	if err != nil {
		panic(fmt.Errorf("can't encode opts: %w", err))
	}
	optsBytesLen := len(optsBytes)
	result := make([]byte, optsBytesLen+4)
	binary.BigEndian.PutUint32(result, uint32(optsBytesLen))
	copy(result[4:], optsBytes)

	return result
}

// decodeOpts десереализует опции в слайс.
func (t *FastAccessTable) decodeOpts(data []byte) (opts []StorageOpt, readN int) {
	bufLen := int(binary.BigEndian.Uint32(data))
	if bufLen == 0 {
		return opts, bufLen
	}
	opts = make([]StorageOpt, 0)
	err := json.Unmarshal(data[4:4+bufLen], &opts)
	if err != nil {
		panic(fmt.Errorf("can't decode FAT options: %w", err))
	}
	return opts, bufLen
}

// GetBytes формирует единый буффер с данными для всех языков, внедряя в начало
// их смещения в едином буфере.
func (t *FastAccessTable) GetBytes() []byte {
	// 8 байт для каждого языка (смещение + размер) + 1 байт для их количества.
	langOffsets := make([]byte, len(t.Buffers)*8+1)
	// Первый байт - количество языков.
	langOffsets[0] = byte(len(t.Buffers))

	dataBuf := bytes.NewBuffer(nil)
	currOffset := len(langOffsets)
	for lang, buf := range t.Buffers {
		// Сразу заполняется буффер с данными для языков.
		dataBuf.Write(buf.Bytes())
		// Заполнение смещения данных языка.
		binary.BigEndian.PutUint32(langOffsets[8*(lang-1)+1:], uint32(currOffset))
		// Заполнение размера данных языка.
		binary.BigEndian.PutUint32(langOffsets[8*(lang-1)+5:], uint32(buf.Len()))
		// Обновление смещения, учитывая все предыдущие данные.
		currOffset += dataBuf.Len()
	}

	// Сериализованные данные опций.
	optsBytes := t.encodeOpts()

	// Объединение буфера смещений для языков с данными языков.
	totalBuf := make([]byte, len(optsBytes)+len(langOffsets)+dataBuf.Len())
	copy(totalBuf, optsBytes)
	copy(totalBuf[len(optsBytes):], langOffsets)
	copy(totalBuf[len(optsBytes)+len(langOffsets):], dataBuf.Bytes())

	return totalBuf
}

// PreloadKeysIntoMap предзагружает данные ключей (смещения и размеры данных в
// индексе) в карту для более быстрого доступа в будущем.
func (t *FastAccessTable) PreloadKeysIntoMap() {
	for t.IterBegin(); t.IterNext(); {
		key, offset, size := t.IterValue()
		t.keysMap[key] = fatItemInfo{
			offset: offset,
			size:   size,
		}
	}
}

// LoadBytes выгружает данные data (полученные из GetBytes) в буфферы языков.
func (t *FastAccessTable) LoadBytes(data []byte) *FastAccessTable {
	self := t
	if len(data) <= 5 {
		return self
	}
	// Предварительная очистка.
	t.Buffers = make(map[alphabetType]*bytes.Buffer)

	// Декодирование сохраненных опций.
	opts, optsSize := t.decodeOpts(data)
	t.initWithOpts(opts)
	data = data[4+optsSize:]

	// Первый байт - количество языковых данных.
	langsNum := int(data[0])
	for lang := 1; lang <= langsNum; lang++ {
		langOffset := binary.BigEndian.Uint32(data[8*(lang-1)+1:])
		langSize := binary.BigEndian.Uint32(data[8*(lang-1)+5:])
		langValue := alphabetType(lang)
		t.Buffers[langValue] = bytes.NewBuffer(data[langOffset : langOffset+langSize])
	}

	t.PreloadKeysIntoMap()

	return self
}

// getBuffer возвращает буффер для конкретного термина по его алфавиту.
func (t *FastAccessTable) getBuffer(s []rune) *bytes.Buffer {
	alpha := getAlphabet(s)
	if alpha == alphabetUnknown {
		return nil
	}
	buf, ok := t.Buffers[alpha]
	if !ok {
		buf = bytes.NewBuffer(nil)
		t.Buffers[alpha] = buf
	}
	return buf
}

// fitKeyBytes обрезает или добавляет нулевые байты, чтобы key стал фиксированной
// длины.
func (t *FastAccessTable) fitKeyBytes(key []byte) []byte {
	if len(key) > t.maxKeyBytesSize {
		key = key[:t.maxKeyBytesSize]
	} else if len(key) < t.maxKeyBytesSize {
		newKey := make([]byte, t.maxKeyBytesSize)
		copy(newKey, key)
		key = newKey
	}

	return key
}

// convertKeyToBytes преобразовывает слайс рун в слайс байт на основе того,
// нужно ли применять алгоритм сжатия.
func (t *FastAccessTable) convertKeyToBytes(key []rune) ([]byte, error) {
	if t.keyCompressing {
		result := make([]byte, t.keyDwordsNum*8)
		keyNums, err := runesToIntegers(key)
		if err != nil {
			return nil, err
		}
		for i, num := range keyNums {
			if i >= t.keyDwordsNum {
				break
			}
			binary.BigEndian.PutUint64(result[i*8:], num)
		}
		return result, nil
	} else {
		return runesToBytes(key)
	}
}

// getKeyOffsetInData возвращает смещение term'а в данных data. Использует
// алгоритм бинарного поиска, т.к. term'ы отсортированы.
// Если совпадений не было, вернет -1.
func (t *FastAccessTable) getKeyOffsetInData(key []rune, data []byte) int {
	// Проверка на корректность данных (должны быть выравнены по размеру элемента
	// данных таблицы).
	if len(data)%t.itemSize != 0 {
		return -1
	}

	// key кодируется в байты фиксированной длины.
	keyBytes, err := t.convertKeyToBytes(key)
	if err != nil {
		return -1
	}
	keyBytes = t.fitKeyBytes(keyBytes)

	minOffset := 0
	maxOffset := len(data) - t.itemSize
	for maxOffset >= minOffset {
		currOffset := (minOffset + maxOffset) / 2
		// выравнивание по t.itemSize
		currOffset -= currOffset % t.itemSize
		currBytes := data[currOffset : currOffset+t.maxKeyBytesSize]
		switch bytes.Compare(currBytes, keyBytes) {
		case 0:
			return currOffset
		case 1:
			maxOffset = currOffset - t.itemSize
		case -1:
			minOffset = currOffset + t.itemSize
		}
	}

	return -1
}

// AddKeyOffset добавляет смещение term'а termOffsetInIndex и размер его данных
// в индексном файле size к основным данным в таблице.
// Важно добавлять term'ы в алфавитном порядке!
func (t *FastAccessTable) AddKeyOffset(
	key []byte, termOffsetInIndex uint64, size uint32,
) error {
	runes := bytes.Runes(key)
	// key кодируется в байты фиксированной длины.
	keyBytes, err := t.convertKeyToBytes(runes)
	if err != nil {
		return err
	}
	keyBytes = t.fitKeyBytes(keyBytes)

	buf := t.getBuffer(runes)
	if buf == nil {
		return fmt.Errorf("can't get buffer for key \"%s\"", string(key))
	}
	if buf.Cap() < buf.Len()+t.itemSize {
		buf.Grow(buf.Len() * 2)
	}

	tmp := make([]byte, t.itemSize)
	copy(tmp, keyBytes)
	binary.BigEndian.PutUint64(tmp[t.maxKeyBytesSize:], termOffsetInIndex)
	binary.BigEndian.PutUint32(tmp[t.maxKeyBytesSize+8:], size)
	buf.Write(tmp)

	t.keysMap[string(key)] = fatItemInfo{
		offset: termOffsetInIndex,
		size:   size,
	}

	return nil
}

func (t *FastAccessTable) getKeyOffsetSize(buf []byte) (uint64, uint32) {
	offset := binary.BigEndian.Uint64(buf)
	size := binary.BigEndian.Uint32(buf[8:])
	return offset, size
}

// GetOffsetSizeByKey возвращает смещение key'а в индексном файле.
// Если совпадение не найдено, то вернет 0, 0.
func (t *FastAccessTable) GetOffsetSizeByKey(key []byte) (uint64, uint32) {
	fatItemInfo := t.keysMap[string(key)]
	return fatItemInfo.offset, fatItemInfo.size

	//runes := bytes.Runes(key)
	//buf := t.getBuffer(runes).Bytes()
	//innerOffset := t.getKeyOffsetInData(runes, buf)
	//if innerOffset == -1 {
	//	return 0, 0
	//}
	//return t.getKeyOffsetSize(buf[innerOffset+t.maxKeyBytesSize:])
}

// IterBegin сбрасывает состояние итератора.
func (t *FastAccessTable) IterBegin() {
	t.currOffset = 0
	t.currAlphabetIdx = 0
	t.alphabets = make([]alphabetType, 0)
	for alpha := range t.Buffers {
		t.alphabets = append(t.alphabets, alpha)
	}
}

// IterNext возвращает true, пока можно итерироваться.
func (t *FastAccessTable) IterNext() bool {
	// TODO: Добавить поддержку итерации по сжатым ключам.
	if len(t.alphabets) == 0 {
		return false
	}
	currAlphabet := t.alphabets[t.currAlphabetIdx]
	buf := t.Buffers[currAlphabet]

	if t.currOffset >= buf.Len() {
		t.currAlphabetIdx++
		if t.currAlphabetIdx >= len(t.alphabets) {
			return false
		}
		currAlphabet = t.alphabets[t.currAlphabetIdx]
		buf = t.Buffers[currAlphabet]
		t.currOffset = 0
	}

	return true
}

// IterValue возвращает текущее значение итератора, смещение и размер данных в
// индексе.
func (t *FastAccessTable) IterValue() (key string, offset uint64, size uint32) {
	currAlphabet := t.alphabets[t.currAlphabetIdx]
	buf := t.Buffers[currAlphabet].Bytes()

	keyBytes := buf[t.currOffset : t.currOffset+t.maxKeyBytesSize]
	runes, _ := bytesToRunes(keyBytes, currAlphabet)
	key = string(runes)
	offset, size = t.getKeyOffsetSize(buf[t.currOffset+t.maxKeyBytesSize:])

	t.currOffset += t.itemSize

	return key, offset, size
}
