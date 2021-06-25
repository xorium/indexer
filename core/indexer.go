package core

import (
	si "bitbucket.org/entrlcom/genproto/gen/go/search/indexer/v1"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/sajari/fuzzy"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	IndexStatusCreating IndexStatus = iota
	IndexStatusReady
)

var (
	emptyStruct          = struct{}{}
	DefaultSearchOptions = SearchOptions{
		TopN:             10,
		Filters:          make(SearchFilters),
		WithFiltersStats: true,
	}
)

func formatTermsAndFilters(terms []string, filters SearchFilters) string {
	result := fmt.Sprintf("%s", terms)
	if len(filters) > 0 {
		result = fmt.Sprintf("%s (%s)", result, filters.String())
	}
	return result
}

type SearchResult struct {
	Docs           []DocID
	FilterStats    map[FilterName]map[FilterValue]int
	SuggestedTerms []string
}

type SearchFilters map[FilterName][]FilterValue

func (f SearchFilters) String() string {
	filtersQuery := url.Values{}
	for filterName, filterValues := range f {
		for _, filterValue := range filterValues {
			filtersQuery.Set(string(filterName), string(filterValue))
		}
	}
	return filtersQuery.Encode()
}

type SearchOptions struct {
	TopN             int
	Filters          SearchFilters
	WithFiltersStats bool
}

type IndexStatus int

type indexState struct {
	*sync.RWMutex
	// Статус (в каком состоянии находится в данный момент).
	status IndexStatus
	// Настройки индекса.
	settings IndexSettings
	// Предзагруженные документы, согласно всем возможным значениям всех фильтров
	// данного индекса.
	preloadedFilters map[FilterName]map[FilterValue]map[DocID]struct{}
	// Префиксное дерево для автодополнения вводимых term'ов.
	trie *Trie
	// Модель для автокоррекции опечаток в term'ах.
	spellCorrector *fuzzy.Model
}

func newIndexState(s IndexSettings) *indexState {
	spellCorrector := fuzzy.NewModel()
	spellCorrector.SetDepth(2)
	spellCorrector.SetThreshold(1)

	return &indexState{
		RWMutex:          new(sync.RWMutex),
		status:           IndexStatusReady,
		settings:         s,
		preloadedFilters: make(map[FilterName]map[FilterValue]map[DocID]struct{}),
		trie:             NewTrie(),
		spellCorrector:   spellCorrector,
	}
}

func (s *indexState) SetFilters(
	filterFieldName FilterName, filtersData map[FilterValue]map[DocID]struct{},
) {
	s.Lock()
	s.preloadedFilters[filterFieldName] = filtersData
	s.Unlock()
}

// ProcessTerm загружает данные term'а в необходимые структуры: префиксное дерево
// для реализации автодополнения, модель автокоррекции при опечатках.
func (s *indexState) ProcessTerm(term string) {
	s.Lock()
	s.trie.Insert([]byte(term), []byte{})
	s.spellCorrector.TrainWord(term)
	s.Unlock()
}

// GetTermsByPrefix возвращает количество term'ов, начинающихся с prefix и
// ограниченное числом limit.
func (s *indexState) GetTermsByPrefix(prefix string, limit int) []string {
	s.RLock()
	defer s.RUnlock()
	keysInBytes := s.trie.GetPrefixKeys([]byte(prefix))

	keys := make([]string, 0)
	for _, keyBytes := range keysInBytes {
		if len(keyBytes) < len(prefix) {
			continue
		}
		keys = append(keys, string(keyBytes))
		if len(keys) >= limit {
			break
		}
	}

	return keys
}

func (s *indexState) CorrectSpell(term string) (correctedTerm string) {
	s.RLock()
	defer s.RUnlock()
	return s.spellCorrector.SpellCheck(term)
}

type IndexSettings struct {
	IdentifierField string   `json:"identifier_field"`
	FilteringFields []string `json:"filtering_fields"`
	IndexingFields  []string `json:"indexing_fields"`
	PreloadFilters  bool     `json:"preload_filters"`
}

type Indexer struct {
	*sync.RWMutex
	Debug   bool
	workDir string
	// Хранилище данных индексов (инвертированных).
	storage Storage
	// Поисковый движок, реализующий логику расчета релевантности, ранжирования
	// и фильтрации документов.
	engine *SearchEngine
	// Key-value база данных для хранения метаинформации индексов (настройки и пр.).
	metaStorage *badger.DB
	// Состояния индексов (индикаторные данных, настройки и т.п.).
	states map[string]*indexState
	// Обработчик текстов документов и запросов (токенизирует, лемматизирует и
	// нумерует).
	processor *TextProcessor
}

func NewManager(workDir string, indicesDataStorage Storage) (*Indexer, error) {
	engine := NewSearchEngine(indicesDataStorage)
	m := &Indexer{
		RWMutex:   new(sync.RWMutex),
		storage:   indicesDataStorage,
		states:    make(map[string]*indexState),
		processor: NewDocumentProcessor(),
		engine:    engine.WithRelevantFunc(RelevantFuncWeightedLinear),
	}
	m.setGracefullyClosing()
	if err := m.setWorkDir(workDir); err != nil {
		return nil, err
	}
	if err := m.preloadIndicesData(); err != nil {
		return nil, err
	}

	return m, nil
}

func (ix *Indexer) gracefullyClose() {
	ix.Lock()
	if ix.metaStorage != nil {
		_ = ix.metaStorage.Close()
	}
	ix.Unlock()
}

func (ix *Indexer) setGracefullyClosing() {
	signals := make(chan os.Signal, 1)
	signal.Notify(
		signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL,
	)
	go func() {
		<-signals
		ix.gracefullyClose()
	}()
}

func (ix *Indexer) Storage() Storage {
	return ix.storage
}

// preloadIndicesData загружает настройки индексов в память и загружает необходимые
// данные индексов (например, данные фильтров), если это было включено в настройках.
func (ix *Indexer) preloadIndicesData() error {
	tx := ix.metaStorage.NewTransaction(false)
	it := tx.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	ix.Lock()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		indexName := string(item.Key())
		err := item.Value(func(settingsData []byte) error {
			settings := new(IndexSettings)
			if err := json.Unmarshal(settingsData, settings); err != nil {
				return err
			}
			ix.states[indexName] = newIndexState(*settings)
			return nil
		})
		if err != nil {
			return err
		}
	}
	ix.Unlock()

	for indexName, indexState := range ix.states {
		for _, searchFieldName := range indexState.settings.IndexingFields {
			searchIndexName := ix.searchName(indexName, searchFieldName)
			opts := []StorageOpt{
				OptMaxKeyAlphaPower(33),
				OptCompressKey(false),
				OptMaxKeySize(24),
				OptFilesPath(path.Join(ix.workDir, searchIndexName)),
				OptMarshaller(NewDocInfoMarshaller()),
			}
			ix.storage.SetOptions(searchIndexName, opts...)
		}
		for _, filterFieldName := range indexState.settings.FilteringFields {
			filterIndexName := ix.filterName(indexName, filterFieldName)
			opts := []StorageOpt{
				OptMaxKeyAlphaPower(33),
				OptCompressKey(false),
				OptMaxKeySize(50),
				OptFilesPath(path.Join(ix.workDir, filterIndexName)),
				OptMarshaller(NewFilterMarshaller()),
			}
			ix.storage.SetOptions(filterIndexName, opts...)
		}

		if indexState.settings.PreloadFilters {
			ix.preloadTermsData(indexName, indexState)
		}
	}

	return nil
}

// setWorkDir создает необходимую иерархию директорий для хранения как данных
// самого индекса, так и его метаданных (настройки и пр.).
func (ix *Indexer) setWorkDir(workDir string) error {
	_ = os.MkdirAll(workDir, os.ModePerm)
	indicesPath := path.Join(workDir, "indices")
	metaPath := path.Join(workDir, "meta")
	_ = os.MkdirAll(indicesPath, os.ModePerm)
	_ = os.MkdirAll(metaPath, os.ModePerm)

	opts := badger.DefaultOptions(metaPath)
	opts.BypassLockGuard = true
	metaDB, err := badger.Open(opts)
	if err != nil {
		return err
	}

	ix.metaStorage = metaDB
	ix.workDir = indicesPath

	return nil
}

func (ix *Indexer) prepareIndexName(name string) string {
	name = path.Clean(name)
	return name
}

func (ix *Indexer) searchName(index, field string) string {
	return fmt.Sprintf("%s_%s", index, field)
}

func (ix *Indexer) filterName(index, field string) string {
	return fmt.Sprintf("%s_filter_%s", index, field)
}

func (ix *Indexer) createIndexFiles(indexName string, settings IndexSettings) error {
	if len(settings.IndexingFields) == 0 {
		return fmt.Errorf("indexing fields length is 0")
	}
	// Создание файлов для индексируемых полей.
	for _, searchFieldName := range settings.IndexingFields {
		name := ix.searchName(indexName, searchFieldName)
		opts := []StorageOpt{
			OptMaxKeyAlphaPower(33),
			OptCompressKey(false),
			OptMaxKeySize(24),
			OptFilesPath(path.Join(ix.workDir, name)),
			OptMarshaller(NewDocInfoMarshaller()),
		}
		if err := ix.storage.StartCreatingIndex(name, opts...); err != nil {
			return err
		}
	}
	// Создание файлов для фильтров.
	for _, filterFieldName := range settings.FilteringFields {
		name := ix.filterName(indexName, filterFieldName)
		opts := []StorageOpt{
			OptMaxKeyAlphaPower(33),
			OptCompressKey(false),
			OptMaxKeySize(50),
			OptFilesPath(path.Join(ix.workDir, name)),
			OptMarshaller(NewFilterMarshaller()),
		}
		if err := ix.storage.StartCreatingIndex(name, opts...); err != nil {
			return err
		}
	}

	return nil
}

func (ix *Indexer) saveSettings(indexName string, settings IndexSettings) error {
	settingsData, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	return ix.metaStorage.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(indexName), settingsData)
	})
}

func (ix *Indexer) prepareSettings(settings *IndexSettings) IndexSettings {
	settings.IdentifierField = strings.ToLower(settings.IdentifierField)
	for i, searchFieldName := range settings.IndexingFields {
		settings.IndexingFields[i] = PrepareFieldName(searchFieldName)
	}
	for i, filterFieldName := range settings.FilteringFields {
		settings.FilteringFields[i] = PrepareFieldName(filterFieldName)
	}
	return *settings
}

func (ix *Indexer) ListIndices() map[string]IndexSettings {
	result := make(map[string]IndexSettings)
	for indexName, state := range ix.states {
		result[indexName] = state.settings
	}
	return result
}

func (ix *Indexer) CreateIndex(name string, settings IndexSettings) error {
	name = ix.prepareIndexName(name)

	settings = ix.prepareSettings(&settings)
	if err := ix.saveSettings(name, settings); err != nil {
		return err
	}
	state := newIndexState(settings)
	if state.status == IndexStatusCreating {
		return fmt.Errorf("index %s is already in creating status", name)
	}
	state.status = IndexStatusCreating
	ix.Lock()
	ix.states[name] = state
	ix.Unlock()

	return ix.createIndexFiles(name, settings)
}

func (ix *Indexer) deleteIndexFiles(indexName string, settings IndexSettings) error {
	// Создание файлов для индексируемых полей.
	for _, searchFieldName := range settings.IndexingFields {
		name := ix.searchName(indexName, searchFieldName)
		if err := ix.storage.DeleteIndex(name); err != nil {
			return err
		}
	}
	// Создание файлов для фильтров.
	for _, filterFieldName := range settings.FilteringFields {
		name := ix.filterName(indexName, filterFieldName)
		if err := ix.storage.DeleteIndex(name); err != nil {
			return err
		}
	}

	return nil
}

func (ix *Indexer) DeleteIndex(name string) error {
	name = ix.prepareIndexName(name)
	state, ok := ix.states[name]
	if !ok {
		return fmt.Errorf("there is not index state for " + name)
	}
	ix.Lock()
	delete(ix.states, name)
	ix.Unlock()
	return ix.deleteIndexFiles(name, state.settings)
}

// preloadTermsData предзагружает данные term'ов поисковых индексов в префиксное
// дерево для автодополнения и индексов фильтров в карту фильтров (туда сохраняются
// все подходящие документы для конкретного значения конкретного фильтра).
func (ix *Indexer) preloadTermsData(indexName string, state *indexState) {
	for _, searchFieldName := range state.settings.IndexingFields {
		searchIndexName := ix.searchName(indexName, searchFieldName)
		fat := ix.storage.FAT(searchIndexName)

		for fat.IterBegin(); fat.IterNext(); {
			searchTermValue, _, _ := fat.IterValue()
			state.ProcessTerm(searchTermValue)
		}
	}
	for _, filterFieldName := range state.settings.FilteringFields {
		filterIndexName := ix.filterName(indexName, filterFieldName)
		filterData := make(map[FilterValue]map[DocID]struct{})
		fat := ix.storage.FAT(filterIndexName)

		for fat.IterBegin(); fat.IterNext(); {
			filterValue, _, _ := fat.IterValue()
			dataIter, err := ix.storage.GetKeyData(filterIndexName, filterValue)
			if err != nil {
				log.Println("error while preloading filters: ", err)
				continue
			}

			filterDocsIds := make(map[DocID]struct{})
			for dataIter.Next() {
				docIdValue := dataIter.Value()
				targetDocId, ok := docIdValue.(int)
				if !ok {
					log.Printf(
						"can't cast doc ID (%v) for filter %s\n",
						docIdValue, filterFieldName,
					)
				}
				filterDocsIds[DocID(targetDocId)] = emptyStruct
			}
			filterData[FilterValue(filterValue)] = filterDocsIds
		}
		state.SetFilters(FilterName(filterFieldName), filterData)
	}
}

func (ix *Indexer) FinishCreatingIndex(name string) error {
	name = ix.prepareIndexName(name)
	ix.Lock()
	defer ix.Unlock()
	state, ok := ix.states[name]
	if !ok {
		return fmt.Errorf("there is not index state for " + name)
	}

	for _, searchFieldName := range state.settings.IndexingFields {
		searchIndexName := ix.searchName(name, searchFieldName)
		if err := ix.storage.FinishCreatingIndex(searchIndexName); err != nil {
			return err
		}
	}
	for _, filterFieldName := range state.settings.FilteringFields {
		filterIndexName := ix.filterName(name, filterFieldName)
		if err := ix.storage.FinishCreatingIndex(filterIndexName); err != nil {
			return err
		}
	}

	if state.settings.PreloadFilters {
		ix.preloadTermsData(name, state)
	}

	state.status = IndexStatusReady
	return nil
}

func (ix *Indexer) convertDocID(idValue string) (int, error) {
	floatVal, err := strconv.ParseFloat(idValue, 64)
	if err == nil {
		return int(floatVal), nil
	}
	intVal, err := strconv.ParseInt(idValue, 10, 64)
	if err == nil {
		return int(intVal), nil
	}
	return 0, fmt.Errorf("can't convert ID value %s to int", idValue)
}

func (ix *Indexer) documentMap(doc *si.Document) (map[string][]interface{}, error) {
	result := make(map[string][]interface{})

	for _, field := range doc.Fields {
		fieldName := PrepareFieldName(field.Name)
		values, ok := result[fieldName]
		if !ok {
			values = make([]interface{}, 0)
			result[fieldName] = values
		}

		switch field.Type {
		case si.DocumentField_STRING:
			values = append(values, strings.ToLower(field.Value))
		case si.DocumentField_FLOAT:
			floatVal, err := strconv.ParseFloat(field.Value, 64)
			if err != nil {
				log.Printf(
					"Can't parse float value %s of field %s\n",
					field.Value, field.Name,
				)
				continue
			}
			values = append(values, floatVal)
		case si.DocumentField_INT:
			intVal, err := strconv.ParseInt(field.Value, 10, 64)
			if err != nil {
				log.Printf(
					"Can't parse int value %s of field %s\n",
					field.Value, field.Name,
				)
				continue
			}
			values = append(values, intVal)
		}
		result[fieldName] = values
	}

	return result, nil
}

func (ix *Indexer) UpdateIndex(name string, document *si.Document) (err error) {
	name = ix.prepareIndexName(name)

	if !ix.Debug {
		defer func() {
			if panicMsg := recover(); panicMsg != nil {
				err = fmt.Errorf(
					"panic while updating index %s: %v", name, panicMsg,
				)
			}
		}()
	}

	ix.RLock()
	defer ix.RUnlock()
	state, ok := ix.states[name]
	if !ok {
		return fmt.Errorf("there is not index state for " + name)
	}

	docID, err := ix.convertDocID(document.Id)
	if err != nil {
		return err
	}
	docMap, err := ix.documentMap(document)
	if err != nil {
		return err
	}

	// Пробуем получить значение(ия) для каждого индексируемого поля и обновить
	// этим занчением(ями) соответствующий файл индекса.
	for _, searchFieldName := range state.settings.IndexingFields {
		values, ok := docMap[searchFieldName]
		if !ok || len(values) == 0 {
			log.Printf("values for field %s are empty\n", searchFieldName)
			continue
		}
		searchIndexName := ix.searchName(name, searchFieldName)
		for _, value := range values {
			strValue, ok := value.(string)
			// Нельзя индексировать нестроковые данные для поиска.
			if !ok {
				log.Printf(
					"Can't index non-string search field %s with value %v\n",
					searchFieldName, value,
				)
				continue
			}
			tokenData, err := ix.processor.ProcessDocumentValue(docID, strValue)
			if err != nil {
				return err
			}
			for _, tokenStats := range tokenData {
				err := ix.storage.UpdateIndex(searchIndexName, TermData{
					Term:     tokenStats.Token,
					TermInfo: tokenStats.Info,
				})
				if err != nil {
					return err
				}
			}
		}
	}
	// Пробуем получить значение(ия) для каждого фильтрующегося поля и обновить
	// этим занчением(ями) соответствующий файл индекса фильтров.
	for _, filterFieldName := range state.settings.FilteringFields {
		values, ok := docMap[filterFieldName]
		if !ok || len(values) == 0 {
			log.Printf("values for field %s are empty\n", filterFieldName)
			continue
		}
		filterIndexName := ix.filterName(name, filterFieldName)
		for _, value := range values {
			// TODO: реализовать фильтрацию по остальным типам значений.
			filterValue := fmt.Sprintf("%v", value)
			err := ix.storage.UpdateIndex(
				filterIndexName,
				FilterData{
					DocId: docID, Value: filterValue,
				},
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// getFilteredDocIds возвращает слайс карт (для каждого фильтра своя карта). В
// каждой карте ключами язвляются значения ID документов, которые связаны со
// значениями конкретного фильтра.
// Если
func (ix *Indexer) getFilteredDocIds(
	indexName string, state *indexState, filters SearchFilters,
) (filteredDocIds []map[DocID]struct{}, err error) {
	// Если есть настройка предзагрузки фильтров в память - считываем из нее.
	if state.settings.PreloadFilters {
		for filterName, filterValues := range filters {
			filterDocsIds, ok := state.preloadedFilters[filterName]
			if !ok {
				continue
			}
			for _, filterValue := range filterValues {
				docIds, ok := filterDocsIds[filterValue]
				if !ok {
					continue
				}
				filteredDocIds = append(filteredDocIds, docIds)
			}
		}
		return filteredDocIds, nil
	}
	// Иначе - считываем напрямую из хранилища.
	for filterName, filterValues := range filters {
		filterIndexName := ix.filterName(indexName, string(filterName))
		filterDocsIds := make(map[DocID]struct{})
		for _, filterValue := range filterValues {
			iter, err := ix.storage.GetKeyData(filterIndexName, string(filterValue))
			if err != nil {
				return nil, err
			}
			for iter.Next() {
				iterVal := iter.Value()
				docId, ok := iterVal.(DocID)
				if !ok {
					log.Printf("Can't cast filter value %v to DocID\n", iterVal)
					continue
				}
				filterDocsIds[docId] = emptyStruct
			}
		}
		filteredDocIds = append(filteredDocIds, filterDocsIds)
	}
	return filteredDocIds, nil
}

func (ix *Indexer) concurrentSearch(
	indexName string, state *indexState, terms []string,
	filteredDocsIds []map[DocID]struct{}, limit int,
) (map[DocID]int, error) {
	goroutinesNum := len(state.settings.IndexingFields)
	docsScoresChan := make(chan map[DocID]int, goroutinesNum)
	errChan := make(chan error, goroutinesNum)

	totalDocsScores := make(map[DocID]int)
	if !ix.Debug {
		var wg sync.WaitGroup
		for _, searchFieldName := range state.settings.IndexingFields {
			wg.Add(1)
			go func(searchIndexName string) {
				defer wg.Done()
				docsScores, err := ix.engine.Search(
					searchIndexName, terms, filteredDocsIds, limit,
				)
				if err != nil {
					errChan <- err
					return
				}
				docsScoresChan <- docsScores
			}(ix.searchName(indexName, searchFieldName))
		}
		wg.Wait()

		for i := 0; i < goroutinesNum; i++ {
			select {
			case docsScores := <-docsScoresChan:
				for docId, score := range docsScores {
					totalDocsScores[docId] += score
				}
			case err := <-errChan:
				return nil, err
			}
		}
	} else {
		for _, searchFieldName := range state.settings.IndexingFields {
			searchIndexName := ix.searchName(indexName, searchFieldName)
			docsScores, err := ix.engine.Search(
				searchIndexName, terms, filteredDocsIds, limit,
			)
			if err != nil {
				return nil, err
			}
			for docId, score := range docsScores {
				totalDocsScores[docId] += score
			}
		}
	}

	return totalDocsScores, nil
}

func (ix *Indexer) prepareSearchOpts(opts *SearchOptions) SearchOptions {
	for filterName, filterValues := range opts.Filters {
		delete(opts.Filters, filterName)
		preparedFilterName := FilterName(PrepareFieldName(string(filterName)))
		preparedFilterValues := make([]FilterValue, len(filterValues))
		for i, filterValue := range filterValues {
			preparedFilterValues[i] = FilterValue(PrepareFieldName(string(filterValue)))
		}
		opts.Filters[preparedFilterName] = preparedFilterValues
	}
	return *opts
}

func (ix *Indexer) searchFiltersToMap(
	filters SearchFilters,
) map[FilterName]map[FilterValue]struct{} {
	filtersMap := make(map[FilterName]map[FilterValue]struct{})
	for filterName, filterValues := range filters {
		filterValuesMap, ok := filtersMap[filterName]
		if !ok {
			filterValuesMap = make(map[FilterValue]struct{})
			filtersMap[filterName] = filterValuesMap
		}
		for _, filterValue := range filterValues {
			filterValuesMap[filterValue] = emptyStruct
		}
	}
	return filtersMap
}

func (ix *Indexer) getSuggestedTerms(
	state *indexState, terms []string,
) (suggested []string, corrected bool) {
	suggested = make([]string, len(terms))
	for i, term := range terms {
		suggestedTerm := state.CorrectSpell(term)
		if suggestedTerm != terms[i] {
			corrected = true
		}
		suggested[i] = suggestedTerm
	}
	return suggested, corrected
}

func (ix *Indexer) getState(indexName string) (*indexState, error) {

	state, ok := ix.states[indexName]
	if !ok {
		return nil, fmt.Errorf("there is not index state for " + indexName)
	}
	if state.status != IndexStatusReady {
		return nil, fmt.Errorf("can't search in index %s: it's not ready", indexName)
	}
	return state, nil
}

func (ix *Indexer) DoSearch(
	indexName string, query string, opts SearchOptions,
) (*SearchResult, error) {
	var (
		suggestedTerms []string
		termsCorrected bool
	)
	startSearching := time.Now()
	opts = ix.prepareSearchOpts(&opts)
	indexName = ix.prepareIndexName(indexName)
	state, err := ix.getState(indexName)
	if err != nil {
		return nil, err
	}
	// Получение term'ов из запроса.
	terms, err := ix.processor.ProcessQuery(query)
	if err != nil {
		return nil, err
	}
	// Искать по маске всех term'ов без фильтров безсмысленно.
	isSearchByMask := len(terms) == 1 && terms[0] == allTermsMask
	if isSearchByMask && len(opts.Filters) == 0 {
		return nil, fmt.Errorf(`can't search by mask "%s" without filters`, allTermsMask)
	}
	// Получение term'ов после коррекции, если поиск не по маске.
	if !isSearchByMask {
		suggestedTerms, termsCorrected = ix.getSuggestedTerms(state, terms)
	}

	// Подготовка данных фильтров (о документах) из опций.
	filteredDocIds, err := ix.getFilteredDocIds(indexName, state, opts.Filters)
	if err != nil {
		return nil, err
	}
	attempt := 1
Search:
	// Получение поисковых результатов (без ранжирования).
	relevantDocsScores, err := ix.concurrentSearch(
		indexName, state, terms, filteredDocIds, opts.TopN,
	)
	if err != nil {
		return nil, err
	}

	log.Printf(
		"Search results number is %d for %s\n",
		len(relevantDocsScores), formatTermsAndFilters(terms, opts.Filters),
	)
	// Если нет результатов и при этом term'ы были исправлены автокорректором, то
	// повторяем поиск с исправленными term'ами.
	if len(relevantDocsScores) == 0 && termsCorrected && attempt <= 2 {
		terms = suggestedTerms
		attempt++
		goto Search
	}
	// Ранжирование поисковых результатов с получением лучших opts.TopN.
	rankedDocsIds := ix.engine.Ranking(opts.TopN, relevantDocsScores)
	result := &SearchResult{
		Docs: rankedDocsIds,
	}
	log.Printf(
		"Search time for %s: %v\n",
		formatTermsAndFilters(terms, opts.Filters), time.Since(startSearching),
	)
	// Расчет статистики по фильтрам, если указано в опциях.
	if opts.WithFiltersStats {
		if !state.settings.PreloadFilters {
			return nil, fmt.Errorf("can't calculate filteredDocIds stats without preloading")
		}
		excludeFilters := ix.searchFiltersToMap(opts.Filters)
		result.FilterStats = ix.engine.CalculateFiltersStats(
			relevantDocsScores, state.preloadedFilters, excludeFilters, 0,
		)
	}
	log.Printf(
		"Search + Calc stats time for %s: %v\n",
		formatTermsAndFilters(terms, opts.Filters), time.Since(startSearching),
	)
	if termsCorrected {
		result.SuggestedTerms = suggestedTerms
	}

	return result, nil
}

func (ix *Indexer) DoAutocomplete(indexName, term string, limit int) ([]string, error) {
	indexName = ix.prepareIndexName(indexName)
	state, ok := ix.states[indexName]
	if !ok {
		return nil, fmt.Errorf("there is not index state for " + indexName)
	}
	return state.GetTermsByPrefix(term, limit), nil
}
