package core

import (
	"github.com/emirpasic/gods/trees/binaryheap"
	"math"
)

const (
	maxFiltersStatsUpperBound = 10_000
	allTermsMask              = "*"
)

type (
	FilterName  string
	FilterValue string
	DocID       int
)

// RelevantFunc тип функции, считающей коэффициент релевантности для конкретного
// документа и term'а из запроса.
// На вход передаются данные term'ов для конкретного документа в виде карты,
// где ключи - позиции в term'ов в запросе, значения - данные о позициях в
// документе. И количество term'ов в запросе.
type RelevantFunc func(docTermsInfos map[int]*DocInfo, termsNum int) int

// RelevantFuncWeightedLinear назначает коэффициент документу, как количество
// term'ов в документе, умноженное на позицию term'а в запросе.
func RelevantFuncWeightedLinear(docTermsInfos map[int]*DocInfo, termsNum int) int {
	score := 0
	for termPos, docInfo := range docTermsInfos {
		score += len(docInfo.Positions) * (termsNum - termPos)
	}
	return score
}

type docResult struct {
	docId    DocID
	docScore int
}

type SearchEngine struct {
	storage      Storage
	relevantFunc RelevantFunc
}

func NewSearchEngine(storage Storage) *SearchEngine {
	return &SearchEngine{
		storage:      storage,
		relevantFunc: RelevantFuncWeightedLinear,
	}
}

func (e *SearchEngine) WithRelevantFunc(f RelevantFunc) *SearchEngine {
	e.relevantFunc = f
	return e
}

func (e *SearchEngine) calculateScores(
	indexName string, terms []string,
) (map[DocID]int, error) {
	docsScores := make(map[DocID]int)
	// Ключи - индексы term'ов в поисковом запросе.
	iteratorsMap := make(map[int]*Iterator)
	for i, term := range terms {
		infoIter, err := e.storage.GetKeyData(indexName, term)
		if err != nil {
			return nil, err
		}
		iteratorsMap[i] = infoIter
	}
	// Пока не закончатся данные во всех итераторах term'ов считаем статистику.
	currDocs := make(map[int]*DocInfo)
	for len(iteratorsMap) > 0 {
		for termPos, docIter := range iteratorsMap {
			// Если текущий итератор невалиден - его нужно удалить.
			if !docIter.Next() {
				delete(iteratorsMap, termPos)
				continue
			}
			// Если для текущей позиции еще есть документы, и она пуста, то
			// происходит ее заполнение.
			if _, ok := currDocs[termPos]; !ok {
				docInfo, ok := docIter.Value().(*DocInfo)
				if !ok {
					continue
				}
				currDocs[termPos] = docInfo
			}
		}
		// Заранее выходит из цикла, если больше не осталось данных документов
		// во всех итераторах.
		if len(currDocs) == 0 {
			break
		}
		// Составление карты данных для term'ов для документа с минимальным
		// идентификатором.
		minDocID := math.MaxInt32
		docTermsInfos := make(map[int]*DocInfo)
		for termPos, docInfo := range currDocs {
			if docInfo.DocId < minDocID {
				minDocID = docInfo.DocId
				docTermsInfos = map[int]*DocInfo{
					termPos: docInfo,
				}
			} else if docInfo.DocId == minDocID {
				docTermsInfos[termPos] = docInfo
			}
		}
		// Чистка идентификаторов карты текущих актуальных документов (с
		// минимальным ID).
		for termPos := range docTermsInfos {
			delete(currDocs, termPos)
		}
		// Расчет релевантности для текущего документа.
		docsScores[DocID(minDocID)] = e.relevantFunc(docTermsInfos, len(terms))
	}
	return docsScores, nil
}

// Ranking ранжирует результаты поиска (ключи docsScores) на основе их коэффициентов
// релевантности (значения docsScores), возвращая лучшие topN результатов.
func (e *SearchEngine) Ranking(topN int, docsScores map[DocID]int) []DocID {
	var (
		cmpRes        int
		currDocResult *docResult
	)
	if topN == 0 {
		return []DocID{}
	}
	heap := binaryheap.NewWith(func(a, b interface{}) int {
		cmpRes = a.(*docResult).docScore - b.(*docResult).docScore
		// Если коэффициенты релевантности равны, то сравниваются идентификаторы
		// документов (сортировка по новизне документа).
		if cmpRes == 0 {
			cmpRes = int(a.(*docResult).docId - b.(*docResult).docId)
		}
		return cmpRes
	})

	for docId, docScore := range docsScores {
		currDocResult = &docResult{docId: docId, docScore: docScore}
		if heap.Size() < topN {
			heap.Push(currDocResult)
			continue
		}
		currMin, _ := heap.Peek()
		if currDocResult.docScore >= currMin.(*docResult).docScore {
			heap.Push(currDocResult)
			heap.Pop()
		}
	}

	n := minInt(topN, heap.Size())
	result := make([]DocID, n)
	for i := n - 1; i >= 0; i-- {
		value, ok := heap.Pop()
		if !ok {
			break
		}
		result[i] = value.(*docResult).docId
	}

	return result
}

func (e *SearchEngine) resultByMaskWithFilters(
	filteredDocsIds []map[DocID]struct{}, limit int,
) (map[DocID]int, error) {
	docsScores := make(map[DocID]int)
	if len(filteredDocsIds) == 0 {
		return docsScores, nil
	}
	i := 0
	for _, filterDocs := range filteredDocsIds {
		for docID := range filterDocs {
			docsScores[docID] = 1
			i++
			if limit > 0 && i >= limit {
				break
			}
		}
	}
	return docsScores, nil
}

// Search возвращает карту результатов, где ключи - ID документов, значения -
// коэффициенты релевантности (считаются по функции релевантности e.relevantFunc).
// Если передали слайс фильтров (каждый фильтр - это карта ID документов, которые
// подходят).
func (e *SearchEngine) Search(
	indexName string, terms []string, filteredDocsIds []map[DocID]struct{},
	limit int,
) (docsScores map[DocID]int, err error) {
	if len(terms) == 1 && terms[0] == allTermsMask {
		return e.resultByMaskWithFilters(filteredDocsIds, limit)
	}
	docsScores, err = e.calculateScores(indexName, terms)
	if err != nil {
		return nil, err
	}

	if len(filteredDocsIds) > 0 {
		// Применение фильтров. Считается, что применяется логическая операция И,
		// т.е. если документа нет хотя бы в одной из карт фильтров, то он
		// отбрасывается.
		for docId := range docsScores {
			for _, filter := range filteredDocsIds {
				if _, ok := filter[docId]; !ok {
					delete(docsScores, docId)
					break
				}
			}
		}
	}

	return docsScores, nil
}

func (e *SearchEngine) CalculateFiltersStats(
	docsScores map[DocID]int, filtersDocs map[FilterName]map[FilterValue]map[DocID]struct{},
	excludedFilters map[FilterName]map[FilterValue]struct{}, upperBound int,
) map[FilterName]map[FilterValue]int {
	result := make(map[FilterName]map[FilterValue]int)
	if upperBound > maxFiltersStatsUpperBound {
		upperBound %= maxFiltersStatsUpperBound
	}
	if upperBound <= 0 {
		upperBound = maxFiltersStatsUpperBound
	}

	for filterName, filterValuesDocs := range filtersDocs {
		excludeValues, excludeValuesExists := excludedFilters[filterName]
		result[filterName] = make(map[FilterValue]int)
		for filterValue, filterDocIds := range filterValuesDocs {
			if excludeValuesExists {
				if _, isExcluded := excludeValues[filterValue]; isExcluded {
					continue
				}
			}
			counter := 0
			for docId := range docsScores {
				if _, ok := filterDocIds[docId]; ok {
					counter++
					if counter > upperBound {
						counter = -1
						break
					}
				}
			}
			result[filterName][filterValue] = counter
		}
	}

	return result
}
