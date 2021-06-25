package core

import (
	cm "bitbucket.org/entrlcom/genproto/gen/go/course-manager/v1"
	v1 "bitbucket.org/entrlcom/genproto/gen/go/iso/639/v1"
	"encoding/csv"
	"fmt"
	"github.com/google/gops/agent"
	"github.com/stretchr/testify/assert"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

type FileCoursesSource struct {
	file    *os.File
	reader  *csv.Reader
	counter int
}

func NewFileCoursesSource(filePath string) (*FileCoursesSource, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	return &FileCoursesSource{
		file:   file,
		reader: csv.NewReader(file),
	}, nil
}

func (fs *FileCoursesSource) close() {
	_ = fs.file.Close()
}

func (fs *FileCoursesSource) GetCourse() (*cm.Course, bool) {
	record, err := fs.reader.Read()
	if err != nil {
		log.Println(err)
		return nil, false
	}
	if len(record) < 2 {
		log.Println("incorrect data length")
		return nil, false
	}
	title, headline := record[0], record[1]
	fs.counter++

	topics := make([]*cm.Topic, 0)
	if fs.counter%2 == 0 {
		topics = append(topics, &cm.Topic{Name: "every2"})
		headline += " every2"
	}
	if fs.counter%5 == 0 {
		topics = append(topics, &cm.Topic{Name: "every5"})
		headline += " every5"
	}
	if fs.counter%10 == 0 {
		topics = append(topics, &cm.Topic{Name: "every10"})
		headline += " every10"
	}
	if fs.counter%50 == 0 {
		topics = append(topics, &cm.Topic{Name: "every50"})
		headline += " every50"
	}
	if fs.counter%100 == 0 {
		topics = append(topics, &cm.Topic{Name: "every100"})
		headline += " every100"
	}
	if fs.counter%500 == 0 {
		topics = append(topics, &cm.Topic{Name: "every500"})
		headline += " every500"
	}

	providers := []string{
		"stepik", "coursera", "udemy",
	}
	providerName := providers[rand.Intn(len(providers))]

	course := &cm.Course{
		Id:          strconv.Itoa(fs.counter),
		Title:       title,
		Headline:    headline,
		Description: "",
		Status:      cm.Course_Status(rand.Intn(6)),
		Level:       cm.Course_Level(rand.Intn(4)),
		Pace:        cm.Course_Pace(rand.Intn(3)),
		Provider:    &cm.Provider{Name: providerName},
		Topics:      topics,
	}

	return course, true
}

func startGops() {
	go func() {
		if err := agent.Listen(agent.Options{Addr: ":1337"}); err != nil {
			log.Fatal(err)
		}
		select {}
	}()
}

func BenchmarkManager_FullIndexCreation(t *testing.B) {
	startGops()
	maxNum := 50000
	indexName := "courses"
	storage := NewFileSystemStorage()
	manager, err := NewManager("/tmp/test_index_50000", storage)
	if err != nil {
		t.Fatal(err)
	}

	coursesSource, err := NewFileCoursesSource("/tmp/courses.csv")
	if err != nil {
		t.Fatal(err)
	}

	err = manager.CreateIndex(indexName, IndexSettings{
		FilteringFields: []string{
			fieldNameTopicsName, fieldNameStatus, fieldNameLevel, fieldNamePace},
		IndexingFields: []string{
			fieldNameTitle, fieldNameProviderName, fieldNamePartnersName,
			fieldNameTopicsName,
		},
		PreloadFilters: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	coursesCount := 0
	for {
		course, ok := coursesSource.GetCourse()
		if !ok {
			break
		}
		if strings.Contains(strings.ToLower(course.Title), "mixable") {
			log.Println(course.Id)
		}
		t.StartTimer()
		err := manager.UpdateIndex(indexName, GetCourseDocument(course))
		if err != nil {
			log.Println("Error while updating index: ", err)
			continue
		}
		t.StopTimer()
		coursesCount++
		log.Printf("%d courses have been indexed\n", coursesCount)
		if coursesCount >= maxNum {
			break
		}
	}
	if err := manager.FinishCreatingIndex(indexName); err != nil {
		t.Fatal(err)
	}

	fmt.Printf("%d courses have been indexed\n", coursesCount)
}

func BenchmarkIndexer_DoAutocomplete(b *testing.B) {
	indexName := "courses"
	storage := NewFileSystemStorage()
	manager, err := NewManager("/tmp/test_index_50000", storage)
	if err != nil {
		b.Fatal(err)
	}
	manager.Debug = true

	prefix := "райг"
	result, err := manager.DoAutocomplete(indexName, prefix, 10)
	if err != nil {
		b.Fatal(err)
	}
	log.Printf("Result for %s: %v\n", prefix, result)
}

func BenchmarkIndexer_SpellCorrection(b *testing.B) {
	indexName := "courses"
	storage := NewFileSystemStorage()
	manager, err := NewManager("/tmp/test_index_50000", storage)
	if err != nil {
		b.Fatal(err)
	}
	manager.Debug = true
	b.ResetTimer()
	b.StartTimer()
	start := time.Now()
	terms := []string{
		"pyhton", "django", "golan", "gollang", "jaava", "райгродский",
	}
	state := manager.states[indexName]
	result, _ := manager.getSuggestedTerms(state, terms)
	b.StopTimer()
	log.Printf("Result for %s: %v in %v\n", terms, result, time.Since(start))
}

func BenchmarkManager_Search50000(t *testing.B) {
	//startGops()
	indexName := "courses"
	storage := NewFileSystemStorage()
	manager, err := NewManager("/tmp/test_index_50000", storage)
	if err != nil {
		t.Fatal(err)
	}
	manager.Debug = true

	query := "python django"
	query = "asdfsdaasdf"
	t.StartTimer()
	startTime := time.Now()
	result, err := manager.DoSearch(
		indexName,
		query,
		SearchOptions{
			TopN: 50,
			Filters: SearchFilters{
				"topics.name": []FilterValue{
					"python",
				},
				//"level": []FilterValue{
				//	"advanced",
				//},
			},
			WithFiltersStats: true,
		},
	)
	fmt.Println("Search time:", time.Now().Sub(startTime))
	t.StopTimer()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("Result for \"%s\": %v\n", query, result.Docs)
}

func TestManager_DoSearch(t *testing.T) {
	indexName := "courses"
	storage := NewFileSystemStorage()
	manager, err := NewManager("/tmp/unit_test_index", storage)
	if err != nil {
		t.Fatal(err)
	}

	err = manager.CreateIndex(indexName, IndexSettings{
		IdentifierField: "Id",
		FilteringFields: []string{"Topics.Name", "Status", "Level", "Pace"},
		IndexingFields:  []string{"Title", "Description"},
		PreloadFilters:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := manager.UpdateIndex(indexName, GetCourseDocument(&cm.Course{
		Id:       "1",
		Title:    "Beyond the Basics in Windows Phone 8",
		Headline: "",
		Description: "This course will walk you through four new features " +
			"included in the Windows Phone 8 SDK including: Speech, In-App " +
			"Purchasing, Wallet transactions and the new Map control.",
		Status: cm.Course_COMING_SOON,
		Level:  cm.Course_BEGINNER,
		Pace:   cm.Course_LIVE_CLASS,
		Topics: []*cm.Topic{
			{Name: "windows"}, {Name: "software"},
		},
	})); err != nil {
		t.Fatal(err)
	}
	if err := manager.UpdateIndex(indexName, GetCourseDocument(&cm.Course{
		Id:       "2",
		Title:    "Sci-Fi Concept Art in Photoshop and Maya",
		Headline: "",
		Description: "In this Photoshop and Maya tutorial, we'll use " +
			"efficient techniques to concept a sci-fi environment. Software " +
			"required: Photoshop CC, Maya 2015.",
		Status: cm.Course_COMING_SOON,
		Level:  cm.Course_ADVANCED,
		Pace:   cm.Course_SELF_PACED,
		Topics: []*cm.Topic{
			{Name: "photoshop"}, {Name: "maya"}, {Name: "software"},
		},
	})); err != nil {
		t.Fatal(err)
	}
	if err := manager.UpdateIndex(indexName, GetCourseDocument(&cm.Course{
		Id:       "3",
		Title:    "Unit Testing with Python",
		Headline: "",
		Description: "This course will help you to write good unit tests for " +
			"your Python code, using tools such as unittest, doctest and py.test. " +
			"Unit the tests should improve code quality, and also support future " +
			"development.",
		Status: cm.Course_IN_PROGRESS,
		Level:  cm.Course_BEGINNER,
		Pace:   cm.Course_LIVE_CLASS,
		Topics: []*cm.Topic{
			{Name: "python"}, {Name: "testing"}, {Name: "software-development"},
		},
	})); err != nil {
		t.Fatal(err)
	}
	if err := manager.UpdateIndex(indexName, GetCourseDocument(&cm.Course{
		Id:       "4",
		Title:    "Web Automation with Python Selenium",
		Headline: "",
		Description: "Automate web browsers! Learn how to automate web " +
			"browsers with Python Selenium. Any website or web app, automatic " +
			"Anything on the web Why take this course? Au…",
		Status: cm.Course_AVAILABLE,
		Level:  cm.Course_ALL,
		Pace:   cm.Course_INSTRUCTOR_PACED,
		Topics: []*cm.Topic{
			{Name: "python"}, {Name: "selenium"}, {Name: "testing"},
		},
	})); err != nil {
		t.Fatal(err)
	}
	if err := manager.UpdateIndex(indexName, GetCourseDocument(&cm.Course{
		Id:       "5",
		Title:    "Java: JSON Databinding with Jackson",
		Headline: "",
		Description: "In this course, you will learn about the Jackson " +
			"Library API and how to bind them with Dates, Lists, Arrays, and " +
			"Enums to create data-driven applications.",
		Status: cm.Course_COMING_SOON,
		Level:  cm.Course_BEGINNER,
		Pace:   cm.Course_LIVE_CLASS,
		Topics: []*cm.Topic{
			{Name: "java"}, {Name: "programming"}, {Name: "jackson"},
		},
	})); err != nil {
		t.Fatal(err)
	}

	if err := manager.FinishCreatingIndex(indexName); err != nil {
		t.Fatal(err)
	}

	result, err := manager.DoSearch(
		indexName,
		"python test software",
		SearchOptions{
			TopN:             4,
			Filters:          nil,
			WithFiltersStats: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 3, len(result.Docs))
	assert.Equal(t, DocID(3), result.Docs[0])
	assert.Equal(t, DocID(4), result.Docs[1])
	assert.Equal(t, DocID(2), result.Docs[2])

	result, err = manager.DoSearch(
		indexName,
		"python test software",
		SearchOptions{
			TopN: 5,
			Filters: map[FilterName][]FilterValue{
				"Topics.Name": {"python"},
			},
			WithFiltersStats: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(result.Docs))
	assert.Equal(t, DocID(3), result.Docs[0])
	assert.Equal(t, DocID(4), result.Docs[1])

	result, err = manager.DoSearch(
		indexName,
		"python test software",
		SearchOptions{
			TopN:             2,
			Filters:          nil,
			WithFiltersStats: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(result.Docs))
	assert.Equal(t, DocID(3), result.Docs[0])
	assert.Equal(t, DocID(4), result.Docs[1])
}

func TestManager_DoSearch2(t *testing.T) {
	indexName := "courses"
	storage := NewFileSystemStorage()
	manager, err := NewManager("/tmp/unit_test_index", storage)
	if err != nil {
		t.Fatal(err)
	}
	manager.Debug = true

	stepikProv := &cm.Provider{
		Id:     "1",
		Name:   "Stepik",
		Domain: "stepik.org",
	}
	courseraProv := &cm.Provider{
		Id:     "2",
		Name:   "Coursera",
		Domain: "coursera.org",
	}
	udemyProv := &cm.Provider{
		Id:     "3",
		Name:   "Udemy",
		Domain: "udemy.com",
	}

	instructorSavvat := &cm.Instructor{
		Id:   "1",
		Name: "Савватеев Алексей Владимирович",
	}
	instructorRaygorod := &cm.Instructor{
		Id:   "2",
		Name: "Райгородский Андрей Михайлович",
	}
	instructorJohnDoe := &cm.Instructor{
		Id:   "3",
		Name: "John Doe",
	}

	err = manager.CreateIndex(indexName, IndexSettings{
		IdentifierField: "Id",
		FilteringFields: []string{
			fieldNameLevel, fieldNameStatus, fieldNamePace, fieldNameLanguages,
			fieldNameVideoDuration, fieldNameSubtitles,
		},
		IndexingFields: []string{
			fieldNameTitle, fieldNameProviderName, fieldNamePartnersName,
			fieldNameInstructorsName, fieldNameTopicsName,
		},
		PreloadFilters: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := manager.UpdateIndex(indexName, GetCourseDocument(&cm.Course{
		Id:                 "1000",
		Title:              "Some test course",
		Status:             cm.Course_COMING_SOON,
		Level:              cm.Course_BEGINNER,
		Pace:               cm.Course_LIVE_CLASS,
		VideoDurationRange: "0-2 hr",
		Languages: []v1.Iso639_1{
			v1.Iso639_1_CH,
		},
		Subtitles: []v1.Iso639_1{
			v1.Iso639_1_CH,
		},
		Instructors: nil,
		Provider:    nil,
		Topics:      nil,
		Partners:    nil,
	})); err != nil {
		t.Fatal(err)
	}
	if err := manager.UpdateIndex(indexName, GetCourseDocument(&cm.Course{
		Id:                 "1",
		Title:              "Beyond the Basics in Windows Phone 8",
		Status:             cm.Course_COMING_SOON,
		Level:              cm.Course_BEGINNER,
		Pace:               cm.Course_LIVE_CLASS,
		VideoDurationRange: "0-2 hr",
		Languages: []v1.Iso639_1{
			v1.Iso639_1_EN, v1.Iso639_1_RU,
		},
		Subtitles: []v1.Iso639_1{
			v1.Iso639_1_EN, v1.Iso639_1_RU,
		},
		Instructors: []*cm.Instructor{
			instructorSavvat, instructorRaygorod, instructorJohnDoe,
		},
		Provider: courseraProv,
		Topics: []*cm.Topic{
			{Name: "windows"}, {Name: "software"},
		},
	})); err != nil {
		t.Fatal(err)
	}
	if err := manager.UpdateIndex(indexName, GetCourseDocument(&cm.Course{
		Id:                 "2",
		Title:              "Sci-Fi Concept Art in Photoshop and Maya",
		Status:             cm.Course_COMING_SOON,
		Level:              cm.Course_ADVANCED,
		Pace:               cm.Course_SELF_PACED,
		VideoDurationRange: "0-2 hr",
		Languages: []v1.Iso639_1{
			v1.Iso639_1_EN, v1.Iso639_1_FR,
		},
		Subtitles: []v1.Iso639_1{
			v1.Iso639_1_EN, v1.Iso639_1_FR,
		},
		Instructors: []*cm.Instructor{
			instructorSavvat, instructorJohnDoe,
		},
		Provider: stepikProv,
		Topics: []*cm.Topic{
			{Name: "photoshop"}, {Name: "maya"}, {Name: "software"},
		},
	})); err != nil {
		t.Fatal(err)
	}
	if err := manager.UpdateIndex(indexName, GetCourseDocument(&cm.Course{
		Id:                 "3",
		Title:              "Unit Testing with Python",
		Status:             cm.Course_IN_PROGRESS,
		Level:              cm.Course_BEGINNER,
		Pace:               cm.Course_LIVE_CLASS,
		VideoDurationRange: "2-10 hr",
		Languages: []v1.Iso639_1{
			v1.Iso639_1_RU,
		},
		Subtitles: []v1.Iso639_1{
			v1.Iso639_1_RU,
		},
		Instructors: []*cm.Instructor{
			instructorRaygorod, instructorJohnDoe,
		},
		Provider: courseraProv,
		Topics: []*cm.Topic{
			{Name: "python"}, {Name: "testing"}, {Name: "software-development"},
		},
	})); err != nil {
		t.Fatal(err)
	}
	if err := manager.UpdateIndex(indexName, GetCourseDocument(&cm.Course{
		Id:                 "4",
		Title:              "Web Automation with Python Selenium",
		Status:             cm.Course_AVAILABLE,
		Level:              cm.Course_ALL,
		Pace:               cm.Course_INSTRUCTOR_PACED,
		VideoDurationRange: "2-10 hr",
		Languages: []v1.Iso639_1{
			v1.Iso639_1_EN,
		},
		Subtitles: []v1.Iso639_1{
			v1.Iso639_1_EN,
		},
		Instructors: []*cm.Instructor{
			instructorSavvat, instructorRaygorod,
		},
		Provider: stepikProv,
		Topics: []*cm.Topic{
			{Name: "python"}, {Name: "selenium"}, {Name: "testing"},
		},
	})); err != nil {
		t.Fatal(err)
	}
	if err := manager.UpdateIndex(indexName, GetCourseDocument(&cm.Course{
		Id:                 "5",
		Title:              "Java: JSON Databinding with Jackson",
		Status:             cm.Course_COMING_SOON,
		Level:              cm.Course_BEGINNER,
		Pace:               cm.Course_LIVE_CLASS,
		VideoDurationRange: "10+ hr",
		Languages: []v1.Iso639_1{
			v1.Iso639_1_FR, v1.Iso639_1_RU,
		},
		Subtitles: []v1.Iso639_1{
			v1.Iso639_1_FR, v1.Iso639_1_RU,
		},
		Instructors: []*cm.Instructor{
			instructorSavvat,
		},
		Provider: udemyProv,
		Topics: []*cm.Topic{
			{Name: "java"}, {Name: "programming"}, {Name: "jackson"},
		},
	})); err != nil {
		t.Fatal(err)
	}

	if err := manager.FinishCreatingIndex(indexName); err != nil {
		t.Fatal(err)
	}

	result, err := manager.DoSearch(
		indexName,
		"Савватеев Райгородский",
		SearchOptions{
			TopN: 5,
			Filters: SearchFilters{
				fieldNameLanguages: {"RU", "FR"},
			},
			WithFiltersStats: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(result.Docs))
	assert.Equal(t, DocID(5), result.Docs[0])

	result, err = manager.DoSearch(
		indexName,
		"Райгородский coursera",
		SearchOptions{
			TopN: 5,
			Filters: SearchFilters{
				"languages": {"RU"},
			},
			WithFiltersStats: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(result.Docs))
	assert.Equal(t, DocID(3), result.Docs[0])
	assert.Equal(t, DocID(1), result.Docs[1])
}
