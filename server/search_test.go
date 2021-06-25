package server

import (
	cm "bitbucket.org/entrlcom/genproto/gen/go/course-manager/v1"
	v1 "bitbucket.org/entrlcom/genproto/gen/go/iso/639/v1"
	si "bitbucket.org/entrlcom/genproto/gen/go/search/indexer/v1"
	"bitbucket.org/entrlcom/indexer/core"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"log"
	"net"
	"testing"
	"time"
)

const (
	testIndexName = "test_courses_index"
	serverAddr    = ":18456"
)

var testCourses = []cm.Course{
	{
		Id:                 "1000",
		Title:              "loren ipsum",
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
	},
	{
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
	},
	{
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
	},
	{
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
	},
	{
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
			{Name: "python"}, {Name: "selenium"}, {Name: "automation"},
		},
	},
	{
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
	},
}

func createTestServer() (*grpc.Server, *IndexerServer) {
	gs := grpc.NewServer()

	storage := core.NewFileSystemStorage()
	manager, err := core.NewManager(fmt.Sprintf("/tmp/%s_index", testIndexName), storage)
	if err != nil {
		panic(err)
	}
	s := NewIndexerServer(manager)
	si.RegisterIndexerServiceServer(gs, s)

	return gs, s
}

func startServer(gs *grpc.Server, addr string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}

	log.Println("gRPC server started...")

	if err := gs.Serve(lis); err != nil {
		panic(err)
	}
}

func getClient(addr string) si.IndexerServiceClient {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		panic(err)
	}

	return si.NewIndexerServiceClient(conn)
}

func TestIndexerServerTestSuite(t *testing.T) {
	suite.Run(t, new(IndexerTestSuite))
}

func BenchmarkExternalServerSearch(t *testing.B) {
	ctx := context.Background()
	cli := getClient(":9001")
	start := time.Now()
	resp, err := cli.Search(ctx, &si.SearchRequest{
		Q: "python",
		//Filter:    "filter_topics.name=python",
		Service:   "courses",
		PageToken: "3",
		PageSize:  10,
	})
	if err != nil {
		t.Fatal(err)
	}
	log.Println("Search time:", time.Now().Sub(start))
	log.Println("Results:", resp.DocIds)
}

type IndexerTestSuite struct {
	suite.Suite
	cli si.IndexerServiceClient
	gs  *grpc.Server
	srv *IndexerServer
}

func (suite *IndexerTestSuite) SetupSuite() {
	suite.gs, suite.srv = createTestServer()

	go startServer(suite.gs, serverAddr)
	suite.cli = getClient(serverAddr)
}

func (suite *IndexerTestSuite) TearDownSuite() {
	suite.gs.Stop()
}

func (suite *IndexerTestSuite) TestIndexerServer_CreatingIndex() {
	t := suite.T()
	cli := suite.cli
	ctx := context.Background()

	_, err := cli.CreateIndex(ctx, &si.CreateIndexRequest{
		Name: testIndexName,
		Settings: &si.CreateIndexRequest_Settings{
			FacetedFields: []string{
				"level", "pace", "status", "video_duration", "languages",
				"subtitles",
			},
			SearchableFields: []string{
				"title", "provider.name", "partners.name", "instructors.name",
				"topics.name",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	docsCli, err := cli.IndexDocuments(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = docsCli.Send(&si.IndexDocumentsRequest{
		Data: &si.IndexDocumentsRequest_ResourceId{
			ResourceId: "/indices/" + testIndexName,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, course := range testCourses {
		err = docsCli.Send(&si.IndexDocumentsRequest{
			Data: &si.IndexDocumentsRequest_Document{
				Document: core.GetCourseDocument(&course),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := docsCli.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	resp, err := cli.Search(ctx, &si.SearchRequest{
		Q:       "python test software",
		Service: testIndexName,
		Filter:  "filter_topics.name=python&filter_topics.name=testing",
	})
	if err != nil {
		t.Fatal(err)
	}
	log.Println("Search time:", time.Now().Sub(start))

	assert.Equal(t, 4, len(resp.DocIds))
	assert.Equal(t, "4", resp.DocIds[0])
	assert.Equal(t, "3", resp.DocIds[1])

	start = time.Now()
	resp, err = cli.Search(ctx, &si.SearchRequest{
		Q:         "python test software",
		Service:   testIndexName,
		PageToken: "2",
		PageSize:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	log.Println("Search time:", time.Now().Sub(start))
	assert.Equal(t, 1, len(resp.DocIds))
	assert.Equal(t, "3", resp.DocIds[0])

	start = time.Now()
	resp, err = cli.Search(ctx, &si.SearchRequest{
		Q:       "pyhton",
		Service: testIndexName,
	})
	if err != nil {
		t.Fatal(err)
	}
	log.Println("Search time:", time.Now().Sub(start))
	assert.Equal(t, 2, len(resp.DocIds))
	assert.Equal(t, "4", resp.DocIds[0])
	assert.Equal(t, "3", resp.DocIds[1])
}

func (suite *IndexerTestSuite) TestIndexerServer_Autocomplete() {
	t := suite.T()
	cli := suite.cli
	ctx := context.Background()

	_, err := cli.CreateIndex(ctx, &si.CreateIndexRequest{
		Name: testIndexName,
		Settings: &si.CreateIndexRequest_Settings{
			FacetedFields: []string{
				"level", "pace", "status", "video_duration", "languages",
				"subtitles",
			},
			SearchableFields: []string{
				"title", "provider.name", "partners.name", "instructors.name",
				"topics.name",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	docsCli, err := cli.IndexDocuments(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = docsCli.Send(&si.IndexDocumentsRequest{
		Data: &si.IndexDocumentsRequest_ResourceId{
			ResourceId: "/indices/" + testIndexName,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, course := range testCourses {
		err = docsCli.Send(&si.IndexDocumentsRequest{
			Data: &si.IndexDocumentsRequest_Document{
				Document: core.GetCourseDocument(&course),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := docsCli.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}

	res, err := cli.Autocomplete(ctx, &si.AutocompleteRequest{
		Term:    "p",
		Service: testIndexName,
		Limit:   4,
	})
	if err != nil {
		t.Fatal(err)
	}
	assert.Len(t, res.Terms, 4)

	res, err = cli.Autocomplete(ctx, &si.AutocompleteRequest{
		Term:    "py",
		Service: testIndexName,
	})
	if err != nil {
		t.Fatal(err)
	}
	assert.Len(t, res.Terms, 1)
	assert.Equal(t, "python", res.Terms[0])
}
