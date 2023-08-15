package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/log.go/v2/log"
	uuid "github.com/satori/go.uuid"

	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"
	"go.mongodb.org/mongo-driver/bson"
)

type CenStructure struct {
	Structure KeyfamiliesStruct `bson:"structure,omitempty"      json:"structure,omitempty"`
}

type KeyfamiliesStruct struct {
	Keyfamilies *KeyfamilyArray `bson:"keyfamilies,omitempty"    json:"keyfamilies,omitempty"`
}

type KeyfamilyArray struct {
	Keyfamily []KeyfamilyDetails `bson:"keyfamily,omitempty"      json:"keyfamily,omitempty"`
}

type KeyfamilyDetails struct {
	ID          string             `bson:"id,omitempty"              json:"id,omitempty"`
	Annotations *AnnotationDetails `bson:"annotations,omitempty"     json:"annotations,omitempty"`
	Name        *NameDetails       `bson:"name,omitempty"            json:"name,omitempty"`
}

type AnnotationDetails struct {
	Annotation []AnnoTextTitle `bson:"annotation,omitempty"       json:"annotation,omitempty"`
}
type AnnoTextTitle struct {
	Text  interface{} `bson:"annotationtext,omitempty"    json:"annotationtext,omitempty"`
	Title string      `bson:"annotationtitle,omitempty"   json:"annotationtitle,omitempty"`
}
type NameDetails struct {
	Value string `bson:"value,omitempty"             json:"value,omitempty"`
}

var (
	censusNationalStatistic = true
	fileName                string
	fullURLFile             string
	num                     int
)

const (
	census2011         string = "c2011"
	censusYear         string = "2011"
	censusVersion      string = "1"
	censusPersonalData string = "Sometimes we need to make changes to data if it is possible to identify individuals. This is known as 'statistical disclosure control'. In the 2011 Census, we:\u000a\u000a" +

		"- swapped records (targeted record swapping), for example if a household could be identified because it has unusual characteristics, the record was swapped with a similar one from the same local area" +
		"(in specific cases the household was swapped with one in a nearby local authority) \u000a" +
		"- reduced the detail included for areas where fewer people lived and could be identified, such as electoral wards\u000a\u000a" +

		"Read more about these [methods and why we chose them for the 2011 Census (PDF, 189KB)]" +
		"(https://webarchive.nationalarchives.gov.uk/20160129174312/http:/www.ons.gov.uk/ons/guide-method/census/2011/the-2011-census/processing-the-information/statistical-methodology/statistical-disclosure-control-for-2011-census.pdf)."
)

// Regular expressions
var (
	httpRegex  = regexp.MustCompile(`(http[^\[]*)(\[[^\[]*\])`)
	titleRegex = regexp.MustCompile(`^[\d|\D].*?\-\s*([\d|\D].*)$`)
)

// CensusContactDetails returns the default values for contact details
func CensusContactDetails() models.ContactDetails {
	return models.ContactDetails{
		Email:     "census.customerservices@ons.gov.uk",
		Name:      "Nomis",
		Telephone: "+44(0) 132 9444972",
	}
}

func main() {

	var mongoURL string
	flag.StringVar(&mongoURL, "mongo-url", "localhost:27017", "mongoDB URL")
	flag.Parse()

	ctx := context.Background()
	downloadFile(ctx)

	conn, err := mongodriver.Open(&mongodriver.MongoDriverConfig{ClusterEndpoint: mongoURL, Database: "datasets", ConnectTimeout: 5 * time.Second})
	if err != nil {
		log.Fatal(ctx, "failed to initialise mongo", err)
		os.Exit(1)
	}
	defer func(conn *mongodriver.MongoConnection) { _ = conn.Close(ctx) }(conn)

	fileLocation := "./NOMIS/def.sdmx.json"
	f, err := os.Open(fileLocation)
	if err != nil {
		log.Fatal(ctx, "failed to open file", err)
		os.Exit(1)
	}

	b, err := io.ReadAll(f)
	if err != nil {
		log.Error(ctx, "failed to read json file as a byte array", err)
		os.Exit(1)
	}

	// var censusData CenStructure
	res := CenStructure{}
	if err := json.Unmarshal(b, &res); err != nil {
		logData := log.Data{"json file": res}
		log.Error(ctx, "failed to unmarshal json", err, logData)
		fmt.Println("error")
		return
	}

	for keyIndex := range res.Structure.Keyfamilies.Keyfamily {
		var annotations = res.Structure.Keyfamilies.Keyfamily[keyIndex].Annotations.Annotation
		censusEditionData := models.EditionUpdate{}
		censusInstances := models.Version{}
		mapData := models.Dataset{}
		var cenId string
		for censusId := range annotations {
			annoIndex := res.Structure.Keyfamilies.Keyfamily[keyIndex].Annotations.Annotation[censusId]
			if annoIndex.Title == "Mnemonic" {
				ref := annoIndex.Text.(string)
				extractId := strings.Split(ref, census2011)
				if len(extractId) < 2 {
					log.Error(ctx, "error mnemonic length invalid", errors.New("error mnemonic length invalid"))
					os.Exit(1)
				}
				cenId = extractId[1]
			}
		}
		title := res.Structure.Keyfamilies.Keyfamily[keyIndex].Name.Value

		mapData.Title = CheckTitle(title)
		datasetUrl := "http://127.0.0.1:12345/datasets/"
		instanceUrl := "http://127.0.0.1:12345/instances/"
		editionUrl := "/editions"
		versionUrl := "/versions"

		createEditionLink := fmt.Sprintf("%s%s%s", datasetUrl, cenId, editionUrl)
		createLatestVersion := fmt.Sprintf("%s%s%s%s%s%s", datasetUrl, cenId, editionUrl, "/"+censusYear, versionUrl, "/"+censusVersion)
		mapData.Links = &models.DatasetLinks{
			Editions:      &models.LinkObject{HRef: createEditionLink},
			LatestVersion: &models.LinkObject{HRef: createLatestVersion},
			Self:          &models.LinkObject{HRef: fmt.Sprintf("%s%s", datasetUrl, cenId)},
		}
		mapData.Contacts = []models.ContactDetails{
			CensusContactDetails(),
		}

		mapData.License = "Open Government Licence v3.0"
		mapData.NationalStatistic = &censusNationalStatistic
		mapData.NextRelease = "To Be Confirmed"
		mapData.ReleaseFrequency = "Decennially"
		mapData.State = "published"
		mapData.Type = "nomis"

		// Model to generate editions document in mongodb
		generalModel := &models.Edition{
			Edition: censusYear,
			Links: &models.EditionUpdateLinks{
				Dataset:       &models.LinkObject{HRef: fmt.Sprintf("%s%s", datasetUrl, cenId), ID: cenId},
				LatestVersion: &models.LinkObject{HRef: fmt.Sprintf("%s%s%s%s%s%s%s", datasetUrl, cenId, editionUrl, "/"+censusYear, versionUrl, "/", censusVersion), ID: censusVersion},
				Self:          &models.LinkObject{HRef: fmt.Sprintf("%s%s%s%s", datasetUrl, cenId, "/editions/", censusYear)},
				Versions:      &models.LinkObject{HRef: fmt.Sprintf("%s%s%s%s%s", datasetUrl, cenId, editionUrl, "/"+censusYear, versionUrl)},
			},
			State: "published",
		}

		id, err := uuid.NewV4()
		if err != nil {
			logData := log.Data{"json file": res}
			log.Error(ctx, "failed to create UUID for censusEditionData.ID", err, logData)
			fmt.Println("error")
			return
		}
		censusEditionData.ID = id.String()
		censusEditionData.Next = generalModel
		censusEditionData.Current = generalModel

		//Model to generate instances documents in mongodb
		genId, err := uuid.NewV4()
		if err != nil {
			logData := log.Data{"json file": res}
			log.Error(ctx, "failed to create UUID for generateId", err, logData)
			fmt.Println("error")
			return
		}
		generateId := genId.String()
		censusInstances = models.Version{
			Edition:     censusYear,
			ID:          generateId,
			LastUpdated: mapData.LastUpdated,
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{HRef: fmt.Sprintf("%s%s", datasetUrl, cenId), ID: cenId},
				Edition: &models.LinkObject{HRef: fmt.Sprintf("%s%s%s%s%s", datasetUrl, cenId, editionUrl, "/", censusYear), ID: censusYear},
				Self:    &models.LinkObject{HRef: fmt.Sprintf("%s%s", instanceUrl, generateId)},
				Version: &models.LinkObject{HRef: fmt.Sprintf("%s%s%s%s%s%s", datasetUrl, cenId, "/editions/", censusYear, versionUrl, "/"+censusVersion), ID: censusVersion},
			},
			State:      "published",
			Version:    1,
			UsageNotes: &[]models.UsageNote{},
		}
		var metaTitleInfo [5]string

		for index1 := range annotations {
			str1 := annotations[index1].Title
			if strings.HasPrefix(str1, "MetadataTitle") {
				title = annotations[index1].Text.(string)
				splitTitle := strings.Split(str1, "MetadataTitle")
				if splitTitle[1] == "" {
					num = 0
				} else {
					temp, _ := strconv.Atoi(splitTitle[1])
					num = temp + 1
				}
				metaTitleInfo[num] = title
			}
		}
		for index := range annotations {
			var example string
			var annotation = res.Structure.Keyfamilies.Keyfamily[keyIndex].Annotations.Annotation[index]
			str := annotation.Title
			switch str {
			case "MetadataText0":
				mapData.Description = annotation.Text.(string)
			case "Keywords":
				keywrd := annotation.Text.(string)
				var split = strings.Split(keywrd, ",")
				mapData.Keywords = split
			case "LastUpdated":
				tt := annotation.Text.(string)
				t, parseErr := time.Parse("2006-01-02 15:04:05", tt)
				if parseErr != nil {
					log.Error(ctx, "error parsing date", err)
					os.Exit(1)
				}
				mapData.LastUpdated = t
				generalModel.LastUpdated = mapData.LastUpdated
			case "Units":
				mapData.UnitOfMeasure = annotation.Text.(string)
			case "Mnemonic":
				mapData.NomisReferenceURL = "https://www.nomisweb.co.uk/census/2011/" + cenId
				mapData.ID = cenId
			case "FirstReleased":
				releaseDt := annotation.Text.(string)
				rd, err := time.Parse("2006-01-02 15:04:05", releaseDt)
				if err != nil {
					log.Error(ctx, "failed to parse date correctly", err)
					os.Exit(1)
				}
				censusInstances.ReleaseDate = rd.Format("2006-01-02T15:04:05.000Z")
			}
			if strings.HasPrefix(str, "MetadataText") {
				if str != "MetadataText0" {
					example = CheckSubString(annotation.Text.(string))
				}
				splitMetaData := strings.Split(str, "MetadataText")
				txtNumber, _ := strconv.Atoi(splitMetaData[1])
				var note, title string
				if splitMetaData[1] == "" {
					note, title = ReplaceStatDis(example, metaTitleInfo[0])
					appendUsageNote(censusInstances.UsageNotes, note, title)
				} else if splitMetaData[1] != "0" {
					note, title = ReplaceStatDis(example, metaTitleInfo[txtNumber+1])
					appendUsageNote(censusInstances.UsageNotes, note, title)
				}
			}
		}

		datasetDoc := &models.DatasetUpdate{
			ID:      mapData.ID,
			Current: &mapData,
			Next:    &mapData,
		}

		createDatasetsDocument(ctx, cenId, datasetDoc, conn)
		createEditionsDocument(ctx, cenId, censusEditionData, conn)
		createInstancesDocument(ctx, cenId, censusInstances, conn)
	}
	fmt.Println("\ndatasets, instances and editions have been added to datasets db")
}

// Inserts a document in the datasets collection
func createDatasetsDocument(ctx context.Context, id string, class interface{}, conn *mongodriver.MongoConnection) {
	var err error
	logData := log.Data{"data": class}
	if _, err = conn.Collection("datasets").UpsertById(ctx, id, bson.M{"$set": class}); err != nil {
		log.Error(ctx, "failed to upsert data in dataset collection", err, logData)
		os.Exit(1)
	}
}

// Inserts a document in the editions collection
func createEditionsDocument(ctx context.Context, id string, class interface{}, conn *mongodriver.MongoConnection) {
	var err error
	logData := log.Data{"data": class}
	selector := bson.M{
		"current.links.dataset.id": id,
	}
	if err = upsertData(ctx, selector, class, conn, "editions", logData); err != nil {
		log.Error(ctx, " failed to insert data in collection", err, logData)
		os.Exit(1)
	}
}

// Inserts a document in the instances collection
func createInstancesDocument(ctx context.Context, id string, class interface{}, conn *mongodriver.MongoConnection) {
	var err error
	logData := log.Data{"data": class}
	selector := bson.M{
		"links.dataset.id": id,
	}
	if err = upsertData(ctx, selector, class, conn, "instances", logData); err != nil {
		log.Error(ctx, " failed to insert data in collection", err, logData)
		os.Exit(1)
	}
}

// Updates document in the specific collection
func upsertData(ctx context.Context, selector, class interface{}, conn *mongodriver.MongoConnection, document string, logData log.Data) error {
	var err error
	if _, err = conn.Collection(document).Upsert(ctx, selector, bson.M{"$set": class}); err != nil {
		log.Error(ctx, "failed to upsert data in collection", err, logData)
		return err
	}
	err = nil
	return err
}

// Download a file from nomis website for census 2011 data
func downloadFile(ctx context.Context) {
	fullURLFile = "https://www.nomisweb.co.uk/api/v01/dataset/def.sdmx.json?search=*c2011*"

	// Build fileName from fullPath
	fileURL, err := url.Parse(fullURLFile)
	if err != nil {
		log.Error(ctx, "error parsing the file", err)
		os.Exit(1)
	}
	path := fileURL.Path
	segments := strings.Split(path, "/")
	fileName = segments[len(segments)-1]
	newFileName := "./NOMIS/" + fileName

	// Create blank file
	file, err := os.Create(newFileName)
	if err != nil {
		log.Error(ctx, "error creating the file", err)
		os.Exit(1)
	}
	client := http.Client{
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			r.URL.Opaque = r.URL.Path
			return nil
		},
	}

	// Put content on file
	resp, err := client.Get(fullURLFile)

	if err != nil {
		log.Error(ctx, "error writing the file", err)
		os.Exit(1)
	}
	defer closeBody(ctx, resp.Body)
	size, err := io.Copy(file, resp.Body)
	if err != nil {
		log.Error(ctx, "error copying a file", err)
		os.Exit(1)
	}
	defer closeFile(ctx, file)
	fmt.Printf("Downloaded a file %s with size %d", fileName, size)
}

func closeBody(ctx context.Context, b io.ReadCloser) {
	if err := b.Close(); err != nil {
		log.Error(ctx, "error closing response body", err)
	}
}

func closeFile(ctx context.Context, f *os.File) {
	if err := f.Close(); err != nil {
		log.Error(ctx, "error closing file", err)
	}
}

// CheckSubString checks if the string has substrings http and [Statistical Disclosure Control].
// If both the substrings exists then it adds parenthesis where necessary and swaps the pattern (url)[text] to [text](url)
// so it can be displayed correctly. If substring does not exist then it returns the original string
func CheckSubString(existingStr string) string {
	return httpRegex.ReplaceAllString(existingStr, `$2($1)`)
}

func CheckTitle(sourceStr string) string {
	return titleRegex.ReplaceAllString(sourceStr, `$1`)
}

func ReplaceStatDis(note string, title string) (string, string) {
	if title == "Statistical Disclosure Control" {
		note = censusPersonalData
		title = "Protecting personal data"
	}
	return note, title
}
func appendUsageNote(cenInst *[]models.UsageNote, note string, title string) *[]models.UsageNote {
	*cenInst = append(*cenInst, models.UsageNote{
		Note:  note,
		Title: title,
	})
	return cenInst
}
