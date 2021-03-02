package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/log.go/log"
	"github.com/globalsign/mgo"
	uuid "github.com/satori/go.uuid"
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
	censusNationalStatistic = false
	fileName                string
	fullURLFile             string
	title                   string
	metaTitle               []int
	num                     int
)

const (
	censusYear    string = "2011"
	censusVersion string = "1"
)

//CensusContactDetails returns the default values for contact details
func CensusContactDetails() models.ContactDetails {
	return models.ContactDetails{
		Email:     "support@nomisweb.co.uk",
		Name:      "Nomis",
		Telephone: "+44(0) 191 3342680",
	}
}

func main() {

	var mongoURL string
	flag.StringVar(&mongoURL, "mongo-url", "localhost:27017", "mongoDB URL")
	flag.Parse()

	downloadFile()
	ctx := context.Background()
	session, err := mgo.Dial(mongoURL)
	if err != nil {
		log.Event(ctx, "failed to initialise mongo", log.FATAL, log.Error(err))
		os.Exit(1)
	}
	defer session.Close()
	fileLocation := "./NOMIS/def.sdmx.json"
	f, err := os.Open(fileLocation)
	if err != nil {
		log.Event(ctx, "failed to open file", log.FATAL, log.Error(err))
		os.Exit(1)
	}

	b, err := ioutil.ReadAll(f)
	if err != nil {
		log.Event(ctx, "failed to read json file as a byte array", log.ERROR, log.Error(err))
		os.Exit(1)
	}

	//var censusData CenStructure
	res := CenStructure{}
	if err := json.Unmarshal(b, &res); err != nil {
		logData := log.Data{"json file": res}
		log.Event(ctx, "failed to unmarshal json", log.ERROR, log.Error(err), logData)
		fmt.Println("error")
		return
	}

	for index0, _ := range res.Structure.Keyfamilies.Keyfamily {
		censusEditionData := models.EditionUpdate{}
		mapData := models.Dataset{}
		cenId := res.Structure.Keyfamilies.Keyfamily[index0].ID
		mapData.Title = res.Structure.Keyfamilies.Keyfamily[index0].Name.Value
		mapData.ID = cenId

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

		//Model to generate editions document in mongodb
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

		censusEditionData.ID = uuid.NewV4().String()
		censusEditionData.Next = generalModel
		censusEditionData.Current = generalModel

		//Model to generate instances documents in mongodb
		generateId := uuid.NewV4().String()
		censusInstances := models.Version{
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
		var annotations = res.Structure.Keyfamilies.Keyfamily[index0].Annotations.Annotation
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
			var annotation = res.Structure.Keyfamilies.Keyfamily[index0].Annotations.Annotation[index]

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
					log.Event(ctx, "error parsing date", log.ERROR, log.Error(err))
					os.Exit(1)
				}
				mapData.LastUpdated = t
				generalModel.LastUpdated = mapData.LastUpdated

			case "Units":
				mapData.UnitOfMeasure = annotation.Text.(string)

			case "Mnemonic":
				ref := annotation.Text.(string)
				param := strings.Split(ref, "c2011")
				if len(param)<2{
					log.Event(nil, "error Mnemonic length invalid", log.ERROR)
					os.Exit(1)
				}
				mapData.NomisReferenceURL = "https://www.nomisweb.co.uk/census/2011/" + param[1]

			case "FirstReleased":
				releaseDt := annotation.Text.(string)
				rd, err := time.Parse("2006-01-02 15:04:05", releaseDt)
				if err != nil {
					log.Event(ctx, "failed to parse date correctly", log.ERROR, log.Error(err))
					os.Exit(1)
				}
				censusInstances.ReleaseDate = rd.Format("2006-01-02T15:04:05.000Z")
			}

			if strings.HasPrefix(str, "MetadataText") {
				if str != "MetadataText0" {
					example, err = CheckSubString(annotation.Text.(string))
					if err != nil {
						log.Event(nil, "failed to get metadatatext", log.ERROR, log.Error(err))
						os.Exit(1)
					}
				}
				splitMetaData := strings.Split(str, "MetadataText")
				txtNumber, _ := strconv.Atoi(splitMetaData[1])
				if splitMetaData[1] == "" && splitMetaData[1] != "0" {
					*censusInstances.UsageNotes = append(*censusInstances.UsageNotes, models.UsageNote{
						Note:  example,
						Title: metaTitleInfo[0],
					})

				} else if splitMetaData[1] != "0" {
					*censusInstances.UsageNotes = append(*censusInstances.UsageNotes, models.UsageNote{
						Note:  example,
						Title: metaTitleInfo[txtNumber+1],
					})
				}

			}
		}
		datasetDoc := &models.DatasetUpdate{
			ID:      mapData.ID,
			Current: &mapData,
			Next:    &mapData,
		}

		createDocument(ctx, datasetDoc, session, "datasets")
		createDocument(ctx, censusEditionData, session, "editions")
		createDocument(ctx, censusInstances, session, "instances")
	}
	fmt.Println("\ndatasets, instances and editions have been added to datasets db")
}

//Inserts a document in the specific collection
func createDocument(ctx context.Context, class interface{}, session *mgo.Session, document string) {
	var err error
	logData := log.Data{"data": class}
	if err = session.DB("datasets").C(document).Insert(class); err != nil {
		log.Event(ctx, "failed to insert data in collection", log.ERROR, log.Error(err), logData)
		os.Exit(1)
	}
}

//Download a file from nomis website for census 2011 data
func downloadFile() {
	fullURLFile = "https://www.nomisweb.co.uk/api/v01/dataset/def.sdmx.json?search=*c2011*"

	// Build fileName from fullPath
	fileURL, err := url.Parse(fullURLFile)
	if err != nil {
		fmt.Println("error Parsing")
		os.Exit(1)
	}
	path := fileURL.Path
	segments := strings.Split(path, "/")
	fileName = segments[len(segments)-1]
	newFileName := "./NOMIS/" + fileName

	// Create blank file
	file, err := os.Create(newFileName)
	if err != nil {
		fmt.Println("error creating the file")
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
		fmt.Println("error writing the file")
		os.Exit(1)
	}
	defer resp.Body.Close()
	size, err := io.Copy(file, resp.Body)
	defer file.Close()

	fmt.Printf("Downloaded a file %s with size %d", fileName, size)
}

/*checkSubString checks if the string has substrings http and [Statistical Disclosure Control].
If both the substrings exists then it adds parenthesis where necessary and swaps the pattern (url)[text] to [text](url)
so it can be displayed correctly. If substrings does not exists then it returns the original string*/
func CheckSubString(existingStr string) (string, error) {

	valueCheck, err := regexp.Compile(`(http[^\[]*)(\[[^\[]*\])`)
	if err != nil {
		return "", err
	}

	return valueCheck.ReplaceAllString(existingStr, `$2($1)`), nil
}
