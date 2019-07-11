package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/Jeffail/gabs"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/s3"
)

var err error

// KinesisRecords  --
var KinesisRecords []*kinesis.PutRecordsRequestEntry

// AWSRegion --
var AWSRegion = "us-west-2"

// CommandlineArguments --
// var CommandlineArguments = os.Args[1:][0]
var CommandlineArguments = os.Args[1:]

// ShowHelp --
func ShowHelp() {
	fmt.Println(`
    Usage: JSONTrafficker --input=<source> --kinesis-stream=<stream name>
    Options:
    --input=            -i  Input mode <s3,file,stdin>
                        s3://BucketName/Key
                        /tmp/logs.json
                        stdin|-

   --kinesis-stream=   -k  Target kinesis stream name  
   --region=           -r	AWS region <us-west-2>
   --help=             -h  Display this help and exit

   Examples:

   JSONTrafficker -i=s3://MylogsBucket/logdata.json -k security-logs

   JSONTrafficker -i=/tmp/logdata.json -k security-logs

   cat /tmp/logdata | JSONTrafficker -i=stdin`)
}

// JSONToKinesisBatch --
func JSONToKinesisBatch(Records [][]byte, KinesisDataStreamName string) {
	sess := session.Must(session.NewSession())
	if (len(Records)) == 0 || (len(Records)) > 500 {
		log.Fatal("Invalid record count")
	}
	// if len(Records) <= 500 {
	for n := range Records {
		ParsedJSON, err := gabs.ParseJSON(Records[n])
		if err != nil {
			log.Fatal(err)
		}
		DataEntry := kinesis.PutRecordsRequestEntry{
			Data:         []byte(ParsedJSON.Bytes()),
			PartitionKey: aws.String("0"),
		}
		KinesisRecords = append(KinesisRecords, &DataEntry)
		// return
	}
	log.Println("Writing", len(Records), "records to", KinesisDataStreamName)

	var kin = kinesis.New(sess)
	_, err := kin.PutRecords(
		&kinesis.PutRecordsInput{
			Records:    KinesisRecords,
			StreamName: aws.String(KinesisDataStreamName),
		})
	if err != nil {
		log.Fatal("An error has occured", err)
	} else {
		log.Println("Record delivery complete")

	}

	// }
	KinesisRecords = nil
}

func main() {
	var JSONContainer *gabs.Container
	var KinesisStreamName string
	var output []rune
	var TestDataSet [][]byte

	if len(CommandlineArguments) > 0 {
		KinesisStreamNameArg := regexp.MustCompile("--kinesis-stream|-ks")
		AWSRegionArg := regexp.MustCompile("--region|-r")
		HelpArg := regexp.MustCompile("--help|-h")
		InputArg := regexp.MustCompile("--input|-i")
		for i := range CommandlineArguments {
			ArgBytes := []byte(CommandlineArguments[i])
			Arg := CommandlineArguments[i]
			switch {
			case KinesisStreamNameArg.Match(ArgBytes):
				KinesisStreamName = strings.Split(Arg, "=")[1]
				fmt.Println(KinesisStreamName)
			case HelpArg.Match(ArgBytes):
				ShowHelp()
				os.Exit(0)
			case AWSRegionArg.Match(ArgBytes):
				AWSRegion = string(ArgBytes)
			case InputArg.Match(ArgBytes):
				// Grab bytes from file
				fmt.Println(string(ArgBytes))
				InputMode := strings.Split(Arg, "=")[1]
				varStdin := regexp.MustCompile("stdin")
				varS3Bucket := regexp.MustCompile("s3|S3")
				// Parse JSON from stdin
				if varStdin.Match([]byte(InputMode)) {
					reader := bufio.NewReader(os.Stdin)
					for {
						input, _, err := reader.ReadRune()
						if err != nil && err == io.EOF {
							break
						}
						output = append(output, input)
					}
					JSONContainer, err = gabs.ParseJSON([]byte(string(output)))
				}
				// Parse JSON from file
				if !varStdin.Match([]byte(InputMode)) && !varS3Bucket.Match([]byte(InputMode)) {
					Filename := strings.Split(Arg, "=")[1]
					FileBytes, err := ioutil.ReadFile(Filename)
					if err != nil {
						log.Fatal(err)
					}
					JSONContainer, err = gabs.ParseJSON(FileBytes)
					if err != nil {
						log.Fatal(err)
					}
				}
				if varS3Bucket.Match([]byte(InputMode)) {
					S3Object := strings.Split(Arg, "=")[1]
					// s3://bucketname/key
					S3Object = strings.Replace(S3Object, "s3://", "", -1)
					BucketName := strings.SplitN(S3Object, "/", 2)[0]
					FileName := "/" + strings.SplitN(S3Object, "/", 2)[1]
					sess := session.New()
					S3Client := s3.New(sess)
					S3InputObject := s3.GetObjectInput{
						Bucket: aws.String(BucketName),
						Key:    aws.String(FileName),
					}
					fmt.Println("Attempting to retrieve log file", BucketName, FileName)
					LogObject, err := S3Client.GetObject(&S3InputObject)
					if err != nil {
						log.Fatal("An error occured when attempting to retrieve ", err, BucketName, FileName)
					}
					LogContents, err := ioutil.ReadAll(LogObject.Body)
					if err != nil {
						log.Fatal("An error occured when attempting to read", err, BucketName, FileName)
					}
					JSONContainer, err = gabs.ParseJSON(LogContents)
					if err != nil {
						log.Fatal("Could not open file")
					}
					// JSONContainer = JSONContainer.Search("Records")
				}
			}
		}
		Children, err := JSONContainer.Children()
		if err != nil {
			fmt.Println(JSONContainer.String())
			log.Fatal("Cannot parse JSON ", err)
		}
		Counter := 1
		Position := 0
		Limit := len(Children)
		for x := range Children {
			// Convert time string to datetime
			if Children[x].Exists("UtcTime") {
				ogTimeStamp := Children[x].Path("UtcTime").String()
				newTimeStamp := strings.Replace(ogTimeStamp, `"`, "", 2)
				TimeStamp, err := time.Parse("2006-01-02 15:04:05.000", newTimeStamp)
				if err != nil {
					log.Panic(err)
				}
				Children[x].SetP(TimeStamp, "UtcTime")
			}
			TestDataSet = append(TestDataSet, Children[x].Bytes())

			if Counter == 500 {
				JSONToKinesisBatch(TestDataSet, KinesisStreamName)
				TestDataSet = [][]byte{}
				Counter = 0
			}
			if Limit == Position+1 {
				JSONToKinesisBatch(TestDataSet, KinesisStreamName)
			}
			Counter++
			Position++
		}
	} else {
		ShowHelp()
	}
}
