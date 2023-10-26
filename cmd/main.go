package main

import(
	"os"
	"fmt"
	"encoding/json"
	"io"
	"bytes"
	"compress/gzip"
	"context"
	"time"
	"strconv"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	logLevel		=	zerolog.DebugLevel // InfoLevel DebugLevel
	version			=	"lambda-go-firehore-transformation version 1.9"
	region			=	"us-east-2"
)

type KinesisFirehoseEventRecordData struct {
	SubscriptionFilters []string `json:"subscriptionFilters"`
}

func init() {
	log.Debug().Msg("init")
	zerolog.SetGlobalLevel(logLevel)
	getEnv()
}

// Loading ENV variables
func getEnv(){
	if os.Getenv("LOG_LEVEL") !=  "" {
		if (os.Getenv("LOG_LEVEL") == "DEBUG"){
			logLevel = zerolog.DebugLevel
		}else if (os.Getenv("LOG_LEVEL") == "INFO"){
			logLevel = zerolog.InfoLevel
		}else if (os.Getenv("LOG_LEVEL") == "ERROR"){
				logLevel = zerolog.ErrorLevel
		}else {
			logLevel = zerolog.DebugLevel
		}
	}
	if os.Getenv("VERSION") !=  "" {
		version = os.Getenv("VERSION")
	}

	if os.Getenv("AWS_REGION") !=  "" {
		region = os.Getenv("AWS_REGION")
	}
}

func main() {
	log.Debug().Msg("main")
	log.Debug().Str("version", version).Msg("")

	// Start lambda handler
	lambda.Start(lambdaHandler)
}

func Unzip(data []byte) ([]byte, error) {
	log.Debug().Msg("Unzip")

	rdata := bytes.NewReader(data)
	r, err := gzip.NewReader(rdata)
	if err != nil {
		log.Error().Err(err).Msg("NewReader")
	 	return nil, err
	}

	uncompressedData, err := io.ReadAll(r)
	if err != nil {
		log.Error().Err(err).Msg("uncompressedData")
		return nil, err
	}
	return uncompressedData, nil
}

func lambdaHandler(ctx context.Context, event events.KinesisFirehoseEvent) (events.KinesisFirehoseResponse, error) {
	log.Debug().Msg("lambdaHandler")

	//fmt.Printf("InvocationID: %s\n", event.InvocationID)
	//fmt.Printf("DeliveryStreamArn: %s\n", event.DeliveryStreamArn)

	var response events.KinesisFirehoseResponse

	for _, record := range event.Records {
		//fmt.Printf("RecordID: %s\n", record.RecordID)
		//fmt.Printf("ApproximateArrivalTimestamp: %s\n", record.ApproximateArrivalTimestamp)

		// Transform data: ToUpper the data
		var transformedRecord events.KinesisFirehoseResponseRecord
		transformedRecord.RecordID = record.RecordID
		transformedRecord.Result = events.KinesisFirehoseTransformedStateOk
		//transformedRecord.Data = []byte(strings.ToUpper(string(record.Data)) + "\n" ) #When test withouy cloudwatach logs

		//Uncompress
		parsed, err := Unzip(record.Data)
		if err != nil {
			log.Printf("GetRecords ERROR: %v\n", err)
			break
		}
		//transformedRecord.Data = []byte(strings.ToUpper(string(parsed)) + "\n" ) #with cloudwatch logs and Uncropress
		var metaData events.KinesisFirehoseResponseRecordMetadata
		var recordData KinesisFirehoseEventRecordData
		partitionKeys := make(map[string]string)

		fmt.Printf(" %v\n", string(parsed) )

		json.Unmarshal(parsed, &recordData)
		partitionKeys["subscriptionFilters"] = string(recordData.SubscriptionFilters[0])

		currentTime := time.Now()
		partitionKeys["year"] = strconv.Itoa(currentTime.Year())
		partitionKeys["month"] = strconv.Itoa(int(currentTime.Month()))
		partitionKeys["day"] = strconv.Itoa(currentTime.Day())
		partitionKeys["hour"] = strconv.Itoa(currentTime.Hour())

		metaData.PartitionKeys = partitionKeys
		transformedRecord.Metadata = metaData
		transformedRecord.Data = []byte(string(parsed) + "\n" )

		fmt.Printf(" transformedRecord.Metadata %v\n", transformedRecord.Metadata )
		fmt.Printf(" transformedRecord.Data %v\n", string(transformedRecord.Data) )
		
		response.Records = append(response.Records, transformedRecord)
	}

	fmt.Printf("%s\n",response)

	return response, nil
}