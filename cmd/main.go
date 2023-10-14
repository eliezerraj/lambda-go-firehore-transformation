package main

import(
	"os"
	//"fmt"
	"context"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

)

var (
	logLevel		=	zerolog.DebugLevel // InfoLevel DebugLevel
	version			=	"lambda-go-firehore-transformation version 1.1"
	region			=	"us-east-2"
)

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
		transformedRecord.Data = []byte(strings.ToUpper(string(record.Data)) + "\n" )

		response.Records = append(response.Records, transformedRecord)
	}

	return response, nil
}