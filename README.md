# lambda-go-firehore-transformation

POC for technical purpose

Lambda use with kinesis firehose stream as a transformation. In this case the lambda add a new line "/n" in each record to keep the files in S3 compatible with ATHENA queries

## Compile lambda

   Manually compile the function

    GOOD=linux GOARCH=amd64 go build -o ../build/main main.go

    zip -jrm ../build/main.zip ../build/main

    aws lambda update-function-code \
    --function-name lambda-go-auth-apigw \
    --zip-file fileb:///mnt/c/Eliezer/workspace/github.com/lambda-go-firehore-transformation/build/main.zip \
    --publish