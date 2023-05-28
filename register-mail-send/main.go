package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

type Input struct {
	MessageId string `json:"MessageId"`
	Subject   string `json:"Subject"`
	Message   string `json:"Message"`
}

func Handler(ctx context.Context, sqsEvent events.SQSEvent) error {

	// lambdaの環境変数からキューのURL、リージョンを取得
	topicArn := os.Getenv("TOPIC_ARN")
	region := os.Getenv("REGION")
	// AWSのセッションを作成
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))
	// SNSのクライアントを作成
	snsSvc := sns.New(sess)

	for _, record := range sqsEvent.Records {

		b := []byte(record.Body)
		var input Input
		err := json.Unmarshal(b, &input)
		if err != nil {
			return err
		}

		var entries []*sns.PublishBatchRequestEntry = make([]*sns.PublishBatchRequestEntry, 3)

		for i := range make([]int, 3) {
			messageJson := map[string]string{
				"default": "default message",
				"sqs":     input.Message,
			}
			bytes, err := json.Marshal(messageJson)
			if err != nil {
				fmt.Println("JSON marshal error: ", err)
			}
			messageForSQS := string(bytes)

			entries[i] = &sns.PublishBatchRequestEntry{
				Id:               aws.String(strconv.Itoa(i)),
				Message:          aws.String(messageForSQS),
				MessageStructure: aws.String("json"),
			}
		}

		_, err = snsSvc.PublishBatch(&sns.PublishBatchInput{
			PublishBatchRequestEntries: entries,
			TopicArn:                   &topicArn,
		})
		if err != nil {
			fmt.Println("Publish Error: ", err)
		}
	}

	return nil
}

func main() {
	lambda.Start(Handler)
}
