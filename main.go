package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	accessKey       = os.Getenv("ACCESS_KEY")
	secretAccessKey = os.Getenv("SECRET_ACCESS_KEY")

	// Lambda Config Notes: Bucket name has format "[bucket-name]/path/to/file.ext" -- path (aka key) becomes "//path//to//file.ext"
	sourceBucketName = os.Getenv("SOURCE_BUCKET_NAME")

	// Lambda Config Notes: Source IP Addresses format should be comma-separated list of IP Addresses from which outbound traffic should be tracked
	sourceIPAddresses = os.Getenv("SOURCE_IP_ADDRESSES")

	// Lambda Config Notes: Bucket name has format /path/to/file[[timestamp]].ext where "[[timestamp]]" is literally the string "[[timestamp]]"
	destBucketName = os.Getenv("DEST_BUCKET_NAME")

	now              = time.Now()
	year, month, day = now.Date()
	timestamp        = fmt.Sprintf("%d-%d-%d", day, int(month), year)

	timestampRegexp = regexp.MustCompile("\\[\\[timestamp\\]\\]")
)

func HandleRequest(ctx context.Context) (string, error) {
	log.Println("Attempting to parse VPC logs from %s", sourceBucketName)

	config := &aws.Config{
		Region:                         aws.String("us-east-1"),
		Credentials:                    credentials.NewStaticCredentials(accessKey, secretAccessKey, ""),
		DisableRestProtocolURICleaning: aws.Bool(true), // May not be needed, but just to be safe
	}

	awsSession, err := session.NewSession(config)
	fatalIf(err)

	s3Client := s3.New(awsSession)

	sourceS3Bucket, sourceS3Key, err := parseBucketAndKeyFromFilePath(sourceBucketName)
	fatalIf(err)

	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(sourceS3Bucket),
		Key:    aws.String(sourceS3Key),
	}

	buf := aws.NewWriteAtBuffer([]byte{})
	downloader := s3manager.NewDownloaderWithClient(s3Client)
	_, err = downloader.Download(buf, getObjectInput)
	fatalIf(err)

	reader := bufio.NewReader(bytes.NewReader(buf.Bytes()))
	outboundVPCLogs := []byte{}
	for {
		//VPC Log has format <version> <account-id> <interface-id> <srcaddr> <dstaddr> <srcport> <dstport> <protocol> <packets> <bytes> <start> <end> <action> <log-status>
		//Outbound traffic is filtered by checking that the `srcaddr` is equal to our IP Address
		vpcLog, _, err := reader.ReadLine()
		if err != nil && err == io.EOF {
			break
		}
		fatalIf(err)

		vpcLogParts := strings.Split(string(vpcLog), " ")
		if len(vpcLogParts) > 3 {
			for _, sourceIPAddress := range strings.Split(sourceIPAddresses, ",") {
				if vpcLogParts[3] == sourceIPAddress {
					log.Printf("Found outbound log from %s: %s\n", sourceIPAddress, string(vpcLog))

					outboundVPCLogs = append(outboundVPCLogs, []byte(fmt.Sprintf("%s\n", string(vpcLog)))...)
				}
			}
		}
	}

	destS3Bucket, destS3Key, err := parseBucketAndKeyFromFilePath(destBucketName)

	putObjectInput := &s3.PutObjectInput{
		Bucket: aws.String(destS3Bucket),
		Key:    aws.String(timestampRegexp.ReplaceAllString(destS3Key, timestamp)), //Add timestamp to the name of the filex
		Body:   bytes.NewReader(outboundVPCLogs),
	}

	_, err = s3Client.PutObject(putObjectInput)
	fatalIf(err)

	return fmt.Sprintf("Done."), nil
}

func parseBucketAndKeyFromFilePath(filePath string) (string, string, error) {
	var (
		bucketName, key string
		parts           = strings.Split(bucketName, "/")
	)

	if len(parts) > 0 {
		bucketName = parts[0]
	} else {
		return bucketName, key, fmt.Errorf("File path string %s not in the correct format - expected [bucket-name]/path/to/file.csv", filePath)
	}

	if len(parts) > 1 {
		key = fmt.Sprintf("//%s", strings.Join(parts[1:], "//"))
	} else {
		return bucketName, key, fmt.Errorf("File path string %s not in the correct format - expected [bucket-name]/path/to/file.csv", filePath)
	}

	return bucketName, key, nil
}

func fatalIf(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	lambda.Start(HandleRequest)
}
