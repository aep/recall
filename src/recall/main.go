package main

import (
    "fmt"
	"time"
	"bufio"
	"os"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

var svc *cloudwatchlogs.CloudWatchLogs;
var group  = "recall";
var stream = "stulle";
var token *string;

func create_group() {
    params := &cloudwatchlogs.CreateLogGroupInput{
        LogGroupName: aws.String(group), // Required
    }

    resp, err := svc.CreateLogGroup(params)

    if err != nil {
        //too lazay. ignore error ResourceAlreadyExistsException
        return
    }
    // Pretty-print the response data.
    fmt.Println(resp)
}

func create_stream() {
    params1 := &cloudwatchlogs.CreateLogStreamInput{
        LogGroupName:  aws.String(group),  // Required
        LogStreamName: aws.String(stream), // Required
    }
    _, err := svc.CreateLogStream(params1)

    if err != nil {
        fmt.Println(err.Error())
        os.Exit(32);
    }
}

func get_sequence_token() *string {
	params := &cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        aws.String(group),
		Descending:          aws.Bool(true),
		Limit:               aws.Int64(1),
		LogStreamNamePrefix: aws.String(stream),
	}
	resp, err := svc.DescribeLogStreams(params)

	if err != nil {
		// Print the error, cast err to awserr. Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		os.Exit(34);
	}

    // no such stream, create it and return token "nil"
    // this needs to be done on demand because the first token of a string
    // must be nil, but there is no way we can know if its the first beyond
    // knowing we just created it

    if (len(resp.LogStreams) < 1) {
        create_stream()
        return nil
    }

	return resp.LogStreams[0].UploadSequenceToken
}

func put_log_entry(message string) {
    if (token == nil) {
        token = get_sequence_token();
    }

	// log stuff
	params2 := &cloudwatchlogs.PutLogEventsInput{
		LogEvents: []*cloudwatchlogs.InputLogEvent{
			{
				Message:   aws.String(message),
				Timestamp: aws.Int64(time.Now().UnixNano()/1000/1000),
			},
		},
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		SequenceToken: token,
	}
	resp2, err := svc.PutLogEvents(params2)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		os.Exit(43);
	}

    token = resp2.NextSequenceToken
}

var bucket = []string{}

func send_bucket(){
    fmt.Printf("::>sending %u messages\n", len(bucket));
    if (len(bucket) < 1) {
        return
    }


    if (token == nil) {
        token = get_sequence_token();
    }

    events := []*cloudwatchlogs.InputLogEvent{}

    for _,element := range bucket {
        events = append(events,&cloudwatchlogs.InputLogEvent{
            Message:   aws.String(element),
            Timestamp: aws.Int64(time.Now().UnixNano()/1000/1000),
        })
    }

    params := &cloudwatchlogs.PutLogEventsInput{
        LogEvents: events,
        LogGroupName:  aws.String(group),
        LogStreamName: aws.String(stream),
        SequenceToken: token,
    }

	resp, err := svc.PutLogEvents(params)
	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		os.Exit(43);
	}

    token = resp.NextSequenceToken
    bucket =  []string{}
}

func main() {
	svc = cloudwatchlogs.New(session.New(),
	&aws.Config{Region: aws.String("eu-west-1")})

	if (len(os.Args) < 2) {
		fmt.Println("usage: recall <system-name>")
		os.Exit(3);
	}
	group = os.Args[1];
    stream, _ = os.Hostname();

    create_group();

    done     := make(chan bool)
    messages := make(chan string)

    go func(){
        scanner := bufio.NewScanner(os.Stdin)
        fmt.Printf("::cloning stdin to awslogs/%s/%s::\n\n", group, stream)
        for scanner.Scan() {
            line := scanner.Text()
            fmt.Println(line)
            if (len(line) < 1) {
                continue
            }
            messages <- line
        }
        done <- true
    }()


    for {
        select {
        case msg := <- messages:
            bucket = append(bucket, msg)
            if (len(bucket) > 30) {
                send_bucket()
            }
        case <-time.After(time.Second * 10):
            send_bucket()
        case <- done:
            send_bucket()
            os.Exit(0);
        }
    }

}
