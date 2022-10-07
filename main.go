package main

import (
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/jessevdk/go-flags"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewLogger(logLevel zapcore.Level) *zap.Logger {
	level := zap.NewAtomicLevel()
	level.SetLevel(logLevel)

	myConfig := zap.Config{
		Level:             level,
		Encoding:          "json",
		DisableStacktrace: false,
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "Time",
			LevelKey:       "Level",
			NameKey:        "Name",
			CallerKey:      "Caller",
			MessageKey:     "Msg",
			StacktraceKey:  "St",
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}
	logger, _ := myConfig.Build()

	defer logger.Sync()
	return logger

}

type Logger interface {
	GetGroupAll() []string
	AssembleQuery(string) (string, error)
}

type Logs struct {
	client *cloudwatchlogs.CloudWatchLogs
	logger *zap.Logger
}

func New(session *session.Session) *Logs {
	return &Logs{
		client: cloudwatchlogs.New(session),
		logger: NewLogger(zap.DebugLevel),
	}
}

type options struct {
	Region    string `short:"r" long:"region" description:"" required:"false" default:"ap-northeast-1"`
	Profile   string `short:"p" long:"profile" description:"" required:"false"`
	GroupName string `short:"g" default:"/"`
	Start     string `long:"start" default:"2022-09-22T00:00:00+09:00"`
	End       string `long:"end" default:"2022-09-22T00:30:00+09:00"`
	KeyWord   string `long:"keyword"`
}

func ParseTime(target string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, target)
	if err != nil {
		return time.Time{}, err
	}

	return parsed, nil
}

func UnixMillisecond(target time.Time) int64 {
	return target.UnixNano() / int64(time.Millisecond)
}

var opts options

func (l Logs) GetGroupAll() []string {
	var sarr []string
	allGroups := cloudwatchlogs.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(opts.GroupName),
	}
	v, err := l.client.DescribeLogGroups(&allGroups)
	if err != nil {
		return sarr
	}
	for _, k := range v.LogGroups {
		sarr = append(sarr, *k.LogGroupName)
	}
	l.logger.Debug("sarr", zap.Strings("sarr", sarr))
	return sarr
}

func (l Logs) AssembleQuery(keyword string) (string, error) {
	return fmt.Sprintf("fields @timestamp, @message, @logStream | filter @message %v", keyword), nil
}

func (l Logs) DoQuery(logGroup, query string) (string, error) {
	l.logger.Debug("query", zap.String("q", query))
	ParsedFrom, err := ParseTime(opts.Start)
	if err != nil {
		return "", err
	}

	ParsedTo, err := ParseTime(opts.End)
	if err != nil {
		return "", err
	}

	input := &cloudwatchlogs.StartQueryInput{
		StartTime:    aws.Int64(UnixMillisecond(ParsedFrom)),
		EndTime:      aws.Int64(UnixMillisecond(ParsedTo)),
		LogGroupName: aws.String(logGroup),
		QueryString:  aws.String(query),
	}

	out, err := l.client.StartQuery(input)
	if err != nil {
		return "", err
	}

	return aws.StringValue(out.QueryId), nil
}

type QueryResult struct {
	Timestamp string
	LogStream string
	Message   string
}

func (l Logs) Result(query string, wait bool) ([]QueryResult, error) {

	input := &cloudwatchlogs.GetQueryResultsInput{QueryId: aws.String(query)}

	out, err := l.client.GetQueryResults(input)
	if err != nil {
		return nil, err
	}

	if wait {
		for {
			if *out.Status == "Complete" {
				break
			}
			out, err = l.client.GetQueryResults(input)
			if err != nil {
				return nil, err
			}
			l.logger.Debug("wait")
			time.Sleep(time.Second * 10)
		}
	}

	var result []QueryResult
	for _, record := range out.Results {

		var q QueryResult
		for _, element := range record {
			switch aws.StringValue(element.Field) {
			case "@timestamp":
				q.Timestamp = aws.StringValue(element.Value)
			case "@logStream":
				q.LogStream = aws.StringValue(element.Value)
			case "@message":
				q.Message = aws.StringValue(element.Value)
			default:
				continue
			}

		}
		result = append(result, q)

	}

	return result, nil

}

func getGroupAll(l Logger) []string {
	return l.GetGroupAll()
}

func assembleQuery(l Logger) (string, error) {
	return l.AssembleQuery(opts.KeyWord)
}

func main() {
	_, err := flags.ParseArgs(&opts, os.Args)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println(opts.KeyWord)

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Profile:           opts.Profile,
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			Region: aws.String(opts.Region),
		},
	}))

	cloudwatch := New(sess)
	q, err := assembleQuery(cloudwatch)
	if err != nil {
		fmt.Println(err)
	}
	for _, v := range getGroupAll(cloudwatch) {
		t, err := cloudwatch.DoQuery(v, q)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		res, err := cloudwatch.Result(t, true)
		if err != nil {
			cloudwatch.logger.Fatal("fatal")
		}
		for _, r := range res {
			fmt.Println(r.Message)
		}
	}
}
