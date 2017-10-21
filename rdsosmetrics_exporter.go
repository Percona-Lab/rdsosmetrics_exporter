package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/version"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

const (
	namespace    = "rdsosmetrics"
	logGroupName = "RDSOSMetrics"
)

type Exporter struct {
	desc map[string]*prometheus.Desc
}

func NewExporter() (*Exporter, error) {
	desc := make(map[string]*prometheus.Desc)

	return &Exporter{
		desc: desc,
	}, nil
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	for _, region := range endpoints.AwsPartition().Services()[endpoints.RdsServiceID].Regions() {
		regionID := region.ID()
		sess := session.Must(session.NewSession(&aws.Config{
			Region: aws.String(regionID),
		}))
		svc := cloudwatchlogs.New(sess)
		DescribeLogStreamsOutput, err := svc.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
			LogGroupName: aws.String(logGroupName),
		})
		if err != nil {
			// region without RDS Enhanced Monitoring
			continue
		}

		for _, LogStream := range DescribeLogStreamsOutput.LogStreams {
			GetLogEventsOutput, err := svc.GetLogEvents(&cloudwatchlogs.GetLogEventsInput{
				Limit:         aws.Int64(1),
				LogGroupName:  aws.String(logGroupName),
				LogStreamName: LogStream.LogStreamName,
			})
			if err != nil {
				log.Fatalln(err)
			}

			var message interface{}
			err = json.Unmarshal([]byte(*GetLogEventsOutput.Events[0].Message), &message)
			if err != nil {
				// not JSON? skipping
				continue
			}

			for key, value := range message.(map[string]interface{}) {
				switch vvalue := value.(type) {
				case float64:
					e.addDesc("", key)
				case map[string]interface{}:
					for kkey, vvvalue := range vvalue {
						switch vvvalue.(type) {
						case float64:
							e.addDesc(key, kkey)
						}
					}
				case []interface{}:
					for i, u := range vvalue {
						switch vvvalue := u.(type) {
						case map[string]interface{}:
							for kkey, vvvvalue := range vvvalue {
								switch vvvvalue.(type) {
								case float64:
									e.addDesc(key, strconv.Itoa(i)+"_"+kkey)
								}
							}
						}
					}
				}
			}
		}
	}

	for _, v := range e.desc {
		ch <- v
	}
}

func (e *Exporter) addDesc(subsystem string, name string) {
	FQName := prometheus.BuildFQName(namespace, subsystem, name)
	if _, ok := e.desc[FQName]; !ok {
		e.desc[FQName] = prometheus.NewDesc(
			strings.ToLower(FQName),
			"Authomatially parsed metric from "+logGroupName+" Log Group",
			[]string{
				"region",
				"instanceID",
			},
			nil,
		)
	}
}

func (e *Exporter) updateMetric(ch chan<- prometheus.Metric, region string, instanceID string, subsystem string, name string, value float64) {
	FQName := prometheus.BuildFQName(namespace, subsystem, name)
	ch <- prometheus.MustNewConstMetric(
		e.desc[FQName],
		prometheus.UntypedValue,
		value,
		region,
		instanceID,
	)
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	for _, region := range endpoints.AwsPartition().Services()[endpoints.RdsServiceID].Regions() {
		regionID := region.ID()
		sess := session.Must(session.NewSession(&aws.Config{
			Region: aws.String(regionID),
		}))
		svc := cloudwatchlogs.New(sess)
		DescribeLogStreamsOutput, err := svc.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
			LogGroupName: aws.String(logGroupName),
		})
		if err != nil {
			// region without RDS Enhanced Monitoring
			continue
		}

		for _, LogStream := range DescribeLogStreamsOutput.LogStreams {
			GetLogEventsOutput, err := svc.GetLogEvents(&cloudwatchlogs.GetLogEventsInput{
				Limit:         aws.Int64(1),
				LogGroupName:  aws.String(logGroupName),
				LogStreamName: LogStream.LogStreamName,
			})
			if err != nil {
				log.Fatalln(err)
			}

			var message interface{}
			err = json.Unmarshal([]byte(*GetLogEventsOutput.Events[0].Message), &message)
			if err != nil {
				// not JSON? skipping
				continue
			}

			instanceID := message.(map[string]interface{})["instanceID"].(string)
			for key, value := range message.(map[string]interface{}) {
				switch vvalue := value.(type) {
				case float64:
					e.updateMetric(ch, regionID, instanceID, "", key, vvalue)
				case map[string]interface{}:
					for kkey, vvvalue := range vvalue {
						switch vvvvalue := vvvalue.(type) {
						case float64:
							e.updateMetric(ch, regionID, instanceID, key, kkey, vvvvalue)
						}
					}
				case []interface{}:
					for i, u := range vvalue {
						switch vvvalue := u.(type) {
						case map[string]interface{}:
							for kkey, vvvvalue := range vvvalue {
								switch vvvvvalue := vvvvalue.(type) {
								case float64:
									e.updateMetric(ch, regionID, instanceID, key, strconv.Itoa(i)+"_"+kkey, vvvvvalue)
								}
							}
						}
					}
				}
			}
		}
	}
}

func main() {
	log.Infoln("Starting rdsosmetrics_exporter", version.Info())
	log.Infoln("Build context", version.BuildContext())

	exporter, err := NewExporter()
	if err != nil {
		log.Fatalln(err)
	}
	prometheus.MustRegister(exporter)

	log.Infoln("Started")

	http.Handle("/metrics", prometheus.Handler())
	log.Fatal(http.ListenAndServe(":9377", nil))
}
