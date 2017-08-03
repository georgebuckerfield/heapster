package cloudwatch

import (
	"net/url"
	"sync"
	"time"

	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/cloudwatch"
	"k8s.io/heapster/metrics/core"
)

type CloudwatchSink struct {
	cloudwatchClient *cloudwatch.CloudWatch
	sync.RWMutex
}

func (sink *CloudwatchSink) Name() string {
	return "Cloudwatch Sink"
}

func (sink *CloudwatchSink) ExportData(dataBatch *core.DataBatch) {
	sink.Lock()
	defer sink.Unlock()

	var datums []cloudwatch.MetricDatum
	for _, metricSet := range dataBatch.MetricSets {
		var metricDimensions []cloudwatch.Dimension
		for dimensionName, dimensionValue := range metricSet.Labels {
			dimension := cloudwatch.Dimension{
				Name:  dimensionName,
				Value: dimensionValue,
			}
			metricDimensions = append(metricDimensions, dimension)
		}
		for metricName, metricValue := range metricSet.MetricValues {
			datum := cloudwatch.MetricDatum{
				Dimensions: metricDimensions,
				MetricName: metricName,
				Value:      float64(metricValue.FloatValue),
			}
			datums = append(datums, datum)
		}
	}
	if len(datums) > 0 {
		sink.sendRequest(datums)
	}
}

func (sink *CloudwatchSink) sendRequest(metrics []cloudwatch.MetricDatum) error {
	_, err := sink.cloudwatchClient.PutMetricData(metrics)
	if err != nil {
		return err
	}
	return nil
}

func (sink *CloudwatchSink) Stop() {
	// Nothing to do here.
}

func CreateCloudwatchSink(uri *url.URL) (core.DataSink, error) {
	region := aws.Regions["eu-west-1"]

	auth, err := aws.GetAuth("your_AccessKeyId", "your_SecretAccessKey", "", time.Now())
	if err != nil {
		return nil, err
	}

	client, err := cloudwatch.NewCloudWatch(auth, region.CloudWatchServicepoint)
	if err != nil {
		return nil, err
	}

	sink := &CloudwatchSink{
		cloudwatchClient: client,
	}
	return sink, nil
}
