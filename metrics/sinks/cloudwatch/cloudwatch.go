// Copyright 2017 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudwatch

import (
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/cloudwatch"
	"github.com/golang/glog"
	"k8s.io/heapster/metrics/core"
)

const (
	metricBatchSize = 10
	awsRegionEnvVar = "AWS_REGION"
	awsAccessEnvVar = "AWS_ACCESS_KEY_ID"
	awsSecretEnvVar = "AWS_SECRET_ACCESS_KEY"
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
		metricDimensions := assignDimensions(metricSet.Labels)
		for metricName, rawValue := range metricSet.MetricValues {
			metricUnit := assignUnit(metricName)
			metricValue, err := convertMetricValue(rawValue)
			if err != nil {
				glog.Errorf("%v", err)
				break
			}
			datum := cloudwatch.MetricDatum{
				Dimensions: metricDimensions,
				MetricName: metricName,
				Value:      metricValue,
				Unit:       metricUnit,
			}
			datums = append(datums, datum)
			if len(datums) >= metricBatchSize {
				sink.sendRequest(datums)
				datums = nil
			}
		}
	}
	if len(datums) > 0 {
		sink.sendRequest(datums)
	}
}

func (sink *CloudwatchSink) sendRequest(metrics []cloudwatch.MetricDatum) {
	// TODO: set cloudwatch namespace to cluster name?
	r, err := sink.cloudwatchClient.PutMetricDataNamespace(metrics, "Kubernetes")
	if err != nil {
		glog.Errorf("%v", err)
	}
	glog.Info("Uploaded metrics: %s", r)
}

func (sink *CloudwatchSink) Stop() {
	// Nothing to do here.
}

func assignUnit(metricName string) string {
	var unit string
	switch metricName {
	case "cpu/usage":
		unit = "Seconds"
	case "filesystem/usage", "filesystem/limit", "filesystem/available":
		unit = "Bytes"
	case "memory/cache", "memory/limit", "memory/request", "memory/usage":
		unit = "Bytes"
	case "network/rx", "network/tx":
		unit = "Bytes"
	case "network/rx_rate", "network/tx_rate":
		unit = "Bytes/Second"
	case "uptime":
		unit = "Milliseconds"
	default:
		unit = "None"
	}
	return unit
}

func assignDimensions(labels map[string]string) []cloudwatch.Dimension {
	var dimensions []cloudwatch.Dimension
	if value, ok := labels["hostname"]; ok {
		hostnameDimension := cloudwatch.Dimension{
			Name:  "hostname",
			Value: value,
		}
		dimensions = append(dimensions, hostnameDimension)
	}
	if value, ok := labels["namespace_name"]; ok {
		namespaceDimension := cloudwatch.Dimension{
			Name:  "namespace",
			Value: value,
		}
		dimensions = append(dimensions, namespaceDimension)
	}
	if value, ok := labels["labels"]; ok {
		labelsDimension := cloudwatch.Dimension{
			Name:  "labels",
			Value: value,
		}
		dimensions = append(dimensions, labelsDimension)
	}
	if value, ok := labels["pod_name"]; ok {
		labelsDimension := cloudwatch.Dimension{
			Name:  "pod_name",
			Value: value,
		}
		dimensions = append(dimensions, labelsDimension)
	}
	if value, ok := labels["type"]; ok {
		labelsDimension := cloudwatch.Dimension{
			Name:  "component",
			Value: value,
		}
		dimensions = append(dimensions, labelsDimension)
	}
	if value, ok := labels["container_name"]; ok {
		labelsDimension := cloudwatch.Dimension{
			Name:  "container_name",
			Value: value,
		}
		dimensions = append(dimensions, labelsDimension)
	}
	return dimensions
}

func convertMetricValue(value core.MetricValue) (float64, error) {
	if float, ok := value.GetValue().(float32); ok {
		metricValue := float64(float)
		return metricValue, nil
	}
	if integer, ok := value.GetValue().(int64); ok {
		metricValue := float64(integer)
		return metricValue, nil
	}
	return float64(0), fmt.Errorf("unsupported value type")
}

func CreateCloudwatchSink(uri *url.URL) (core.DataSink, error) {
	accessKey := os.Getenv(awsAccessEnvVar)
	secretKey := os.Getenv(awsSecretEnvVar)
	auth, err := aws.GetAuth(accessKey, secretKey, "", time.Now())
	if err != nil {
		return nil, err
	}
	glog.Infof("authentication object created")

	region := aws.Regions[os.Getenv(awsRegionEnvVar)]
	client, err := cloudwatch.NewCloudWatch(auth, region.CloudWatchServicepoint)
	if err != nil {
		return nil, err
	}
	glog.Infof("cloudwatch service created")

	sink := &CloudwatchSink{
		cloudwatchClient: client,
	}
	glog.Infof("cloudwatch sink created")
	return sink, nil
}
