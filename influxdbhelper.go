package influxdbhelper

import (
	"context"
	"fmt"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"log"
	"strings"
	"time"
)

type InfluxDBClient struct {
	Config    *InfluxDBConfig
	Client    influxdb2.Client
	Writer    api.WriteAPI
	Querier   api.QueryAPI
	QueryFlux strings.Builder
	Err       error
	IsDebug   bool
	clone     int
}

type InfluxDBConfig struct {
	Url    string
	Token  string
	Org    string
	Bucket string
}

func Client(config *InfluxDBConfig) *InfluxDBClient {
	client := influxdb2.NewClient(config.Url, config.Token)
	writer := client.WriteAPI(config.Org, config.Bucket)
	query := client.QueryAPI(config.Org)
	influxdb := &InfluxDBClient{
		Config:  config,
		Client:  client,
		Writer:  writer,
		Querier: query,
		clone:   1,
	}
	return influxdb
}

func (i *InfluxDBClient) Error() error {
	client := i.getInstance()
	return client.Err
}

func (i *InfluxDBClient) Debug() *InfluxDBClient {
	client := i.getInstance()
	client.IsDebug = true
	return client
}

func (i *InfluxDBClient) Write(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) *InfluxDBClient {
	client := i.getInstance()
	point := influxdb2.NewPoint(measurement, tags, fields, ts)
	client.Writer.WritePoint(point)
	return client
}

func (i *InfluxDBClient) WritePoint(point *write.Point) *InfluxDBClient {
	client := i.getInstance()
	client.Writer.WritePoint(point)
	return client
}

func (i *InfluxDBClient) Flush() {
	client := i.getInstance()
	client.Writer.Flush()
}

func (i *InfluxDBClient) FromBucket(bucket string) *InfluxDBClient {
	client := i.getInstance()
	fromFlux := fmt.Sprintf("from(bucket: \"%s\")", bucket)
	client.QueryFlux.WriteString(fromFlux)
	return client
}

func (i *InfluxDBClient) Range(timeFrom string, timeTo string) *InfluxDBClient {
	client := i.getInstance()
	timeFromParse, err := time.Parse(time.DateTime, timeFrom)
	if err != nil {
		client.Err = err
		return client
	}
	timeToParse, err := time.Parse(time.DateTime, timeTo)
	if err != nil {
		client.Err = err
		return client
	}
	start := timeFromParse.Format("2006-01-02T15:04:05+08:00")
	end := timeToParse.Format("2006-01-02T15:04:05+08:00")
	rangeFlux := fmt.Sprintf("|> range(start: %s ,stop: %s)", start, end)
	client.QueryFlux.WriteString(rangeFlux)
	return client
}

func (i *InfluxDBClient) RangeRecent(rangeTime string) *InfluxDBClient {
	client := i.getInstance()
	rangeFlux := fmt.Sprintf("|> range(start: %s)", rangeTime)
	client.QueryFlux.WriteString(rangeFlux)
	return client
}

func (i *InfluxDBClient) Measurement(measurement string) *InfluxDBClient {
	client := i.getInstance()
	rangeFlux := fmt.Sprintf("|> filter(fn: (r) => r._measurement == \"%s\")", measurement)
	client.QueryFlux.WriteString(rangeFlux)
	return client
}

func (i *InfluxDBClient) Tag(tagKey string, tagValues ...string) *InfluxDBClient {
	client := i.getInstance()
	if len(tagValues) == 0 {
		return client
	}
	var filterTagsStr []string
	for _, v := range tagValues {
		temp := fmt.Sprintf("r.%s == \"%s\"", tagKey, v)
		filterTagsStr = append(filterTagsStr, temp)
	}
	filterTag := strings.Join(filterTagsStr, " or ")
	tagFlux := fmt.Sprintf("|> filter(fn: (r) => %s)", filterTag)
	client.QueryFlux.WriteString(tagFlux)
	return client
}

func (i *InfluxDBClient) Field(fieldValues ...string) *InfluxDBClient {
	client := i.getInstance()
	if len(fieldValues) == 0 {
		return client
	}
	var filterTagsStr []string
	for _, v := range fieldValues {
		temp := fmt.Sprintf("r._field == \"%s\"", v)
		filterTagsStr = append(filterTagsStr, temp)
	}
	filterTag := strings.Join(filterTagsStr, " or ")
	tagFlux := fmt.Sprintf("|> filter(fn: (r) => %s)", filterTag)
	client.QueryFlux.WriteString(tagFlux)
	return client
}

func (i *InfluxDBClient) Filter(filter string) *InfluxDBClient {
	client := i.getInstance()
	filterFlux := fmt.Sprintf("|> filter(fn: (r) => %s)", filter)
	client.QueryFlux.WriteString(filterFlux)
	return client
}

// AggregateWindow fn: mean,first,last...
func (i *InfluxDBClient) AggregateWindow(every string, fn string, createEmpty bool) *InfluxDBClient {
	client := i.getInstance()
	aggregateWindowFlux := fmt.Sprintf("|> aggregateWindow(every: %s, fn: %s, createEmpty: %t)", every, fn, createEmpty)
	client.QueryFlux.WriteString(aggregateWindowFlux)
	return client
}

func (i *InfluxDBClient) Limit(num int) *InfluxDBClient {
	client := i.getInstance()
	limitFlux := fmt.Sprintf("|> limit(n: %d)", num)
	client.QueryFlux.WriteString(limitFlux)
	return client
}

func (i *InfluxDBClient) Fill(usePrevious bool) *InfluxDBClient {
	client := i.getInstance()
	fillFlux := fmt.Sprintf("|> fill(usePrevious: %t)", usePrevious)
	client.QueryFlux.WriteString(fillFlux)
	return client
}

func (i *InfluxDBClient) First() *InfluxDBClient {
	client := i.getInstance()
	firstFlux := fmt.Sprintf("|> first()")
	client.QueryFlux.WriteString(firstFlux)
	return client
}

func (i *InfluxDBClient) Last() *InfluxDBClient {
	client := i.getInstance()
	lastFlux := fmt.Sprintf("|> last()")
	client.QueryFlux.WriteString(lastFlux)
	return client
}

func (i *InfluxDBClient) QueryFluxAppend(flux string) *InfluxDBClient {
	client := i.getInstance()
	client.QueryFlux.WriteString(flux)
	return client
}

func (i *InfluxDBClient) Query() (result *api.QueryTableResult, err error) {
	client := i.getInstance()
	if client.IsDebug {
		log.Printf("%s\n", i.QueryFlux.String())
	}
	result, err = client.Querier.Query(context.Background(), client.QueryFlux.String())
	if err != nil {
		return
	}
	return
}

func (i *InfluxDBClient) QueryByCustomFlux(flux string) (result *api.QueryTableResult, err error) {
	client := i.getInstance()
	result, err = client.Querier.Query(context.Background(), flux)
	if err != nil {
		return
	}
	return
}

func (i *InfluxDBClient) getInstance() *InfluxDBClient {
	if i.clone > 0 {
		client := influxdb2.NewClient(i.Config.Url, i.Config.Token)
		writer := client.WriteAPI(i.Config.Org, i.Config.Bucket)
		query := client.QueryAPI(i.Config.Org)
		influxdb := &InfluxDBClient{
			Client:  client,
			Writer:  writer,
			Querier: query,
			clone:   0,
		}
		return influxdb
	}
	return i
}
