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
	Client    influxdb2.Client
	Org       string
	Bucket    string
	Point     *write.Point
	Writer    api.WriteAPI
	Querier   api.QueryAPI
	QueryFlux strings.Builder
	Err       error
	IsDebug   bool
}

type InfluxDBConfig struct {
	Url    string
	Token  string
	Org    string
	Bucket string
}

func NewClient(config *InfluxDBConfig) InfluxDBClient {
	client := influxdb2.NewClient(config.Url, config.Token)
	writer := client.WriteAPI(config.Org, config.Bucket)
	query := client.QueryAPI(config.Org)
	influxdb := InfluxDBClient{
		Client:  client,
		Org:     config.Org,
		Bucket:  config.Bucket,
		Writer:  writer,
		Querier: query,
	}
	return influxdb
}

func (i *InfluxDBClient) Error() error {
	return i.Err
}

func (i *InfluxDBClient) Debug() *InfluxDBClient {
	i.IsDebug = true
	return i
}

func (i *InfluxDBClient) Write(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) *InfluxDBClient {
	point := influxdb2.NewPoint(measurement, tags, fields, ts)
	i.Writer.WritePoint(point)
	return i
}

func (i *InfluxDBClient) WritePoint(point *write.Point) *InfluxDBClient {
	i.Writer.WritePoint(point)
	return i
}

func (i *InfluxDBClient) Flush() {
	i.Writer.Flush()
}

func (i *InfluxDBClient) FromBucket(bucket string) *InfluxDBClient {
	fromFlux := fmt.Sprintf("from(bucket: \"%s\")", bucket)
	i.QueryFlux.WriteString(fromFlux)
	return i
}

func (i *InfluxDBClient) Range(timeFrom string, timeTo string) *InfluxDBClient {
	timeFromParse, err := time.Parse(time.DateTime, timeFrom)
	if err != nil {
		i.Err = err
		return i
	}
	timeToParse, err := time.Parse(time.DateTime, timeTo)
	if err != nil {
		i.Err = err
		return i
	}
	start := timeFromParse.Format("2006-01-02T15:04:05+08:00")
	end := timeToParse.Format("2006-01-02T15:04:05+08:00")
	rangeFlux := fmt.Sprintf("|> range(start: %s ,stop: %s)", start, end)
	i.QueryFlux.WriteString(rangeFlux)
	return i
}

func (i *InfluxDBClient) RangeRecent(rangeTime string) *InfluxDBClient {
	rangeFlux := fmt.Sprintf("|> range(start: %s)", rangeTime)
	i.QueryFlux.WriteString(rangeFlux)
	return i
}

func (i *InfluxDBClient) Measurement(measurement string) *InfluxDBClient {
	rangeFlux := fmt.Sprintf("|> filter(fn: (r) => r._measurement == \"%s\")", measurement)
	i.QueryFlux.WriteString(rangeFlux)
	return i
}

func (i *InfluxDBClient) Tag(tagKey string, tagValues ...string) *InfluxDBClient {
	if len(tagValues) == 0 {
		return i
	}
	var filterTagsStr []string
	for _, v := range tagValues {
		temp := fmt.Sprintf("r.%s == \"%s\"", tagKey, v)
		filterTagsStr = append(filterTagsStr, temp)
	}
	filterTag := strings.Join(filterTagsStr, " or ")
	tagFlux := fmt.Sprintf("|> filter(fn: (r) => %s)", filterTag)
	i.QueryFlux.WriteString(tagFlux)
	return i
}

func (i *InfluxDBClient) Field(fieldValues ...string) *InfluxDBClient {
	if len(fieldValues) == 0 {
		return i
	}
	var filterTagsStr []string
	for _, v := range fieldValues {
		temp := fmt.Sprintf("r._field == \"%s\"", v)
		filterTagsStr = append(filterTagsStr, temp)
	}
	filterTag := strings.Join(filterTagsStr, " or ")
	tagFlux := fmt.Sprintf("|> filter(fn: (r) => %s)", filterTag)
	i.QueryFlux.WriteString(tagFlux)
	return i
}

func (i *InfluxDBClient) Filter(filter string) *InfluxDBClient {
	filterFlux := fmt.Sprintf("|> filter(fn: (r) => %s)", filter)
	i.QueryFlux.WriteString(filterFlux)
	return i
}

// AggregateWindow fn: mean,first,last...
func (i *InfluxDBClient) AggregateWindow(every string, fn string, createEmpty bool) *InfluxDBClient {
	aggregateWindowFlux := fmt.Sprintf("|> aggregateWindow(every: %s, fn: %s, createEmpty: %t)", every, fn, createEmpty)
	i.QueryFlux.WriteString(aggregateWindowFlux)
	return i
}

func (i *InfluxDBClient) Limit(num int) *InfluxDBClient {
	limitFlux := fmt.Sprintf("|> limit(n: %d)", num)
	i.QueryFlux.WriteString(limitFlux)
	return i
}

func (i *InfluxDBClient) Fill(usePrevious bool) *InfluxDBClient {
	fillFlux := fmt.Sprintf("|> fill(usePrevious: %t)", usePrevious)
	i.QueryFlux.WriteString(fillFlux)
	return i
}

func (i *InfluxDBClient) First() *InfluxDBClient {
	firstFlux := fmt.Sprintf("|> first()")
	i.QueryFlux.WriteString(firstFlux)
	return i
}

func (i *InfluxDBClient) Last() *InfluxDBClient {
	lastFlux := fmt.Sprintf("|> last()")
	i.QueryFlux.WriteString(lastFlux)
	return i
}

func (i *InfluxDBClient) QueryFluxAppend(flux string) *InfluxDBClient {
	i.QueryFlux.WriteString(flux)
	return i
}

func (i *InfluxDBClient) Query() (result *api.QueryTableResult, err error) {
	if i.IsDebug {
		log.Printf("%s\n", i.QueryFlux.String())
	}
	result, err = i.Querier.Query(context.Background(), i.QueryFlux.String())
	if err != nil {
		return
	}
	return
}

func (i *InfluxDBClient) QueryByCustomFlux(flux string) (result *api.QueryTableResult, err error) {
	result, err = i.Querier.Query(context.Background(), flux)
	if err != nil {
		return
	}
	return
}
