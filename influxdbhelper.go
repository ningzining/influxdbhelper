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
	Config    InfluxDBConfig
	Writer    api.WriteAPI
	Querier   api.QueryAPI
	Deleter   api.DeleteAPI
	QueryFlux strings.Builder
	IsDebug   bool
	clone     int
}

type InfluxDBConfig struct {
	Url    string
	Token  string
	Org    string
	Bucket string
}

func Client(config InfluxDBConfig) *InfluxDBClient {
	client := influxdb2.NewClient(config.Url, config.Token)
	influxdb := &InfluxDBClient{
		Config:  config,
		Writer:  client.WriteAPI(config.Org, config.Bucket),
		Querier: client.QueryAPI(config.Org),
		Deleter: client.DeleteAPI(),
		clone:   1,
	}
	return influxdb
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

func (i *InfluxDBClient) Range(timeFrom time.Time, timeTo time.Time) *InfluxDBClient {
	client := i.getInstance()
	rangeFlux := fmt.Sprintf("|> range(start: %s ,stop: %s)", timeFrom.Format(time.RFC3339), timeTo.Format(time.RFC3339))
	client.QueryFlux.WriteString(rangeFlux)
	return client
}

func (i *InfluxDBClient) RangeRecent(rangeTime time.Time) *InfluxDBClient {
	client := i.getInstance()
	rangeFlux := fmt.Sprintf("|> range(start: %s)", rangeTime.Format(time.RFC3339))
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

func (i *InfluxDBClient) Group(columns []string, mode string) *InfluxDBClient {
	client := i.getInstance()
	var columnStr strings.Builder
	for k, column := range columns {
		if k != 0 {
			columnStr.WriteString(", ")
		}
		columnStr.WriteString(fmt.Sprintf("\"%s\"", column))
	}
	flux := fmt.Sprintf("|> group(columns: [%s], mode: \"%s\")", columnStr.String(), mode)
	client.QueryFlux.WriteString(flux)
	return client
}

func (i *InfluxDBClient) Sort(columns []string) *InfluxDBClient {
	client := i.getInstance()
	var columnStr strings.Builder
	for k, column := range columns {
		if k != 0 {
			columnStr.WriteString(", ")
		}
		columnStr.WriteString(fmt.Sprintf("\"%s\"", column))
	}
	flux := fmt.Sprintf("|> sort(columns: [%s])", columnStr.String())
	client.QueryFlux.WriteString(flux)
	return client
}

func (i *InfluxDBClient) Limit(num int) *InfluxDBClient {
	client := i.getInstance()
	limitFlux := fmt.Sprintf("|> limit(n: %d)", num)
	client.QueryFlux.WriteString(limitFlux)
	return client
}

// AggregateWindow fn: mean,first,last...
func (i *InfluxDBClient) AggregateWindow(every string, fn string, createEmpty bool) *InfluxDBClient {
	client := i.getInstance()
	aggregateWindowFlux := fmt.Sprintf("|> aggregateWindow(every: %s, fn: %s, createEmpty: %t)", every, fn, createEmpty)
	client.QueryFlux.WriteString(aggregateWindowFlux)
	return client
}

func (i *InfluxDBClient) Increase() *InfluxDBClient {
	client := i.getInstance()
	client.QueryFlux.WriteString("|> increase()")
	return client
}

func (i *InfluxDBClient) MovingAverage(n int) *InfluxDBClient {
	client := i.getInstance()
	client.QueryFlux.WriteString(fmt.Sprintf("|> movingAverage(n: %d)", n))
	return client
}

func (i *InfluxDBClient) TimedMovingAverage(every string, period string) *InfluxDBClient {
	client := i.getInstance()
	client.QueryFlux.WriteString(fmt.Sprintf("|> timedMovingAverage(every: %s, period: %s)", every, period))
	return client
}

func (i *InfluxDBClient) Derivative(unit string, nonNegative bool) *InfluxDBClient {
	client := i.getInstance()
	client.QueryFlux.WriteString(fmt.Sprintf("|> derivative(unit: %s, nonNegative: %t)", unit, nonNegative))
	return client
}

func (i *InfluxDBClient) Rate(every string, unit string) *InfluxDBClient {
	client := i.getInstance()
	client.QueryFlux.WriteString(fmt.Sprintf("|> aggregate.rate(every: %s,unit: %s)", every, unit))
	return client
}

func (i *InfluxDBClient) FillWithPrevious(usePrevious bool) *InfluxDBClient {
	client := i.getInstance()
	client.QueryFlux.WriteString(fmt.Sprintf("|> fill(usePrevious: %t)", usePrevious))
	return client
}

func (i *InfluxDBClient) FillWithValue(value float64) *InfluxDBClient {
	client := i.getInstance()
	client.QueryFlux.WriteString(fmt.Sprintf("|> fill(value: %.2f)", value))
	return client
}

func (i *InfluxDBClient) Median() *InfluxDBClient {
	client := i.getInstance()
	client.QueryFlux.WriteString("|> median()")
	return client
}

func (i *InfluxDBClient) CumulativeSum() *InfluxDBClient {
	client := i.getInstance()
	client.QueryFlux.WriteString("|> cumulativeSum()")
	return client
}

func (i *InfluxDBClient) First() *InfluxDBClient {
	client := i.getInstance()
	client.QueryFlux.WriteString("|> first()")
	return client
}

func (i *InfluxDBClient) Last() *InfluxDBClient {
	client := i.getInstance()
	client.QueryFlux.WriteString("|> last()")
	return client
}

func (i *InfluxDBClient) QueryFluxAppend(flux string) *InfluxDBClient {
	client := i.getInstance()
	client.QueryFlux.WriteString(flux)
	return client
}

func (i *InfluxDBClient) Query() (*api.QueryTableResult, error) {
	client := i.getInstance()
	if client.IsDebug {
		log.Printf("%s\n", i.QueryFlux.String())
	}
	result, err := client.Querier.Query(context.Background(), client.QueryFlux.String())
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (i *InfluxDBClient) QueryByCustomFlux(flux string) (*api.QueryTableResult, error) {
	client := i.getInstance()
	result, err := client.Querier.Query(context.Background(), flux)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (i *InfluxDBClient) DeleteWithName(ctx context.Context, start, stop time.Time, predicate string) error {
	client := i.getInstance()
	return client.Deleter.DeleteWithName(ctx, client.Config.Org, client.Config.Bucket, start, stop, predicate)
}

func (i *InfluxDBClient) getInstance() *InfluxDBClient {
	if i.clone > 0 {
		client := influxdb2.NewClient(i.Config.Url, i.Config.Token)
		influxdb := &InfluxDBClient{
			Config:  i.Config,
			Writer:  client.WriteAPI(i.Config.Org, i.Config.Bucket),
			Querier: client.QueryAPI(i.Config.Org),
			Deleter: client.DeleteAPI(),
			clone:   0,
		}
		return influxdb
	}
	return i
}
