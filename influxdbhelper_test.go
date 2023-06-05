package influxdbhelper

import (
	"fmt"
	"testing"
	"time"
)

const (
	url    = "http://139.224.36.174:8086"
	token  = "FwPlCbMZLr_-UNEDeWGHaYt1iU2KUktaHu_ayCMGOjj2DsdYpIX1bU4BMzZQQCoFM_4HZsNwg-QyGOstl6MeWQ=="
	org    = "cotton"
	bucket = "test"
)

func TestInfluxDBClient_Write(t *testing.T) {
	config := InfluxDBConfig{
		Url:    url,
		Token:  token,
		Org:    org,
		Bucket: bucket,
	}
	client := Client(config)
	year, month, day := time.Now().Date()
	date := time.Date(year, month, day, 8, 0, 0, 0, time.Local)
	floatValue := 20.33
	for i := 0; i < 60; i++ {
		client.Write("test_data", map[string]string{"imei": "123456", "iccid": "654321"}, map[string]interface{}{"speed": floatValue}, date).Flush()
		date = date.Add(time.Minute)
		floatValue += 1
	}
}

func TestInfluxDBClient_Query(t *testing.T) {
	config := InfluxDBConfig{
		Url:    url,
		Token:  token,
		Org:    org,
		Bucket: bucket,
	}
	client := Client(config)

	year, month, day := time.Now().Date()
	startTime := time.Date(year, month, day, 0, 0, 0, 0, time.Local)
	endTime := time.Date(year, month, day, 59, 59, 59, 0, time.Local)
	start := startTime.Format("2006-01-02 15:04:05")
	end := endTime.Format("2006-01-02 15:04:05")

	result, err := client.Debug().
		FromBucket("test").
		Range(start, end).
		Measurement("test_data").
		Tag("imei", "123456", "123345").
		Tag("iccid", "654321").
		Field("speed", "floatType").
		AggregateWindow("1m", "last", false).
		Fill(true).
		Limit(100).
		Query()

	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	for result.Next() {
		record := result.Record()
		field := record.Field()
		value := record.Value()
		ts := record.Time().Local()
		fmt.Printf("field:%s, value:%v, ts:%s\n", field, value, ts)
	}
}
