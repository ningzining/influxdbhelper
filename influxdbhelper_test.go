package influxdbhelper

import (
	"context"
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
	for i := 0; i < 30; i++ {
		client.Write("test_data", map[string]string{"imei": "123123", "iccid": "234234"}, map[string]interface{}{"speed": floatValue}, date).Flush()
		date = date.Add(time.Minute)
		floatValue += 1
	}
	for i := 0; i < 30; i++ {
		client.Write("test_data", map[string]string{"imei": "123123", "iccid": "234234"}, map[string]interface{}{"speed": floatValue}, date).Flush()
		date = date.Add(time.Minute)
		floatValue -= 1
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
	endTime := time.Date(year, month, day, 23, 05, 0, 0, time.Local)

	result, err := client.Debug().
		FromBucket(bucket).
		Range(startTime, endTime).
		Measurement("test_data").
		Tag("imei", "123123").
		Field("speed").
		Query()

	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}
	m := make(map[interface{}]struct{})
	for result.Next() {
		record := result.Record()
		field := record.Field()
		value := record.Value()
		ts := record.Time().Local()
		m[value] = struct{}{}
		fmt.Printf("field:%s, value:%v, ts:%s\n", field, value, ts)
	}
	fmt.Printf("map: %v\n", m)
}

func TestInfluxDbClient_Delete(t *testing.T) {
	config := InfluxDBConfig{
		Url:    url,
		Token:  token,
		Org:    org,
		Bucket: bucket,
	}
	influxDBClient := Client(config)

	start := time.Date(2023, 6, 13, 8, 0, 0, 0, time.Local)
	stop := time.Date(2023, 6, 13, 8, 20, 0, 0, time.Local)
	predicate := "_measurement=\"test_data\" AND imei=\"123123\""

	influxDBClient.DeleteWithName(context.Background(), start, stop, predicate)
}
