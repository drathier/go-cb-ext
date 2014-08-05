package cbext

import (
	"fmt"
	"github.com/couchbaselabs/go-couchbase"
	"reflect"
	"strconv"
	"strings"
)

func GetCurrentDatacenter() string {
	return "ryd"
}

func GetDatacenters() []string {
	return []string{"ryd", "awsSwe", "awsNY"}
}

func AppendToArray(bucket *couchbase.Bucket, key string, value interface{}) (err error) {
	var keyValue int
	err = bucket.Get(key+"_"+GetCurrentDatacenter(), &keyValue)
	if err != nil && !strings.Contains(err.Error(), "KEY_ENOENT") {
		return err
	}

	if err != nil && strings.Contains(err.Error(), "KEY_ENOENT") {
		bucket.Set(key+"_"+GetCurrentDatacenter(), 0, 0)
		keyValue = 0
	}

	keyValue += 1
	err = bucket.Set(key+"_"+GetCurrentDatacenter()+"_"+strconv.Itoa(keyValue), 0, value)

	if err != nil {
		return err
	}

	err = bucket.Set(key+"_"+GetCurrentDatacenter(), 0, keyValue)
	if err != nil {
		return err
	}

	return nil
}

func GetArray(bucket *couchbase.Bucket, key string, rv interface{}) (err error) {
	count := 0
	for _, dc := range GetDatacenters() {
		val := 0
		err = bucket.Get(key+"_"+dc, &val)
		count += val
	}

	slice := reflect.ValueOf(rv).Elem()
	slice.Set(reflect.MakeSlice(slice.Type(), 0, count))

	for _, dc := range GetDatacenters() {
		var keyValue int
		err = bucket.Get(key+"_"+dc, &keyValue)
		if err != nil {
			continue
		}

		for i := 1; i <= keyValue; i += 1 {
			v := reflect.New(slice.Type().Elem())
			err = bucket.Get(key+"_"+dc+"_"+strconv.Itoa(i), v.Interface())
			if err == nil {
				fmt.Print("\n\n", v.Elem().Interface())
				slice.Set(reflect.Append(slice, v.Elem()))
			}
		}
	}
	return nil
}
