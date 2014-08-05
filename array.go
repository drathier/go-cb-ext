package cbext

import (
	"encoding/json"
	"errors"
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

func AppendToArray(bucket *couchbase.Bucket, key string, value interface{}) error {
	var keyValue int
	err := bucket.Get(key+"_"+GetCurrentDatacenter(), &keyValue)
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

func FlushArray(bucket *couchbase.Bucket, key string, value interface{}) error {
	count := 0
	for _, dc := range GetDatacenters() {
		val := 0
		err := bucket.Get(key+"_"+dc, &val)
		if err != nil && !strings.Contains(err.Error(), "KEY_ENOENT") {
			return err
		}
		count += val
	}

	for _, dc := range GetDatacenters() {
		var keyValue int
		err := bucket.Get(key+"_"+dc, &keyValue)
		if err != nil && strings.Contains(err.Error(), "KEY_ENOENT") {
			continue
		}

		for i := 1; i <= keyValue; i += 1 {
			err = bucket.Delete(key + "_" + dc + "_" + strconv.Itoa(i))
			if err != nil && !strings.Contains(err.Error(), "KEY_ENOENT") {
				return err
			}
		}
	}

	for _, dc := range GetDatacenters() {
		err := bucket.Delete(key + "_" + dc)
		if err != nil && !strings.Contains(err.Error(), "KEY_ENOENT") {
			return err
		}
	}

	return nil
}

func DeleteArrayObject(bucket *couchbase.Bucket, key string, value interface{}) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}

	for _, dc := range GetDatacenters() {
		var keyValue int
		err = bucket.Get(key+"_"+dc, &keyValue)
		if err != nil && strings.Contains(err.Error(), "KEY_ENOENT") {
			continue
		}

		for i := 1; i <= keyValue; i += 1 {
			var v []byte
			v, err = bucket.GetRaw(key + "_" + dc + "_" + strconv.Itoa(i))
			if err == nil {
				if string(b) == string(v) {
					return bucket.Delete(key + "_" + dc + "_" + strconv.Itoa(i))
				}
			}
		}
	}

	return errors.New("OBJECT_NOT_FOUND_IN_ARRAY")
}

func GetArray(bucket *couchbase.Bucket, key string, rv interface{}) error {
	count := 0
	for _, dc := range GetDatacenters() {
		val := 0
		err := bucket.Get(key+"_"+dc, &val)
		count += val
		if err != nil && !strings.Contains(err.Error(), "KEY_ENOENT") {
			return err
		}
	}

	slice := reflect.ValueOf(rv).Elem()
	slice.Set(reflect.MakeSlice(slice.Type(), 0, count))

	for _, dc := range GetDatacenters() {
		var keyValue int
		err := bucket.Get(key+"_"+dc, &keyValue)
		if err != nil && strings.Contains(err.Error(), "KEY_ENOENT") {
			continue
		}

		for i := 1; i <= keyValue; i += 1 {
			v := reflect.New(slice.Type().Elem())
			err = bucket.Get(key+"_"+dc+"_"+strconv.Itoa(i), v.Interface())
			if err == nil {
				slice.Set(reflect.Append(slice, v.Elem()))
			}
		}
	}
	return nil
}
