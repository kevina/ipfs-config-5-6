package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"reflect"
)

func main() {
	if len(os.Args) != 2 {
		usage()
	}
	direction := os.Args[1]
	var err error
	switch direction {
	case "5-to-6":
		err = convert(os.Stdin, os.Stdout, ver5to6)
	case "6-to-5":
		err = convert(os.Stdin, os.Stdout, ver6to5)
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "conversion %s failed: %v\n", direction, err)
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s 5-to-6|6-to-5 < old.config > new.config\n", os.Args[0])
	os.Exit(1)
}

func convert(in io.Reader, out io.Writer, convFunc func(ds map[string]interface{}) error) error {
	data, err := ioutil.ReadAll(in)
	if err != nil {
		return err
	}
	conf := make(map[string]interface{})
	err = json.Unmarshal(data, &conf)
	if err != nil {
		return err
	}
	fromLC := lcMap(conf)
	ds := conf[fromLC["datastore"]].(map[string]interface{})
	err = convFunc(ds)
	if err != nil {
		return err
	}
	conf[fromLC["datastore"]] = ds
	fixed, err := json.MarshalIndent(conf, "", "  ")
	if err != nil {
		return err
	}
	out.Write(fixed)
	out.Write([]byte("\n"))
	return nil
}

func ver5to6(ds map[string]interface{}) error {
	fromLC := lcMap(ds)
	noSyncVal, _ := ds[fromLC["nosync"]]
	if noSyncVal == nil {
		noSyncVal = interface{}(false)
	}
	noSync, ok := noSyncVal.(bool)
	if !ok {
		return fmt.Errorf("unsupported value for Datastore.NoSync fields: %v", noSyncVal)
	}
	delete(ds, fromLC["nosync"])
	
	dsTypeVal, _ := ds[fromLC["type"]]
	if dsTypeVal == nil {
		dsTypeVal = interface{}("")
	}
	dsType, ok := dsTypeVal.(string)
	if !ok || (dsType != "default" && dsType != "leveldb" && dsType != "") {
		return fmt.Errorf("unsupported value for Datastore.Type fields: %s", dsType)
	}
	delete(ds, fromLC["type"])

	// Path and Params never appear to have been used so just delete them
	delete(ds, fromLC["path"])
	delete(ds, fromLC["params"])
	
	ds["Spec"] = DatastoreSpec(!noSync)
	return nil
}

// lcMap create a map of an all lowecase version as the keys in map as
// the JSON library is case insensitive when matching json values to
// struct fields
func lcMap(conf map[string]interface{}) map[string]string {
	fromLC := make(map[string]string)
	for key, _ := range conf {
		fromLC[strings.ToLower(key)] = key
	}
	return fromLC
}

func ver6to5(ds map[string]interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("incompatible config detected, downgrade not possible: %v", r.(error))
		}
	}()
	fromLC := lcMap(ds)
	spec := ds[fromLC["spec"]].(map[string]interface{})
	mounts := spec["mounts"].([]interface{})
	var root, blocks interface{}
	sync := true
	for _, mount := range mounts {
		switch mountpoint := mount.(map[string]interface{})["mountpoint"].(string); mountpoint {
		case "/blocks":
			sync = mount.(map[string]interface{})["child"].(map[string]interface{})["sync"].(bool)
			blocks = mount
		case "/":
			root = mount
		default:
			return fmt.Errorf("unknown mountpoint")
		}
	}
	// normalize spec
	spec["mounts"] = []interface{}{blocks, root}
	expected := DatastoreSpec(sync)
	if !reflect.DeepEqual(spec, expected) {
		return fmt.Errorf("Datastore.Spec field not of a supported value, can't downgrade")
	}
	delete(ds, fromLC["spec"])
	ds["Type"] = "leveldb"
	ds["Params"] = nil
	ds["NoSync"] = !sync
	return nil
}

func DatastoreSpec(sync bool) map[string]interface{} {
	return map[string]interface{}{
		"type": "mount",
		"mounts": []interface{}{
			map[string]interface{}{
				"mountpoint": "/blocks",
				"type":       "measure",
				"prefix":     "flatfs.datastore",
				"child": map[string]interface{}{
					"type":      "flatfs",
					"path":      "blocks",
					"sync":      sync,
					"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
				},
			},
			map[string]interface{}{
				"mountpoint": "/",
				"type":       "measure",
				"prefix":     "leveldb.datastore",
				"child": map[string]interface{}{
					"type":        "levelds",
					"path":        "datastore",
					"compression": "none",
				},
			},
		},
	}
}
