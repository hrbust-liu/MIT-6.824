package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	KV := make(map[string][] string, 0)
	for i := 0; i < nMap; i++ {
		reduceFile, err1 := os.Open(reduceName(jobName, i, reduceTask))
		if (err1 != nil) {
			log.Fatal("Read reduceFile error:", err1)
		}
		defer reduceFile.Close()
		dec := json.NewDecoder(reduceFile)
		for {
			var kv KeyValue
			err2 := dec.Decode(&kv)
			if (err2 != nil) {
				break
			}
			_, ok := KV[kv.Key]
			if (!ok) {
				KV[kv.Key] = make([]string, 0)
			}
			KV[kv.Key] = append(KV[kv.Key], kv.Value)	//将所有key对应的value放到一个数组里
		}
	}
	var keys []string
	for k, _ := range KV {
		keys = append(keys, k)
	}
	sort.Strings(keys)	//对key进行排序
	mergeName := mergeName(jobName, reduceTask)
	mergeFile, err3 := os.Create(mergeName)
	if (err3 != nil) {
		log.Fatal("Create mergeFile error:", err3)
	}
	defer mergeFile.Close()
	enc := json.NewEncoder(mergeFile)
	for _, k := range keys {
		err := enc.Encode(KeyValue{k, reduceF(k, KV[k])})		//将reduceF的结果与key并入一个文件里
		if (err != nil) {
			log.Fatal("MergeFile encode error:", err)
		}
	}
}
