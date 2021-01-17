package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
)

var (
	f string
	s int
)

func exists(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

func main() {
	flag.StringVar(&f, "f", "", "input data file name")
	flag.IntVar(&s, "s", 0, "shards of data")
	flag.Parse()
	if !exists(f) {
		fmt.Fprintf(os.Stderr, "file not exist")
	}
	if s == 0 {
		s = runtime.NumCPU()
	}
	execer := newQueryExecer(s, f)
	if execer == nil {
		log.Fatalf("iput data file name must be set")
	}
	avgf := func(nums []int) int {
		var sum int
		for _, num := range nums {
			sum += num
		}
		avg := sum / len(nums)
		return avg
	}
	err := execer.run(avgf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run err:%v", err)
	}
}
