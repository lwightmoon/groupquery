package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"testing"
)

const testfname = "tdata.csv"

//a取值范围[1,30000],b取值[1,4]
func generateTestData() error {
	f, err := os.Create(testfname) //创建文件
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	gsize := 5
	for i := 1; i <= 30000; i++ {
		d1 := strconv.Itoa(i)
		key := i % gsize
		keyStr := strconv.Itoa(key)
		err = w.Write([]string{d1, keyStr})
		if err != nil {
			return err
		}
	}
	w.Flush()
	return nil
}
func TestMain(m *testing.M) {
	err := generateTestData()
	if err != nil {
		panic(fmt.Sprintf("generate test data err:%v", err))
	}
	m.Run()
	os.Remove(testfname)
}
func TestRunGroupFunc(t *testing.T) {
	execer := newQueryExecer(4, testfname)
	retch, err := execer.runGroupFunc(func(nums []int) int {
		var sum int
		for _, num := range nums {
			sum += num
		}
		avg := sum / len(nums)
		return avg
	})
	if err != nil {
		t.Fatalf("run err:%v", err)
	}
	// var rets []result
	for v := range retch {
		switch v.b {
		case 1:
			if v.ret != (1+29996)/2 {
				t.Fail()
			}
		case 2:
			if v.ret != (2+29997)/2 {
				t.Fail()
			}
		case 3:
			if v.ret != (3+29998)/2 {
				t.Fail()
			}
		case 4:
			if v.ret != (4+29999)/2 {
				t.Fail()
			}
		case 0:
			if v.ret != (5+30000)/2 {
				t.Fail()
			}
		}
	}
}
