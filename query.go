package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
)

type record struct {
	a int
	b int
}

type result struct {
	b   int
	ret int
}

type queryExecer struct {
	concurrentSize int    //call func 并发数
	fname          string //data file
}

func newQueryExecer(size int, fname string) *queryExecer {
	if fname == "" {
		return nil
	}
	return &queryExecer{
		concurrentSize: size,
		fname:          fname,
	}
}

func (e *queryExecer) getRecords() (<-chan record, error) {
	ch := make(chan record)
	f, _ := os.Open(e.fname)
	r := csv.NewReader(f)
	lines, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	go func() {
		for _, datas := range lines {
			if len(datas) != 2 {
				panic(fmt.Errorf("data size err datas:%v", datas))
			}
			a, err := strconv.Atoi(datas[0])
			if err != nil {
				panic(fmt.Errorf("a not num err:%v ,data:%s", err, datas[0]))
			}
			b, err := strconv.Atoi(datas[1])
			if err != nil {
				panic(fmt.Errorf("b not num err:%v ,data:%s", err, datas[1]))
			}
			r := record{
				a: a,
				b: b,
			}
			ch <- r
		}
		close(ch)
	}()

	return ch, nil
}

// shuffle 分发数据到多个channel
func (e *queryExecer) shuffle(ch <-chan record) []chan record {
	chs := make([]chan record, e.concurrentSize)
	for i := 0; i < e.concurrentSize; i++ {
		chs[i] = make(chan record)
	}
	go func() {
		for r := range ch {
			i := r.b % (e.concurrentSize)
			ch := chs[i]
			ch <- r
		}
		for i := 0; i < e.concurrentSize; i++ {
			close(chs[i])
		}
	}()
	return chs
}

// sortAndExec 对数据排序；获取grop key对应的数组；对数组执行func
func (e *queryExecer) sortAndExec(ch <-chan record, f func([]int) int) <-chan result {
	if ch == nil || f == nil {
		return nil
	}
	retch := make(chan result)
	go func() {
		var records []record
		for v := range ch {
			records = append(records, v)
		}
		sort.Slice(records, func(i, j int) bool {
			return records[i].b < records[j].b
		})
		size := len(records)
		var i int
		for i < size {
			j := i + 1
			values := []int{}
			key := records[i].b
			values = append(values, records[i].a)
			for j < size && records[j].b == records[i].b {
				values = append(values, records[j].a)
				j++
			}
			retV := f(values)
			retch <- result{
				b:   key,
				ret: retV,
			}
			i = j
		}
		close(retch)
	}()
	return retch
}

func merge(chs []<-chan result) <-chan result {
	ret := make(chan result)
	var wg = &sync.WaitGroup{}
	wg.Add(len(chs))
	go func() {
		for _, citem := range chs {
			go func(ch <-chan result) {
				for v := range ch {
					ret <- v
				}
				wg.Done()
			}(citem)
		}
	}()
	go func() {
		wg.Wait()
		close(ret)
	}()
	return ret
}

func output(ch <-chan result) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range ch {
			fmt.Printf("avg(a):%d,b:%d\n", v.ret, v.b)
		}
	}()
	wg.Wait()
}

func (e *queryExecer) runGroupFunc(f func([]int) int) (<-chan result, error) {
	//read records
	recordCh, err := e.getRecords()
	if err != nil {
		return nil, err
	}
	// shuffle by b to multi channel
	shuffedChs := e.shuffle(recordCh)

	//query result ch
	retChs := make([]<-chan result, 0, len(shuffedChs))
	for _, ch := range shuffedChs {
		// run avg func for every chanel
		retch := e.sortAndExec(ch, f)
		retChs = append(retChs, retch)
	}
	// merge result
	allRet := merge(retChs)
	return allRet, nil
}

func (e *queryExecer) run(f func([]int) int) error {
	ch, err := e.runGroupFunc(f)
	if err != nil {
		return err
	}
	//print
	output(ch)
	return nil
}
