package main

import "log"

func main() {
	execer := newQueryExecer(2, "12")
	if execer == nil {
		log.Fatalf("fname must be set")
	}
	execer.run(func(nums []int) int {
		var sum int
		for _, num := range nums {
			sum += num
		}
		avg := sum / len(nums)
		return avg
	})

}
