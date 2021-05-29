package main

import "fmt"

func test() (err error) {
	for {
		fmt.Println('a')
	}
}
func main() {
	test()
}

// 死循环不需要return
