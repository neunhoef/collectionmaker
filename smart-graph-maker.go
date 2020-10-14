package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/neunhoef/smart-graph-maker/cmd"
)

func main() {
	fmt.Print("Hello world!\n")
	rand.Seed(time.Now().UnixNano())
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
