package main
import (
	"fmt"
	"time"
)
func main() {
	timeoutSecs := 1.0
	fmt.Println(time.Duration(timeoutSecs * float64(time.Second)))
}
