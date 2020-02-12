package main

import (
	"fmt"
	"runtime"
)

func zeroValues() {
	var a int
	var b string
	var c float64
	var d bool

	fmt.Printf("var a %T = %+v\n", a, a)
	fmt.Printf("var b %T = %q\n", b, b)
	fmt.Printf("var c %T = %+v\n", c, c)
	fmt.Printf("var d %T = %+v\n", d, d)
}

func stringAndSlices() {
	s := "Hello, Сергей!"
	sb := []byte(s)
	sr := []rune(s)

	j := 1
	for _, r := range sr {
		fmt.Printf("%d: %s\n", j, string(r))
		j++
	}
	fmt.Printf("length of '%s': %d(string), %d([]byte), %d([]rune)", s, len(s), len(sb), len(sr))
}

func stringLoop() {
	s := "Hello, Сергей!"
	for i, c := range s {
		fmt.Printf("%d: %s\n", i, string(c))
	}
	fmt.Printf("length of '%s': %d", s, len(s))
}

const (
	_ = iota
	Green
	Blue
	Red
)

func iotaPrint() {
	fmt.Printf("The value of Green is %v\n", Green)
	fmt.Printf("The value of Blue is %v\n", Blue)
	fmt.Printf("The value of Red is %v\n", Red)
}

const (
	Read   = 1 << iota // 00000001 = 1
	Write              // 00000010 = 2
	Remove             // 00000100 = 4
	// admin have all permissions
	Admin = Read | Write | Remove
)

func iotaShift() {
	fmt.Printf("The value of Read is %v\n", Read)
	fmt.Printf("The value of Write is %v\n", Write)
	fmt.Printf("The value of Remove is %v\n", Remove)
	fmt.Printf("The value of Admin is %v\n", Admin)
}

func loops() {
	fmt.Println("Loops")
	fmt.Printf("Обычный цикл for: ")
	for i := 0; i < 2; i++ {
		fmt.Printf("%d ", i)
	}
	fmt.Println()

	var counter int
	fmt.Printf("Бесконечный цикл: ") // Выход по return либо break
	for {
		fmt.Printf("%d ", counter)
		if counter == 2 {
			break
		}
		counter++
	}

	fmt.Println("Range по слайсу: ")
	slice := []int{0, 1, 2, 3}
	for i, value := range slice {
		// value - это переменная цикла, значение которой совпадает с элементом i-ой итерции
		fmt.Printf("\tslice[%d]=%v, &value=%p, &slice[i]=%p\n", i, value, &value, &slice[i])
	}
}

func switchSample() {
	fmt.Print("Go runs on ")
	switch os := runtime.GOOS; os {
	case "darwin", "freebsd":
		fmt.Println("OS X.")
	case "linux":
		fmt.Println("Linux.")
	default:
		// freebsd, openbsd,
		// plan9, windows...
		fmt.Printf("%s.", os)
	}
	fmt.Println()
}

func sliceAppend() {
	otherStocks := []string{"YNDX", "MSFT", "FB", "TCS"}
	stocks := make([]string, 0, len(otherStocks))
	fmt.Printf("len=%d, cap=%d\n", len(stocks), cap(stocks))
	for _, stock := range otherStocks {
		stocks = append(stocks, stock[:1])
		fmt.Printf("len=%d, cap=%d\n", len(stocks), cap(stocks))
	}

	fmt.Println("stocks: ", stocks)
}

func sliceMistake() {
	otherStocks := []string{"YNDX", "MSFT", "FB", "TCS"}
	stocks := make([]int, len(otherStocks), len(otherStocks))
	fmt.Printf("len=%d, cap=%d\n", len(stocks), cap(stocks))
	for _, stock := range otherStocks {
		stocks = append(stocks, int(stock[0]))
		fmt.Printf("len=%d, cap=%d\n", len(stocks), cap(stocks))
	}

	fmt.Println("stocks: ", stocks)
}

func subslice() {
	stocks := []string{"YNDX", "MSFT", "FB", "TCS"}

	fmt.Println(stocks[1:3])

	fmt.Println(stocks[2:len(stocks)])
	fmt.Println(stocks[2:])

	fmt.Println(stocks[0:3])
	fmt.Println(stocks[:3])
}

func copySlice() {
	var stocks []string
	if stocks == nil {
		fmt.Println("slice is nil")
	}

	original := []string{"YNDX", "MSFT", "FB", "TCS"}

	ref := original

	cpy := make([]string, len(original))

	copy(cpy, original)
	ref[0] = "_Y_"
	original[1] = "_M_"

	fmt.Println("original: ", original)
	fmt.Println("ref: ", ref)
	fmt.Println("cpy: ", cpy)
}

func mapSample() {
	yield := make(map[string]int)

	yield["YNDX"] = 1000
	yield["TCS"] = 10000

	fmt.Printf("TCS: %d; YNDX: %d\n", yield["TCS"], yield["YNDX"])

	delete(yield, "YNDX")

	fmt.Printf("TCS: %d; YNDX: %d\n", yield["TCS"], yield["YNDX"])
}

func checkMapKeyExistence() {
	stocks := []string{"YNDX", "FB", "TCS"}
	yield := map[string]int{
		"YNDX": 1000,
		"TCS":  10000,
	}
	for _, stock := range stocks {
		y, ok := yield[stock]
		if !ok {
			continue
		}
		fmt.Println("yield for", stock, "is", y)
	}
}

func rangeMap() {
	yield := map[string]int{
		"YNDX": 1000,
		"TCS":  10000,
	}
	for stock, yield := range yield {
		fmt.Println("yield for", stock, "is", yield)
	}
	for stock := range yield {
		fmt.Println("yield for", stock)
	}
}

func mapMistake() {
	var yield map[string]int
	yield["YNDX"] = 1000
}

func checkCapBehave() {
	const maxOuts = 60
	var slice []int

	var c, j int
	for {
		slice = append(slice, 1)

		if c != 0 {
			if j > maxOuts-1 {
				break
			}
			newC := cap(slice)
			g := float64(newC-c) / float64(newC) * 100
			fmt.Printf("%d: slice growth %.2f%%(c/newC)(%d/%d)\n", j, g, c, newC)
			j++
			c = 0
		}

		if len(slice) == cap(slice) {
			c = cap(slice)
		}
	}
}

type sample func()

var samples = map[string]sample{
	"zeroValue":            zeroValues,
	"stringAndSlices":      stringAndSlices,
	"stringLoop":           stringLoop,
	"iotaShift":            iotaShift,
	"loops":                loops,
	"switchSample":         switchSample,
	"sliceAppend":          sliceAppend,
	"sliceMistake":         sliceMistake,
	"subslice":             subslice,
	"copySlice":            copySlice,
	"mapSample":            mapSample,
	"checkMapKeyExistence": checkMapKeyExistence,
	"rangeMap":             rangeMap,
	"checkCapBehave":       checkCapBehave,
	"mapMistake":           mapMistake,
}

func main() {
	for n, f := range samples {
		fmt.Printf(" === %s ===\n", n)
		f()
		fmt.Printf("\n\n")
	}
}
