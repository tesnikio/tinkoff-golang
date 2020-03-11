package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Candle struct {
	Ticker       string
	Timestamp    time.Time
	OpeningPrice float64
	MaxPrice     float64
	MinPrice     float64
	ClosingPrice float64
}

type Trade struct {
	Ticker    string
	Price     float64
	Amount    int
	Timestamp time.Time
}

// helper functions
func getMaxCandlePrice(trade []Trade) float64 {
	maxPriceValue := trade[0].Price

	for _, currentValue := range trade {
		if currentValue.Price > maxPriceValue {
			maxPriceValue = currentValue.Price
		}
	}

	return maxPriceValue
}

func getMinCandlePrice(trade []Trade) float64 {
	minPriceValue := trade[0].Price

	for _, currentValue := range trade {
		if currentValue.Price < minPriceValue {
			minPriceValue = currentValue.Price
		}
	}

	return minPriceValue
}

func candleFormat(candle Candle) string {
	timestamp := candle.Timestamp.Format(time.RFC3339)
	openingPrice := strconv.FormatFloat(candle.OpeningPrice, 'f', -1, 64)
	maxPrice := strconv.FormatFloat(candle.MaxPrice, 'f', -1, 64)
	minPrice := strconv.FormatFloat(candle.MinPrice, 'f', -1, 64)
	closingPrice := strconv.FormatFloat(candle.ClosingPrice, 'f', -1, 64)

	resultString := fmt.Sprintf("%s,%s,%s,%s,%s,%s\n", candle.Ticker, timestamp, openingPrice, maxPrice, minPrice, closingPrice)

	return resultString
}

func readFileConcurrently(cntx context.Context, filename string, start <-chan struct{}) (<-chan Trade, error) { //nolint
	resultFile := make(chan Trade)
	tradesCSV, err := os.Open(filename)

	if err != nil {
		return nil, fmt.Errorf("fatal error, can't open file: %s", err)
	}

	reader := csv.NewReader(bufio.NewReader(tradesCSV))

	go func(file *os.File) {
		defer file.Close()
		defer close(resultFile)

		<-start

		for {
			line, err := reader.Read()

			if err == io.EOF {
				return
			}

			timestamp := line[3]

			tsDate := strings.Split(timestamp, " ")[0]
			tsTime := strings.Split(timestamp, " ")[1]

			timestampData, err := time.Parse(time.RFC3339, tsDate+"T"+tsTime+"Z")

			if err != nil {
				fmt.Println("Couldn't parse time: ", err)
			}

			price, err := strconv.ParseFloat(line[1], 64)

			if err != nil {
				fmt.Println("Couldn't parse price: ", err)
			}

			count, err := strconv.Atoi(line[2])

			if err != nil {
				fmt.Println("Couldn't parse amount: ", err)
			}

			trade := Trade{
				Ticker:    line[0],
				Price:     price,
				Amount:    count,
				Timestamp: timestampData,
			}

			select {
			case resultFile <- trade:
			case <-cntx.Done():
				fmt.Println("Context terminated due to timeout")
				return
			}
		}
	}(tradesCSV)

	return resultFile, nil
}

func writeCSV(channelData <-chan []Candle, filename string) {
	file, err := os.Create(filename)

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	for candles := range channelData {
		if len(candles) == 0 {
			continue
		}

		for _, candle := range candles {
			str := candleFormat(candle)

			if _, err := file.WriteString(str); err != nil {
				log.Fatal(err)
			}
		}
	}
}

func writeResult(candles5minChan, candles30minChan, candles240minChan <-chan []Candle) {
	var wg sync.WaitGroup

	classesCnt := 3

	wg.Add(classesCnt)

	writeToFile := func(filename string, channelData <-chan []Candle) {
		defer wg.Done()
		writeCSV(channelData, filename)
	}

	go writeToFile("candles_5m.csv", candles5minChan)
	go writeToFile("candles_30m.csv", candles30minChan)
	go writeToFile("candles_240m.csv", candles240minChan)

	wg.Wait()
}

func groupTradesByTimestampInterval(tradeChannelData <-chan Trade) (<-chan Trade, <-chan Trade, <-chan Trade) {
	trade5minData := make(chan Trade)
	trade30minData := make(chan Trade)
	trade240minData := make(chan Trade)

	go func(<-chan Trade, <-chan Trade, <-chan Trade) {
		defer close(trade5minData)
		defer close(trade30minData)
		defer close(trade240minData)

		for tradeVal := range tradeChannelData {
			trade5minData <- tradeVal
			trade30minData <- tradeVal
			trade240minData <- tradeVal
		}
	}(trade5minData, trade30minData, trade240minData)

	return trade5minData, trade30minData, trade240minData
}

func computeCandleFromTrade(trades []Trade, timestamp time.Time) Candle {
	candle := Candle{
		Ticker:       trades[0].Ticker,
		OpeningPrice: trades[0].Price,
		MaxPrice:     getMaxCandlePrice(trades),
		MinPrice:     getMinCandlePrice(trades),
		ClosingPrice: trades[len(trades)-1].Price,
		Timestamp:    timestamp,
	}

	return candle
}

func createCandles(tickers map[string][]Trade, start time.Time) []Candle {
	var candle Candle

	var candles []Candle

	for _, tradeVal := range tickers {
		candle = computeCandleFromTrade(tradeVal, start)
		candles = append(candles, candle)
	}

	sort.Slice(candles, func(lhs, rhs int) bool {
		return candles[lhs].MinPrice < candles[rhs].MinPrice
	})

	return candles
}

func createCandleFromTradeWithInterval(tradeDataChannel <-chan Trade, candleDataChannel chan []Candle, interval time.Duration) {
	tickers := make(map[string][]Trade)

	var ts time.Time

	var err error

	for trade := range tradeDataChannel {
		if trade.Timestamp.Hour() < 7 && trade.Timestamp.Hour() >= 3 {
			candles := createCandles(tickers, ts)

			day := strings.Split(trade.Timestamp.String(), " ")[0]

			ts, err = time.Parse(time.RFC3339, day+"T07:00:00Z")

			if err != nil {
				fmt.Println("can`t parse the time: ", err)
			}

			candleDataChannel <- candles

			tickers = map[string][]Trade{}

			continue
		}

		if ts.Add(interval).Before(trade.Timestamp) {
			candles := createCandles(tickers, ts)
			candleDataChannel <- candles

			tickers = map[string][]Trade{}

			ts = ts.Add(interval)
		}

		tickers[trade.Ticker] = append(tickers[trade.Ticker], trade)
	}

	candles := createCandles(tickers, ts)
	candleDataChannel <- candles
}

func getCandlesWithIntervals(tradeChannelData5min, tradeChannelData30min, tradeChannelData240min <-chan Trade) (<-chan []Candle, <-chan []Candle, <-chan []Candle) {
	candles5minChan := make(chan []Candle)
	candles30minChan := make(chan []Candle)
	candles240minChan := make(chan []Candle)

	constructCandle := func(tradeChannelData <-chan Trade, candleChannelData chan []Candle, interval time.Duration) {
		defer close(candleChannelData)
		createCandleFromTradeWithInterval(tradeChannelData, candleChannelData, interval)
	}

	go constructCandle(tradeChannelData5min, candles5minChan, 5*time.Minute)       //nolint
	go constructCandle(tradeChannelData30min, candles30minChan, 30*time.Minute)    //nolint
	go constructCandle(tradeChannelData240min, candles240minChan, 240*time.Minute) //nolint

	return candles5minChan, candles30minChan, candles240minChan
}

func createPipeline(fileReadingChan <-chan Trade) {
	candles5min, candles30min, candles240min := groupTradesByTimestampInterval(fileReadingChan)
	candles5minChan, candles30minChan, candles240minChan := getCandlesWithIntervals(candles5min, candles30min, candles240min)

	writeResult(candles5minChan, candles30minChan, candles240minChan)
}

func main() {
	start := make(chan struct{})

	waitTime := 5 * time.Second //nolint
	cntx, finish := context.WithTimeout(context.Background(), waitTime)

	defer finish()

	var filename string

	flag.StringVar(&filename, "file", "", "")
	flag.Parse()

	fileReadingChan, err := readFileConcurrently(cntx, filename, start)
	if err != nil {
		log.Fatal("can`t read file: ", err)
	}

	start <- struct{}{}

	createPipeline(fileReadingChan)
}
