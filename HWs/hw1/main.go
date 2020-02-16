package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

func getMaxDifference(high []float64, low []float64) (float64, float64, float64) {
	sort.Float64s(high)
	sort.Float64s(low)
	return (high[len(high)-1] - low[0]), high[len(high)-1], low[0]
}

func getMaxRevenueForEachCompany() ([]float64, []float64, []float64) {
	csvfile, err := os.Open("candles_5m.csv")
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	reader := csv.NewReader(csvfile)
	var appleStocksHigh []float64
	var appleStocksLow []float64

	var amazonStocksHigh []float64
	var amazonStocksLow []float64

	var sberStocksHigh []float64
	var sberStocksLow []float64
	for {
		stockData, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		if stockData[0] == "AAPL" {
			if value, err := strconv.ParseFloat(stockData[3], 32); err == nil {
				appleStocksHigh = append(appleStocksHigh, value)
			}

			if value, err := strconv.ParseFloat(stockData[4], 32); err == nil {
				appleStocksLow = append(appleStocksLow, value)
			}
		} else if stockData[0] == "AMZN" {
			if value, err := strconv.ParseFloat(stockData[3], 32); err == nil {
				amazonStocksHigh = append(amazonStocksHigh, value)
			}

			if value, err := strconv.ParseFloat(stockData[4], 32); err == nil {
				amazonStocksLow = append(amazonStocksLow, value)
			}
		} else {
			if value, err := strconv.ParseFloat(stockData[3], 32); err == nil {
				sberStocksHigh = append(sberStocksHigh, value)
			}

			if value, err := strconv.ParseFloat(stockData[4], 32); err == nil {
				sberStocksLow = append(sberStocksLow, value)
			}
		}
	}

	appleMaxDifference, appleHigh, appleLow := getMaxDifference(appleStocksHigh, appleStocksLow)
	amazonMaxDifference, amazonHigh, amazonLow := getMaxDifference(amazonStocksHigh, amazonStocksLow)
	sberMaxDifference, sberHigh, sberLow := getMaxDifference(sberStocksHigh, sberStocksLow)

	return []float64{appleMaxDifference, appleHigh, appleLow}, []float64{amazonMaxDifference, amazonHigh, amazonLow}, []float64{sberMaxDifference, sberHigh, sberLow}
}

func getUsersRevenue(companyTicker string) map[string]string {
	csvfile, err := os.Open("user_trades.csv")
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}
	r := csv.NewReader(csvfile)

	tableBuy := make(map[string]string)
	tableSell := make(map[string]string)

	tableResult := make(map[string]string)

	for {
		stock, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		if stock[2] == companyTicker {
			if stock[3] != "0" {
				tableBuy[stock[0]] = stock[3]
			}

			if stock[4] != "0" {
				tableSell[stock[0]] = stock[4]
			}
		}
	}

	for stock := range tableBuy {
		var buyStock float64
		var sellStock float64

		if val, err := strconv.ParseFloat(tableSell[stock], 64); err == nil {
			sellStock = val
		}

		if val, err := strconv.ParseFloat(tableBuy[stock], 64); err == nil {
			buyStock = val
		}

		res := betterFormat(sellStock - buyStock)
		tableResult[stock] = res
	}

	return tableResult
}

func betterFormat(num float64) string {
	s := fmt.Sprintf("%.2f", num)
	return strings.TrimRight(strings.TrimRight(s, "0"), ".")
}

func getTimeToSellAndBuyForCompany(companyTicker string, highVal float64, lowVal float64) []string {
	csvfile, err := os.Open("candles_5m.csv")
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	r := csv.NewReader(csvfile)

	var buySellTimeAndDate []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		if companyTicker == "AAPL" {
			if record[0] == "AAPL" && record[3] == betterFormat(highVal) {
				buySellTimeAndDate = append(buySellTimeAndDate, record[1])
			}

			if record[0] == "AAPL" && record[4] == betterFormat(lowVal) {
				buySellTimeAndDate = append(buySellTimeAndDate, record[1])
			}
		} else if companyTicker == "AMZN" {
			if record[0] == "AMZN" && record[3] == betterFormat(highVal) {
				buySellTimeAndDate = append(buySellTimeAndDate, record[1])
			}

			if record[0] == "AMZN" && record[4] == betterFormat(lowVal) {
				buySellTimeAndDate = append(buySellTimeAndDate, record[1])
			}
		} else {
			if record[0] == "SBER" && record[3] == betterFormat(highVal) {
				buySellTimeAndDate = append(buySellTimeAndDate, record[1])
			}

			if record[0] == "SBER" && record[4] == betterFormat(lowVal) {
				buySellTimeAndDate = append(buySellTimeAndDate, record[1])
			}
		}
	}

	return buySellTimeAndDate
}

func main() {
	appleMaxRevenueArr, amazonMaxRevenueArr, sberMaxRevenueArr := getMaxRevenueForEachCompany()

	appleMaxRevenue := appleMaxRevenueArr[0]
	amazonMaxRevenue := amazonMaxRevenueArr[0]
	sberMaxRevenue := sberMaxRevenueArr[0]

	appleUsersRevenueMap := getUsersRevenue("AAPL")
	appleBestTimes := getTimeToSellAndBuyForCompany("AAPL", appleMaxRevenueArr[1], appleMaxRevenueArr[2])

	var data [][]string

	for stock := range appleUsersRevenueMap {
		userID := stock
		ticker := "AAPL"
		userRevenue := appleUsersRevenueMap[stock]
		currentMaxRevenue := betterFormat(appleMaxRevenue)

		var usersRevenueFloat float64
		if val, err := strconv.ParseFloat(appleUsersRevenueMap[stock], 64); err == nil {
			usersRevenueFloat = val
		}

		diff := betterFormat(appleMaxRevenue - usersRevenueFloat)
		buyDate := appleBestTimes[0]
		sellDate := appleBestTimes[1]

		data = append(data, []string{userID, ticker, userRevenue, currentMaxRevenue, diff, sellDate, buyDate})
	}

	amazonUsersRevenueMap := getUsersRevenue("AMZN")
	amazonBestTimes := getTimeToSellAndBuyForCompany("AMZN", amazonMaxRevenueArr[1], amazonMaxRevenueArr[2])

	for stock := range amazonUsersRevenueMap {
		userID := stock
		ticker := "AMZN"
		userRevenue := amazonUsersRevenueMap[stock]
		currentMaxRevenue := betterFormat(amazonMaxRevenue)

		var usersRevenueFloat float64
		if val, err := strconv.ParseFloat(amazonUsersRevenueMap[stock], 64); err == nil {
			usersRevenueFloat = val
		}

		diff := betterFormat(amazonMaxRevenue - usersRevenueFloat)
		buyDate := amazonBestTimes[0]
		sellDate := amazonBestTimes[1]

		data = append(data, []string{userID, ticker, userRevenue, currentMaxRevenue, diff, sellDate, buyDate})
	}

	sberUsersRevenueMap := getUsersRevenue("SBER")
	sberBestTimes := getTimeToSellAndBuyForCompany("SBER", sberMaxRevenueArr[1], sberMaxRevenueArr[2])

	for stock := range sberUsersRevenueMap {
		userID := stock
		ticker := "SBER"
		userRevenue := sberUsersRevenueMap[stock]
		currentMaxRevenue := betterFormat(sberMaxRevenue)

		var usersRevenueFloat float64
		if val, err := strconv.ParseFloat(sberUsersRevenueMap[stock], 64); err == nil {
			usersRevenueFloat = val
		}

		diff := betterFormat(sberMaxRevenue - usersRevenueFloat)
		buyDate := sberBestTimes[0]
		sellDate := sberBestTimes[1]

		data = append(data, []string{userID, ticker, userRevenue, currentMaxRevenue, diff, sellDate, buyDate})
	}

	file, err := os.Create("result.csv")
	checkError("Cannot create file", err)
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, value := range data {
		err := writer.Write(value)
		checkError("Cannot write to file", err)
	}
}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}
