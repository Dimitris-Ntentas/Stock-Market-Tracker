package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type TradeMessage struct {
	Type string `json:"type"`
	Data []struct {
		Symbol    string  `json:"s"`
		Price     float64 `json:"p"`
		Volume    float64 `json:"v"`
		Timestamp int64   `json:"t"`
	} `json:"data"`
}

type TradeData struct {
	Trades      []Trade
	Mutex       sync.Mutex
	File        *os.File
	Candlestick *os.File
	MovingAvg   *os.File
}

type Trade struct {
	Price     float64
	Volume    float64
	Timestamp time.Time
}

var symbols = []string{"TSLA", "NVDA", "AAPL", "GOOGL", "IC MARKETS:1", "BINANCE:BTCUSDT"}
var tradeDataMap = make(map[string]*TradeData)
var wg sync.WaitGroup

func main() {
	// Initialize files and data structures
	for _, s := range symbols {
		tradeDataMap[s] = initTradeData(s)
	}

	// Start ticker for calculations
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			calculateCandlesticks()
			calculateMovingAverages()
		}
	}()

	for {
		err := connectAndSubscribe()
		if err != nil {
			log.Println("Error during connection, retrying in 1 second:", err)
			time.Sleep(1 * time.Second)
			continue
		}
	}
}

func connectAndSubscribe() error {
	w, _, err := websocket.DefaultDialer.Dial("wss://ws.finnhub.io?token=coudikpr01qhf5nregn0coudikpr01qhf5nregng", nil)
	if err != nil {
		return err
	}
	defer w.Close()

	for _, s := range symbols {
		msg, _ := json.Marshal(map[string]interface{}{"type": "subscribe", "symbol": s})
		err := w.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			return err
		}
	}

	for {
		_, message, err := w.ReadMessage()
		if err != nil {
			return err
		}

		var tradeMsg TradeMessage
		if err := json.Unmarshal(message, &tradeMsg); err != nil {
			log.Println("Unmarshal error:", err)
			continue
		}

		processTrades(tradeMsg)
	}
}

func initTradeData(symbol string) *TradeData {
	tradesFile, err := os.OpenFile(symbol+"_trades.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open trades file for symbol %s: %v", symbol, err)
	}

	candlestickFile, err := os.OpenFile(symbol+"_candlestick.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open candlestick file for symbol %s: %v", symbol, err)
	}

	movingAvgFile, err := os.OpenFile(symbol+"_moving_avg.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open moving average file for symbol %s: %v", symbol, err)
	}

	return &TradeData{
		Trades:      []Trade{},
		File:        tradesFile,
		Candlestick: candlestickFile,
		MovingAvg:   movingAvgFile,
	}
}

func processTrades(tradeMsg TradeMessage) {
	for _, data := range tradeMsg.Data {
		trade := Trade{
			Price:     data.Price,
			Volume:    data.Volume,
			Timestamp: time.Unix(0, data.Timestamp*int64(time.Millisecond)),
		}

		tradeDataMap[data.Symbol].Mutex.Lock()
		tradeDataMap[data.Symbol].Trades = append(tradeDataMap[data.Symbol].Trades, trade)
		writeTradeToFile(tradeDataMap[data.Symbol].File, trade)
		tradeDataMap[data.Symbol].Mutex.Unlock()
	}
}

func writeTradeToFile(file *os.File, trade Trade) {
	timeFormatted := trade.Timestamp.Format("2006-01-02 15:04:05")
	line := fmt.Sprintf("Price: %.2f | Volume: %.4f | Timestamp: %s\n", trade.Price, trade.Volume, timeFormatted)
	fmt.Println("Writing to: ", file.Name())
	fmt.Println(line)
	file.WriteString(line)
}

func calculateCandlesticks() {
	for _, symbol := range symbols {
		tradeDataMap[symbol].Mutex.Lock()
		trades := tradeDataMap[symbol].Trades
		if len(trades) == 0 {
			tradeDataMap[symbol].Mutex.Unlock()
			continue
		}

		// Calculate candlestick for the last minute
		var open, close, high, low, volume float64
		open = trades[0].Price
		close = trades[len(trades)-1].Price
		high, low = trades[0].Price, trades[0].Price

		for _, trade := range trades {
			if trade.Price > high {
				high = trade.Price
			}
			if trade.Price < low {
				low = trade.Price
			}
			volume += trade.Volume
		}

		// Write candlestick data to file
		candlestickLine := fmt.Sprintf("Open: %.2f | Close: %.2f | High: %.2f | Low: %.2f | Volume: %.4f\n", open, close, high, low, volume)
		tradeDataMap[symbol].Candlestick.WriteString(candlestickLine)

		// Clear trades for the next minute
		tradeDataMap[symbol].Trades = []Trade{}
		tradeDataMap[symbol].Mutex.Unlock()
	}
}

func calculateMovingAverages() {
	for _, symbol := range symbols {
		tradeDataMap[symbol].Mutex.Lock()
		trades := tradeDataMap[symbol].Trades
		if len(trades) == 0 {
			tradeDataMap[symbol].Mutex.Unlock()
			continue
		}

		var sumPrices, sumVolumes float64
		count := 0
		now := time.Now()

		for i := len(trades) - 1; i >= 0; i-- {
			if now.Sub(trades[i].Timestamp).Minutes() <= 15 {
				sumPrices += trades[i].Price
				sumVolumes += trades[i].Volume
				count++
			} else {
				break
			}
		}

		if count > 0 {
			movingAvgPrice := sumPrices / float64(count)
			movingAvgLine := fmt.Sprintf("Moving Avg Price: %.2f | Total Volume: %.4f\n", movingAvgPrice, sumVolumes)
			tradeDataMap[symbol].MovingAvg.WriteString(movingAvgLine)
		}

		tradeDataMap[symbol].Mutex.Unlock()
	}
}
