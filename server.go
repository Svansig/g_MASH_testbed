package main

import (
	"context"
	"github.com/gofiber/fiber"
	"github.com/gofiber/websocket"
	"github.com/segmentio/kafka-go"
	"log"
	"strconv"
	"sync"
	"time"
)

func main() {
	app := fiber.New()

	app.Get("/ws", websocket.New(func(c *websocket.Conn) {
		for {
			// get a JSON string
			type Messages struct {
				Topic string
				Num   int
			}

			m  := Messages{}

			err := c.ReadJSON(&m)
			if err != nil {
				log.Printf("error reading ws message: %s", err)
				break
			}

			log.Printf("message received: %+v", m)
			
			w := kafka.NewWriter(kafka.WriterConfig{
				Brokers:  []string{"localhost:9092"},
				Topic:    m.Topic,
				Balancer: &kafka.LeastBytes{},
			})
			
			var wg sync.WaitGroup
			var mu sync.Mutex
			
			start := time.Now()


			for i := 0; i <= m.Num; i++ {
				wg.Add(1)
				go sendMessage(i, w, c, &wg, &mu)
			}
			wg.Wait()
			log.Printf("%d messages sent in %d ms.", m.Num, time.Since(start).Milliseconds())
			
		}
	}))
	
	app.Static("/main.js", "./public/main.js")
	app.Static("/", "./public/index.html")
	
	log.Fatal(app.Listen(2024))
}

func sendMessage(key int, w *kafka.Writer, c *websocket.Conn, wg *sync.WaitGroup, mu *sync.Mutex) {
	type Message struct {
		Key   int
		Value string
	}
	defer wg.Done()
	// k := Message{Key: key, Value: "Hello from Go!"}
	
	err := w.WriteMessages(context.Background(), kafka.Message{Key: []byte(strconv.Itoa(key)), Value: []byte("Hello from Go! - " + strconv.Itoa(key))})
	if err != nil {
		log.Println("Error writing to Kafka: ", err)
	}
	// mu.Lock()
	// defer mu.Unlock()
	// err = c.WriteJSON(k)
	// log.Printf("message send: %+v", k)
	if err != nil {
		log.Println("Error sending websocket message:", err)
	}
}
