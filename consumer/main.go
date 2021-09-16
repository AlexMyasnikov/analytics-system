package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
	"log"
)

type Event struct {
	Id string `gorn:primaryKey`
	UserId string
	EventType string
	Path string
	Search string
	Title string
	URL string
	CreatedAt int64 `gorm:"autoCreateTime"`
	UpdatedAt int64 `gorm:"autoUpdateTime"`
}

func SaveEvent(db *gorm.DB, event Event) (Event, error) {
	result := db.Clauses(
		clause.OnConflict{
			UpdateAll: true,
			Columns: []clause.Column{},
		}).Create(&event)
	if result.Error != nil {
		log.Println(result.Error)
		return event, result.Error
	}
	return event, nil
}

func main() {
	dsn := "host=localhost user=alex password=meat dbname=analytics-system port=5432"

	ormConfig := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			TablePrefix: "kafka_",
		},
	}

	db, err := gorm.Open(postgres.Open(dsn), ormConfig)
	if err != nil {
		panic("Unable to connect to db")
	}
	log.Println("=>Connected successfully:", db)

	err = db.AutoMigrate(&Event{})
	if err != nil {
		log.Println("Error migrating schema:", err)
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id": "myGroup",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"PAGE_VIEW"}, nil)

	for  {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			var event Event
			err := json.Unmarshal(msg.Value, &event)
			if err != nil {
				log.Println("=> error converting event object:", err)
			}

			_, err = SaveEvent(db, event)
			if err != nil {
				log.Println("=> error saving event to db...")
			}
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}