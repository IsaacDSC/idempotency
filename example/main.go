package main

import (
	"context"
	"fmt"
	"github.com/IsaacDSC/idempotency"
	"github.com/redis/go-redis/v9"
	"log"
	"time"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	fmt.Println("ping", rdb.Conn().Ping(context.Background()))
	ctx := context.Background()

	defer rdb.Close()

	ik := idempotency.NewIdempotencyKey(rdb.Conn(), false, ":")

	orderID := "71230ada-b9fd-480b-836a-0aaecf9944fb"
	key1, alreadyExec1 := ik.CreateIdempotencyKey(ctx, time.Minute*5, "create-card", orderID)

	var (
		cardID string
		err    error
	)
	if !alreadyExec1 {
		//processando k1
		cardID, err = CreateCard(orderID)
		if err != nil {
			log.Fatal(err)
			return
		}
	}

	key2, alreadyExec2 := ik.CreateIdempotencyKey(ctx, time.Minute*5, "update-card", orderID, cardID)
	if !alreadyExec2 {
		//processando k2
		if err := UpdateCard(cardID, "123"); err != nil {
			log.Fatal(err)
			return
		}
	}

	fmt.Println("Keys", key1, key2)
	ik.Commit(ctx, []string{key1, key2})
}

func CreateCard(orderID string) (cardID string, err error) {
	time.Sleep(time.Second)
	cardID = "f9097b9b-1cc2-442a-a06a-7aea467dcae2"
	return
}

func UpdateCard(cardID string, value string) error {
	time.Sleep(time.Second)
	return nil
}
