package main

import (
	"context"
	"errors"
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

	var (
		cardID string
		err    error
	)

	createCard := func(ctx context.Context) error {
		//processando k1
		fmt.Println("processando k1")
		cardID, err = CreateCard(orderID)
		if err != nil {
			return err
		}

		return nil
	}

	err = ik.Once(ctx, createCard, time.Minute*5, "create-card", orderID)
	if err != nil {
		log.Fatal(err)
		return
	}

	err = ik.Once(ctx, func(ctx context.Context) error {
		//	//processando k2
		if err := UpdateCard(cardID, "123"); err != nil {
			return err
		}

		return nil
	}, time.Minute*5, "update-card", orderID, cardID)

}

func CreateCard(orderID string) (cardID string, err error) {
	fmt.Println("running create card")
	time.Sleep(time.Second)
	cardID = "f9097b9b-1cc2-442a-a06a-7aea467dcae2"
	return
}

func UpdateCard(cardID string, value string) error {
	fmt.Println("running update card")
	time.Sleep(time.Second)
	return errors.New("updated with error")
}
