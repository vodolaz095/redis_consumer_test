package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vodolaz095/pkg/stopper"
	"golang.org/x/sync/errgroup"
)

const publishInterval = time.Millisecond
const consumerLag = 10 * time.Millisecond
const channelName = "RedisConsumerTest"
const nConsumers = 10
const limit = 1000
const bufferSize = 10
const nConcurrentConsumers = 10
const nSharedChannelConsumers = 10

func main() {
	trigger := make(chan bool, 1)
	stopCtx, cancel := stopper.New()
	defer cancel()
	dsn := os.Getenv("REDIS_URL")
	if dsn == "" {
		dsn = "redis://localhost:6379"
	}
	var nSend int64
	var nReceived int64
	stats := make([]int64, nConsumers)
	concurrentStats := make([]int64, nConcurrentConsumers)
	concurrentChannelStats := make([]int64, nConcurrentConsumers)
	opts, err := redis.ParseURL(dsn)
	if err != nil {
		log.Fatalf("ошибка синтаксиса строки соединения с redis: %s", err)
	}
	log.Printf("Соединяемся с редисом через %s...", opts.Addr)

	eg, ctx := errgroup.WithContext(stopCtx)

	eg.Go(func() error {
		publisher := redis.NewClient(opts)
		ticker := time.NewTicker(publishInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Println("Отправитель останавливается")
				return nil
			case <-ticker.C:
				atomic.AddInt64(&nSend, 1)
				errP := publisher.Publish(ctx, channelName, nSend).Err()
				if errP != nil {
					return errP
				}
				if nSend == limit/2 {
					trigger <- true
				}

				log.Printf("Сообщение %v отправлено", nSend)
				if nSend == limit {
					log.Println("Достигнут лимит сообщений")
					return nil
				}
			}
		}
	})

	for i := 0; i < nConsumers; i++ {
		eg.Go(func() error { // создаём 10 потребителей, каждый из них обладает своим соединением с редис
			var counter int64
			s := redis.NewClient(opts).Subscribe(ctx, channelName)
			c := s.Channel(redis.WithChannelSize(bufferSize))
			defer s.Close()
			for {
				select {
				case <-ctx.Done():
					log.Printf("Потребитель %v останавливается", i)
					return nil
				case msg := <-c:
					time.Sleep(consumerLag)
					atomic.AddInt64(&nReceived, 1)
					atomic.AddInt64(&counter, 1)
					log.Printf("Сообщение %s принято потребителем %v", msg.String(), i)
					if msg.Payload == fmt.Sprintf("%v", limit) {
						log.Printf("Все сообщения приняты. Потребитель %v обработал %v сообщений", i, counter)
						stats[i] = counter
						return nil
					}
				}
			}
		})
	}

	sharedChannelClient := redis.NewClient(opts).Subscribe(ctx, channelName)
	sharedChannel := sharedChannelClient.Channel()
	defer sharedChannelClient.Close()
	sharedChannelContext, sharedChannelCancel := context.WithCancel(ctx)
	// создаём 10 потребителей, каждый из них использует один и тот же клиент и канал подписок
	for j := 0; j < nSharedChannelConsumers; j++ {
		eg.Go(func() error {
			var counter int64
			for {
				select {
				case <-sharedChannelContext.Done():
					concurrentStats[j] = counter
					log.Printf("Конкурентный потребитель %v останавливается", j)
					return nil
				case msg := <-sharedChannel:
					time.Sleep(consumerLag)
					atomic.AddInt64(&nReceived, 1)
					atomic.AddInt64(&counter, 1)
					log.Printf("Сообщение %s принято конкурентным потребителем %v", msg.String(), j)
					if msg.Payload == fmt.Sprintf("%v", limit) {
						log.Printf("Все сообщения приняты. Конкурентный потребитель %v обработал %v сообщений", j, counter)
						concurrentStats[j] = counter
						sharedChannelCancel()
						return nil
					}
				}
			}
		})
	}

	concurrentChannelConsumerClient := redis.NewClient(opts).Subscribe(ctx, channelName)
	defer concurrentChannelConsumerClient.Close()
	concurrentChannelContext, concurrentChannelCancel := context.WithCancel(ctx)
	// создаём 10 потребителей, каждый из них использует одно и тоже соединение, но разные каналы
	for k := 0; k < nConcurrentConsumers; k++ {
		eg.Go(func() error {
			concurrentConsumerChannel := concurrentChannelConsumerClient.Channel()
			var counter int64
			for {
				select {
				case <-concurrentChannelContext.Done():
					concurrentChannelStats[k] = counter
					log.Printf("Конкурентный потребитель %v останавливается", k)
					return nil
				case msg := <-concurrentConsumerChannel:
					time.Sleep(consumerLag)
					atomic.AddInt64(&nReceived, 1)
					atomic.AddInt64(&counter, 1)
					log.Printf("Сообщение %s принято конкурентным потребителем %v", msg.String(), k)
					if msg.Payload == fmt.Sprintf("%v", limit) {
						log.Printf("Все сообщения приняты. Конкурентный потребитель %v обработал %v сообщений", k, counter)
						concurrentChannelStats[k] = counter
						concurrentChannelCancel()
						return nil
					}
				}
			}
		})
	}

	var delayedCounter int64
	// создаём потребителя, который запускается, когда будут отправлены 50% сообщений, в итоге он потеряет более 50% сообщений
	eg.Go(func() error {
		<-trigger
		s := redis.NewClient(opts).Subscribe(ctx, channelName)
		c := s.Channel(redis.WithChannelSize(bufferSize))
		defer s.Close()
		for {
			select {
			case <-ctx.Done():
				log.Printf("Отложенный потребитель останавливается")
				return nil
			case msg := <-c:
				time.Sleep(consumerLag)
				atomic.AddInt64(&nReceived, 1)
				atomic.AddInt64(&delayedCounter, 1)
				log.Printf("Сообщение %s принято отложенным потребителем", msg.String())
				if msg.Payload == fmt.Sprintf("%v", limit) {
					log.Printf("Все сообщения приняты. Отложенный потребитель обработал %v сообщений", delayedCounter)
					return nil
				}
			}
		}
	})

	err = eg.Wait()
	if err != nil {
		log.Printf("eg error: %s", err)
	}
	log.Println("===============================================================")
	log.Println("Приложение остановлено!")
	log.Printf("Интервал отправки: %s", publishInterval)
	log.Printf("Задержка обработки: %s", consumerLag)
	log.Printf("Количество потребителей : %v", nConsumers)
	log.Printf("Количество конкурентных потребителей с разделённым каналами : %v", nSharedChannelConsumers)
	log.Printf("Количество конкурентных потребителей с уникальными каналами : %v", nConcurrentConsumers)
	log.Printf("Размер буфера канала : %v", bufferSize)
	log.Printf("Отправлено сообщений: %v", nSend)
	log.Printf("Принято всего: %v", nReceived)
	log.Printf("Должно быть принято: %v", nSend*(nConsumers)+nSend*3)
	log.Printf("Доля потерянных сообщений: %.2f%%", 100-(100*float64(nReceived)/float64(nSend*(nConsumers)+nSend+nSend)))
	for i := range stats {
		log.Printf("Потребитель %v: принял %v сообщений, доля потерянных сообщений: %.2f%%",
			i, stats[i], 100-float64(100*float64(stats[i])/float64(nSend)),
		)
	}
	for i := range concurrentStats {
		log.Printf("Конкуренный потребитель на одном канале %v: принял %v сообщений, доля потерянных сообщений: %.2f%%",
			i, concurrentStats[i], 100-float64(1000*float64(concurrentStats[i])/float64(nSend)),
		)
	}
	for i := range concurrentChannelStats {
		log.Printf("Конкуренный потребитель с персональным каналом %v: принял %v сообщений, доля потерянных сообщений: %.2f%%",
			i, concurrentChannelStats[i], 100-float64(1000*float64(concurrentChannelStats[i])/float64(nSend)),
		)
	}
	log.Printf("Отложенный потребитель принял %v сообщений, доля потерянных: %.2f%%",
		delayedCounter, 100-float64(100*float64(delayedCounter)/float64(nSend)),
	)
}
