package main

import (
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vodolaz095/pkg/stopper"
	"golang.org/x/sync/errgroup"
)

const publishInterval = time.Millisecond
const consumerLag = 100 * time.Millisecond
const channelName = "RedisConsumerTest"
const nConsumers = 10

func main() {
	stopCtx, cancel := stopper.New()
	defer cancel()
	dsn := os.Getenv("REDIS_URL")
	if dsn == "" {
		dsn = "redis://localhost:6379"
	}
	var nSend int64
	var nReceived int64

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
				log.Printf("Сообщение %v отправлено", nSend)
			}
		}
	})

	for i := 0; i < nConsumers; i++ {
		eg.Go(func() error {
			consumer := redis.NewClient(opts)
			s := consumer.Subscribe(ctx, channelName)
			c := s.Channel()
			defer s.Close()
			for {
				select {
				case <-ctx.Done():
					log.Printf("Потребитель %v останавливается", i)
					return nil
				case msg := <-c:
					time.Sleep(consumerLag)
					atomic.AddInt64(&nReceived, 1)
					log.Printf("Сообщение %s принято потребителем %v", msg.String(), i)
				}
			}
		})
	}
	err = eg.Wait()
	if err != nil {
		log.Printf("eg error: %s", err)
	}
	log.Println("Приложение остановлено!")
	log.Printf("Интервал отправки: %s", publishInterval)
	log.Printf("Задержка обработки: %s", consumerLag)
	log.Printf("Количество потребителей : %v", nConsumers)
	log.Printf("Статистика: отправлено %v, принято %v, доля потерянных сообщений: %.2f%%",
		nSend, nReceived, 100-float64(100*nReceived/nSend),
	)
}
