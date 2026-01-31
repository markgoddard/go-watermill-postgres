package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	watermillsql "github.com/ThreeDotsLabs/watermill-sql/v2/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	_ "github.com/lib/pq" // Postgres driver
)

/*
Moving the proof-of-concept to production requires addressing several reliability, performance, and maintenance issues—specifically the "Zombie Data" problem created by dynamic consumer groups.

Here is the checklist to make this application production-ready:

### 1. Implement a "Zombie" Offset Cleaner

**The Problem:** In the demo, every time a pod restarts, it creates a new Consumer Group ID (e.g., `OrderHandler_uuid1`). The database retains the offset tracking rows for `uuid1` forever, even after the pod is gone. Over weeks, your `watermill_offsets` table will contain millions of rows, slowing down the database.

**The Solution:** You need a background worker (or a cron job) to delete inactive consumer groups.

* **Logic:** Delete rows where the `consumer_group` matches your dynamic pattern AND hasn't been updated in  hours.
* **Example SQL:**
```sql
DELETE FROM watermill_offsets
WHERE consumer_group LIKE '%_instance_%'
AND last_read_at < NOW() - INTERVAL '24 hours';

```

### 2. Use the Transactional Outbox Pattern

**The Problem:** In the demo, we publish the event directly. If your application crashes *after* saving an order to your DB but *before* publishing the event, your system is in an inconsistent state.

**The Solution:** Use Watermill’s **Forwarder** component or the SQL Publisher inside a transaction.

1. Start a SQL Transaction.
2. Insert your business data (e.g., the "Order" row).
3. Publish the event using the Watermill SQL Publisher *within the same transaction*.
4. Commit the transaction.
5. Watermill's background "Forwarder" picks up the message and sends it to the actual subscriber (or ensures it is processed).

### 3. Replace "Auto-Init" with Proper Migrations

**The Problem:** The demo uses `InitializeSchema: true`. In production, having multiple pods racing to create tables on startup is dangerous and can cause locking issues.

**The Solution:**

* Disable `InitializeSchema` in your Go code.
* Copy the SQL Schema from the Watermill library documentation.
* Add it to your standard migration tool (e.g., `golang-migrate`, `Flyway`) to run *before* the application starts.

### 4. Database Connection Tuning

**The Problem:** The standard `sql.Open` uses default settings which can lead to connection exhaustion or latency in high-throughput environments.

**The Solution:** Configure the `*sql.DB` object explicitly:

```go
db.SetMaxOpenConns(25)       // Limit total connections
db.SetMaxIdleConns(5)        // Keep some open for speed
db.SetConnMaxLifetime(5 * time.Minute) // Rotate connections to prevent stale timeouts

```

### 5. Structured Logging & Metrics

**The Problem:** `log.Printf` is insufficient for debugging distributed systems.

**The Solution:**

* **Structured Logging:** Inject a logger like **Zap** or **Logrus** into Watermill. This allows you to filter logs by `message_uuid` or `handler_name`.
* **Metrics:** Wrap your publisher/subscriber with the **Watermill Prometheus Middleware**. This gives you Grafana dashboards for "Messages processed per second," "Processing duration," and "Error rates."

### 6. Poison Queue (Dead Letter Queue)

**The Problem:** If a handler crashes (panics) or returns an error while processing an event, Watermill (by default) will retry indefinitely. This blocks the entire queue for that specific Consumer Group.

**The Solution:** Configure the `PoisonQueue` middleware.

* If a message fails  times, move it to a separate topic (e.g., `events_poison`).
* This allows the main stream to continue while you investigate the bad message later.

```go
// Example Middleware configuration
poisonQueue, _ := middleware.NewPoisonQueue(
    publisher,
    "events_poison",
)
router.AddMiddleware(poisonQueue.Middleware)

```

### 7. Graceful Shutdown

**The Problem:** The demo uses a basic `signal.NotifyContext`. If a message is half-processed when the pod receives `SIGTERM`, it might result in duplicate processing later.

**The Solution:** Ensure the `router.Close()` method is called and waited upon. Watermill handles this well, but your `main` function needs to wait for the router to finish closing before exiting the process completely.

### Summary Checklist

| Component | Dev / Demo | Production Ready |
| --- | --- | --- |
| **Consumer IDs** | Random UUID | Hostname (StatefulSet) or UUID + **Cleanup Job** |
| **Consistency** | Direct Publish | **Transactional Outbox** |
| **Schema** | Auto-Init | **SQL Migrations** |
| **Errors** | Infinite Retry | **Poison Queue / DLQ** |
| **Observability** | `fmt.Printf` | **Prometheus + Structured Log** |
| **Config** | Hardcoded | Environment Variables (`kelseyhightower/envconfig`) |

Would you like to see the code for the **Cleanup SQL Job** or the **Transactional Outbox** setup?
*/

/*
FIXME: When a new process starts, it receives all historical events. Is it possible to only receive new events?

Yes, by default, a new Consumer Group ID starts reading from the "beginning" of the topic history.

To receive **only new events** (events published *after* your process starts), you have two options.

### Option 1: The "Application Filter" (Simplest)

Accept all messages, but discard the old ones in your handler.

* **Pros:** Very easy to implement.
* **Cons:** If you have 1 million historical events, your application will still download and acknowledge 1 million messages on startup, which wastes network bandwidth and CPU.

**Implementation:**
Capture the startup timestamp in `main()` and pass it to your handler.

```go
// In main.go
startupTime := time.Now()
// ... register handler: &OrderCreatedHandler{..., StartTime: startupTime}

// In Handler
func (h OrderCreatedHandler) Handle(ctx context.Context, event interface{}) error {
    e := event.(*OrderCreated)
    if e.CreatedAt.Before(h.StartTime) {
        return nil // Skip old event
    }
    // Process new event...
}

```

---

### Option 2: The "Offset Fast-Forward" (Production Grade)

This is the performance-optimized approach. You force the database to believe this new subscriber has already read everything up to the current moment.

Since Watermill doesn't have a built-in `StartFromLatest` config for SQL, we can create a **Subscriber Wrapper** that intercepts the `Subscribe` call and "fast-forwards" the offsets table before listening.

Here is the complete solution using a Wrapper.

#### 1. Add this Helper Struct

Add this code to your project. It wraps the standard subscriber.

```go
import (
    "context"
    "database/sql"
    "fmt"
    "github.com/ThreeDotsLabs/watermill/message"
)

// LatestEventsSubscriber wraps a real subscriber.
// Before subscribing to a topic, it checks the DB for the latest offset
// and marks it as "read" for this consumer group.
type LatestEventsSubscriber struct {
    realSubscriber message.Subscriber
    db             *sql.DB
    consumerGroup  string
}

func (s *LatestEventsSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
    // 1. Fast-forward: Insert a fake "ack" for the latest message in this topic.
    // We utilize "INSERT INTO ... SELECT" to only insert if messages exist.
    // This assumes default table names: 'watermill_messages' and 'watermill_offsets'
    query := `
        INSERT INTO watermill_offsets (consumer_group, topic, offset_acked)
        SELECT $1, $2, MAX("offset")
        FROM watermill_messages
        WHERE topic = $2
        GROUP BY topic
        ON CONFLICT (consumer_group, topic) DO UPDATE
        SET offset_acked = EXCLUDED.offset_acked;
    `

    _, err := s.db.ExecContext(ctx, query, s.consumerGroup, topic)
    if err != nil {
        return nil, fmt.Errorf("failed to fast-forward offsets: %w", err)
    }

    // 2. Now that the DB thinks we are up-to-date, let the real subscriber start.
    return s.realSubscriber.Subscribe(ctx, topic)
}

func (s *LatestEventsSubscriber) Close() error {
    return s.realSubscriber.Close()
}

```

#### 2. Update your `SubscriberConstructor`

Modify the constructor in `main.go` to wrap the SQL subscriber with our new logic.

```go
// ... inside your cqrs.EventProcessorConfig ...
SubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
    uniqueConsumerGroup := fmt.Sprintf("%s_%s", handlerName, instanceID)

    // 1. Create the real SQL subscriber
    sqlSubscriber, err := sql.NewSubscriber(
        db,
        sql.SubscriberConfig{
            ConsumerGroup:    uniqueConsumerGroup,
            SchemaAdapter:    sql.DefaultPostgreSQLSchema{},
            OffsetsAdapter:   sql.DefaultPostgreSQLOffsetsAdapter{},
            InitializeSchema: true,
        },
        logger,
    )
    if err != nil {
        return nil, err
    }

    // 2. Return the Wrapper instead
    return &LatestEventsSubscriber{
        realSubscriber: sqlSubscriber,
        db:             db,
        consumerGroup:  uniqueConsumerGroup,
    }, nil
},

```

### How it works

1. When your application starts, it creates the `uniqueConsumerGroup`.
2. Watermill calls `Subscribe("main.OrderCreated")`.
3. Our wrapper intercepts this call.
4. It runs the SQL: *"Find the MAX ID from messages where topic='OrderCreated' and insert it into offsets for my new group."*
5. It then calls the real subscriber.
6. The real subscriber queries the DB: *"Give me messages > [MAX ID]"*.
7. The DB returns nothing (until a **new** event comes in).
*/

type EventBus interface {
	Publish(ctx context.Context, event any) error
}

// --- 1. Define the Event ---
type OrderCreated struct {
	OrderID   string    `json:"order_id"`
	CreatedAt time.Time `json:"created_at"`
}

// --- 2. Define the Event Handler ---
type OrderCreatedHandler struct {
	InstanceID string
}

func (h OrderCreatedHandler) HandlerName() string {
	return "OrderCreatedHandler"
}

func (h OrderCreatedHandler) NewEvent() any {
	return &OrderCreated{}
}

func (h OrderCreatedHandler) Handle(ctx context.Context, event interface{}) error {
	e := event.(*OrderCreated)
	// Log with the InstanceID to prove different instances are processing the same event
	log.Printf("[%s] 🔔 RECEIVED event: OrderID=%s", h.InstanceID, e.OrderID)
	return nil
}

func main() {
	// Generate a unique ID for THIS running process.
	// In a real K8s app, this might come from os.Hostname() or a pod UID.
	instanceID := uuid.NewString()[:8] // Shorten for readability
	logger := watermill.NewSlogLogger(slog.Default())

	log.Printf("[%s] Starting application instance...", instanceID)

	// --- 3. Database Connection ---
	dbStr := "postgres://user:password@localhost:5432/watermill?sslmode=disable"
	db, err := sql.Open("postgres", dbStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// --- 4. Define SQL Publisher (To send events) ---
	publisher, err := watermillsql.NewPublisher(
		db,
		watermillsql.PublisherConfig{
			SchemaAdapter: watermillsql.DefaultPostgreSQLSchema{},
		},
		logger,
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		publisher.Close()
	}()

	// Initialize the SQL Schema (creates watermill_messages table)
	// We only need to do this once, but doing it on startup is safe.
	if err := publisher.Publish(expectedSchemaInitTopic, &message.Message{}); err != nil {
		// This dummy publish ensures tables exist; ignore errors for this demo
	}

	eventBus, err := cqrs.NewEventBusWithConfig(publisher, cqrs.EventBusConfig{
		GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
			return params.EventName, nil
		},
		Marshaler: cqrs.JSONMarshaler{},
		Logger:    logger,
	})
	if err != nil {
		panic(err)
	}

	// --- 5. Define CQRS Router with BROADCAST Subscriber ---
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}
	defer func() {
		router.Close()
	}()

	eventProcessor, err := cqrs.NewEventProcessorWithConfig(
		router,
		cqrs.EventProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
				return params.EventName, nil
			},
			// PRODUCER: Connects the CQRS bus to the SQL Publisher
			SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				// !!! KEY LOGIC FOR BROADCAST !!!
				// We append the unique instanceID to the handler name.
				// This creates a unique Consumer Group in Postgres (e.g. "OrderCreatedHandler_a1b2c3d4").
				// Therefore, Postgres treats this instance as a completely new subscriber
				// and sends it a copy of every message.
				uniqueConsumerGroup := fmt.Sprintf("%s_%s", params.HandlerName, instanceID)

				return watermillsql.NewSubscriber(
					db,
					watermillsql.SubscriberConfig{
						ConsumerGroup:    uniqueConsumerGroup,
						SchemaAdapter:    watermillsql.DefaultPostgreSQLSchema{},
						OffsetsAdapter:   watermillsql.DefaultPostgreSQLOffsetsAdapter{},
						InitializeSchema: true, // Ensure offsets table exists
					},
					logger,
				)
			},
			Marshaler: cqrs.JSONMarshaler{},
			Logger:    logger,
		},
	)
	if err != nil {
		panic(err)
	}

	err = subscribe(eventProcessor, instanceID)
	if err != nil {
		panic(err)
	}

	// --- 6. Background Publisher (Simulation) ---
	// We start a goroutine that publishes an event every 3 seconds.
	// Only ONE of your running instances needs to do this, but for this demo,
	// it doesn't hurt if both publish.
	go publish(eventBus, instanceID)

	// --- 7. Run Application ---
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("[%s] 🚀 Listening for events. Press Ctrl+C to stop.", instanceID)
	if err := router.Run(ctx); err != nil {
		panic(err)
	}
}

func subscribe(eventProcessor *cqrs.EventProcessor, instanceID string) error {
	// Register the Handler
	return eventProcessor.AddHandlers(&OrderCreatedHandler{InstanceID: instanceID})
}

func publish(eventBus EventBus, instanceID string) {
	for {
		time.Sleep(3 * time.Second)
		newID := uuid.NewString()[:4]
		log.Printf("[%s] 📤 Publishing Order Created: %s", instanceID, newID)

		_ = eventBus.Publish(context.Background(), &OrderCreated{
			OrderID:   newID,
			CreatedAt: time.Now(),
		})
	}
}

// Helper constant just to trigger schema init
var expectedSchemaInitTopic = "schema_init"
