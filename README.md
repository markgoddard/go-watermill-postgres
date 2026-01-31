# Demo of event handling in Go using Watermill and PostgreSQL

Aims:
Demonstrate how [Watermill](https://watermill.io/) can be used in a Go application with a PostgreSQL backend to enable an Event-Driven Architecture (EDA).
Consume events using both fan-out/broadcast and queuing semantics.

Start the postgres container:

```sh
docker compose up -d
```

Start one instance of the application:

```sh
go run main.go
```

In another terminal, start a second instance of the application:

```sh
go run main.go
```

You should see both application instances publishing an event every 3 seconds.
Each instance should receive every event as a `LISTENER` (fan-out/broadcast semantics).
One instance should receive every event as a `WORKER` (queue semantics).

*Created with help from Gemini.*
