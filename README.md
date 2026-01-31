# Demo of fan-out event handling using Go Watermill and PostgreSQL

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

You should see both application instances publishing an event every 3 seconds, and each receiving every event published.

Created with help from Gemini.
