package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/mastrasec/vectoria"
)

const (
	SERVER_HEADER string        = "vectoria-server"
	APP_NAME      string        = "Vectoria"
	READ_TIMEOUT  time.Duration = 10 * time.Second
	WRITE_TIMEOUT time.Duration = 10 * time.Second
)

type entrypoint struct {
	addr string
	path string
	app  *fiber.App
	db   *vectoria.DB
}

func newEntrypoint(addr, path string, shouldLog bool, opts ...vectoria.Options) (*entrypoint, error) {
	db, err := vectoria.New(path, opts...)
	if err != nil {
		return nil, err
	}

	return &entrypoint{
		addr: addr,
		path: path,
		app:  newApp(shouldLog),
		db:   db,
	}, nil
}

func newApp(shouldLog bool) *fiber.App {
	app := fiber.New(fiber.Config{
		ServerHeader: SERVER_HEADER,
		AppName:      APP_NAME,
		ReadTimeout:  READ_TIMEOUT,
		WriteTimeout: WRITE_TIMEOUT,
	})

	app.Use(recover.New())

	if shouldLog {
		app.Use(logger.New())
	}

	return app
}

func (entry *entrypoint) registerRoutes() {
	system := entry.app.Group("/system")
	system.Get("/health", entry.health)

	entry.app.Post("/new", entry.new).
		Post("/add", entry.add).
		Get("/get", entry.get)
}

func (entry *entrypoint) health(ctx *fiber.Ctx) error {
	// TODO
	return nil
}

type addReq struct {
	IndexName string    `json:"index_name"`
	ItemID    string    `json:"item_id"`
	ItemVec   []float64 `json:"item_vec"`
}

type addRes struct{}

func (entry *entrypoint) add(ctx *fiber.Ctx) error {
	payload := &addReq{}

	if err := ctx.BodyParser(payload); err != nil {
		log.Println(err)
		return ctx.Status(http.StatusBadRequest).SendString("{}")
	}

	err := entry.db.Add(payload.ItemID, payload.ItemVec, payload.IndexName)
	if err != nil {
		log.Println(err)
		return ctx.Status(http.StatusInternalServerError).SendString("{}")
	}

	return ctx.Status(http.StatusOK).JSON(&addRes{})
}

type getReq struct {
	IndexName string    `json:"index_name"`
	Query     []float64 `json:"query"`
	Threshold float64   `json:"threshold"`
	K         uint32    `json:"k"`
}

type getRes struct {
	IDs []string `json:"ids"`
}

func (entry *entrypoint) get(ctx *fiber.Ctx) error {
	payload := &getReq{}

	if err := ctx.BodyParser(payload); err != nil {
		log.Println(err)
		return ctx.Status(http.StatusBadRequest).SendString("{}")
	}

	res, err := entry.db.Get(payload.Query, payload.Threshold, payload.K, payload.IndexName)
	if err != nil {
		log.Println(err)
		return ctx.Status(http.StatusInternalServerError).SendString("{}")
	}

	return ctx.Status(http.StatusOK).JSON(&getRes{IDs: res[payload.IndexName]})
}

func (entry *entrypoint) new(ctx *fiber.Ctx) error {
	payload := newReq{}

	if err := ctx.BodyParser(payload); err != nil {
		return ctx.Status(http.StatusBadRequest).SendString("{}")
	}

	_, err := vectoria.New(
		payload.Path,
		vectoria.WithIndexLSH(payload.LSHConfs...),
	)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).SendString("{}")
	}

	res, err := json.Marshal(newRes{})
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).SendString("{}")
	}

	return ctx.Status(http.StatusOK).Send(res)
}

func (entry *entrypoint) listen() error {
	entry.registerRoutes()

	return entry.app.Listen(entry.addr)
}

func main() {
	path := ""
	addr := "127.0.0.1:8558"

	entry, err := newEntrypoint(addr, path, true,
		vectoria.WithIndexLSH(&vectoria.LSHConfig{
			IndexName:      "demo",
			NumRounds:      10,
			NumHyperPlanes: 100,
			SpaceDim:       1500,
		}),
	)
	if err != nil {
		log.Println(err)
		return
	}

	if err := entry.listen(); err != nil {
		log.Println(err)
		return
	}
}

type newReq struct {
	Path     string                `json:"path"`
	LSHConfs []*vectoria.LSHConfig `json:"lsh_configs"`
}

type newRes struct{}
