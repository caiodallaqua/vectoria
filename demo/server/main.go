package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/mastrasec/vectoria"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

const (
	SERVER_HEADER string        = "vectoria-server"
	APP_NAME      string        = "Vectoria"
	READ_TIMEOUT  time.Duration = 10 * time.Second
	WRITE_TIMEOUT time.Duration = 10 * time.Second
)

type entrypoint struct {
	addr   string
	path   string
	app    *fiber.App
	db     *vectoria.DB
	logger *slog.Logger
}

type addReq struct {
	IndexName string    `json:"index_name"`
	ItemID    string    `json:"item_id"`
	ItemVec   []float64 `json:"item_vec"`
}

type addRes struct{}

type getReq struct {
	IndexName string    `json:"index_name"`
	Query     []float64 `json:"query"`
	Threshold float64   `json:"threshold"`
	K         uint32    `json:"k"`
}

type getRes struct {
	IDs []string `json:"ids"`
}

type Captions struct {
	URL       string    `json:"Url"`
	Embedding []float64 `json:"Embedding"`
	// Description string    `json:"Description"`
	// We may add description later on in order to clean up captions with bad descriptions
}

type newReq struct {
	Path     string                `json:"path"`
	LSHConfs []*vectoria.LSHConfig `json:"lsh_configs"`
}

type newRes struct{}

func newEntrypoint(logger *slog.Logger, addr, path string, shouldLogDB bool, opts ...vectoria.Options) (*entrypoint, error) {
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	db, err := vectoria.New(path, opts...)
	if err != nil {
		logger.Error("unable to create database", "function", "newEntrypoint", "error", err)
		return nil, err
	}

	return &entrypoint{
		addr:   addr,
		path:   path,
		app:    newApp(shouldLogDB),
		db:     db,
		logger: logger,
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
	app.Use(cors.New())

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
		Post("/get", entry.get)
}

func (entry *entrypoint) health(ctx *fiber.Ctx) error {
	// TODO
	return nil
}

func (entry *entrypoint) add(ctx *fiber.Ctx) error {
	logDebug := entry.logger.With("function", "add")
	payload := &addReq{}

	if err := ctx.BodyParser(payload); err != nil {
		logDebug.Error("unable to parse payload data", "error", err.Error())
		return ctx.Status(http.StatusBadRequest).SendString("{}")
	}

	err := entry.db.Add(payload.ItemID, payload.ItemVec, payload.IndexName)
	if err != nil {
		logDebug.Error("unable to add data to database", "error", err.Error())
		return ctx.Status(http.StatusInternalServerError).SendString("{}")
	}

	return ctx.Status(http.StatusOK).JSON(&addRes{})
}

func (entry *entrypoint) get(ctx *fiber.Ctx) error {
	logDebug := entry.logger.With("function", "get")
	payload := &getReq{}

	if err := ctx.BodyParser(payload); err != nil {
		logDebug.Error("unable to parse payload data", "error", err.Error())
		return ctx.Status(http.StatusBadRequest).SendString("{}")
	}

	res, err := entry.db.Get(payload.Query, payload.Threshold, payload.K, payload.IndexName)
	if err != nil {
		logDebug.Error("unable to get data from database", "error", err.Error())
		return ctx.Status(http.StatusInternalServerError).SendString("{}")
	}

	return ctx.Status(http.StatusOK).JSON(&getRes{IDs: res[payload.IndexName]})
}

func (entry *entrypoint) new(ctx *fiber.Ctx) error {
	logDebug := entry.logger.With("function", "new")

	payload := &newReq{}

	if err := ctx.BodyParser(payload); err != nil {
		logDebug.Error("unable to parse payload data", "error", err.Error())
		return ctx.Status(http.StatusBadRequest).SendString("{}")
	}

	_, err := vectoria.New(
		payload.Path,
		vectoria.WithIndexLSH(payload.LSHConfs...),
	)
	if err != nil {
		logDebug.Error("unable to create database instance", "error", err.Error())
		return ctx.Status(http.StatusInternalServerError).SendString("{}")
	}

	res, err := json.Marshal(newRes{})
	if err != nil {
		logDebug.Error("unable to marshal response", "error", err.Error())
		return ctx.Status(http.StatusInternalServerError).SendString("{}")
	}

	return ctx.Status(http.StatusOK).Send(res)
}

func (entry *entrypoint) writeToDB(data []Captions) error {
	for _, caption := range data {
		if err := entry.db.Add(caption.URL, caption.Embedding); err != nil {
			entry.logger.Error("unable to wtite item to database", "function", "writeToDB", "error", err.Error())
			return err
		}
	}

	return nil
}

func (entry *entrypoint) listen() error {
	entry.registerRoutes()

	return entry.app.Listen(entry.addr)
}

func (entry *entrypoint) getDataset(url string) (dest string, err error) {
	logDebug := entry.logger.With("function", "getDataset")

	tokens := strings.Split(url, "/")
	filename := tokens[len(tokens)-1]
	destPath := filename

	resp, err := http.Get(url)
	if err != nil {
		logDebug.Error("unable to make get request", "error", err.Error())
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("bad status code: %d", resp.StatusCode)
		logDebug.Error("expected status ok", "error", err.Error())
		return "", err
	}

	outFile, err := os.Create(destPath)
	if err != nil {
		logDebug.Error("unable to create destination file", "error", err.Error())
		return "", err
	}
	defer outFile.Close()

	if _, err = io.Copy(outFile, resp.Body); err != nil {
		logDebug.Error("unable to copy the file", "error", err.Error())
		return "", err
	}

	return destPath, nil
}

func (entry *entrypoint) readParquet(path string) ([]byte, error) {
	var err error
	logDebug := entry.logger.With("function", "readParquet")

	fileReader, err := local.NewLocalFileReader(path)
	if err != nil {
		logDebug.Error("unable to create local file reader", "error", err.Error())
		return nil, err
	}
	defer fileReader.Close()

	parquetReader, err := reader.NewParquetReader(fileReader, nil, 4)
	if err != nil {
		logDebug.Error("unable to create parquet reader", "error", err.Error())
		return nil, err
	}
	defer parquetReader.ReadStop()

	rawData, err := parquetReader.ReadByNumber(int(parquetReader.GetNumRows()))
	if err != nil {
		logDebug.Error("unable to read raw data", "error", err.Error())
		return nil, err
	}

	data, err := json.Marshal(rawData)
	if err != nil {
		logDebug.Error("unable to marshal data to json", "error", err.Error())
		return nil, err
	}

	return data, nil
}

func (entry *entrypoint) getData(remoteDatasetPath string) ([]Captions, error) {
	logDebug := entry.logger.With("function", "getData")

	entry.logger.Info("downloading dataset")
	datasetPath, err := entry.getDataset(remoteDatasetPath)
	if err != nil {
		logDebug.Error("unable to get dataset", "error", err.Error())
		return nil, err
	}
	entry.logger.Info("finished downloading dataset")

	entry.logger.Info("reading parquet")
	data, err := entry.readParquet(datasetPath)
	if err != nil {
		logDebug.Error("unable to read parquet", "error", err.Error())
		return nil, err
	}
	entry.logger.Info("finished reading parquet")

	captions := []Captions{}
	entry.logger.Info("unmarshalling data to struct")
	if err := json.Unmarshal(data, &captions); err != nil {
		logDebug.Error("unable to unmarshal data", "error", err.Error())
		return nil, err
	}
	entry.logger.Info("finished unmarshalling data to struct")

	return captions, err
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	logDebug := logger.With("function", "main")
	slog.SetDefault(logger)

	var (
		path              string = ""
		addr              string = "127.0.0.1:8558"
		remoteDatasetPath string = "https://github.com/mastrasec/vectoria/releases/download/demo_dataset_v0/sbu_captions_embeddings.parquet"
		embeddingLen      uint32 = 384
	)

	log.Println(path)

	logger.Info("launching vector database")
	entry, err := newEntrypoint(logger, addr, path, true,
		vectoria.WithIndexLSH(&vectoria.LSHConfig{
			IndexName:      "demo",
			NumRounds:      50,
			NumHyperPlanes: 20,
			SpaceDim:       embeddingLen,
		}),
	)
	if err != nil {
		logDebug.Error("unable to create entrypoint", "error", err.Error())
		return
	}
	logger.Info("vector database is up")

	data, err := entry.getData(remoteDatasetPath)
	if err != nil {
		logDebug.Error("unable to get data", "error", err.Error())
		return
	}
	runtime.GC()

	logger.Info("writing to database")
	if err := entry.writeToDB(data); err != nil {
		logDebug.Error("unable to write to database", "error", err.Error())
		return
	}
	logger.Info("finished writing to database")
	runtime.GC()

	if err := entry.listen(); err != nil {
		logDebug.Error("unable to listen to server", "error", err.Error())
		return
	}
}
