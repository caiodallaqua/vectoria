package main

import (
	_ "embed"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/caiodallaqua/vectoria"
	"github.com/gofiber/fiber/v2"
	"github.com/steinfletcher/apitest"
	"github.com/stretchr/testify/assert"
)

//go:embed testdata/add_request.json
var addRequestBody string

//go:embed testdata/add_response.json
var addResponseBody string

//go:embed testdata/get_request.json
var getRequestBody string

//go:embed testdata/get_response.json
var getResponseBody string

func TestNewApp(t *testing.T) {
	want := struct {
		ServerHeader string
		AppName      string
		ReadTimeout  time.Duration
		WriteTimeout time.Duration
	}{
		ServerHeader: SERVER_HEADER,
		AppName:      APP_NAME,
		ReadTimeout:  READ_TIMEOUT,
		WriteTimeout: WRITE_TIMEOUT,
	}

	app := newApp(true)

	config := app.Config()

	assert.Equal(t, want.ServerHeader, config.ServerHeader)
	assert.Equal(t, want.AppName, config.AppName)
	assert.Equal(t, want.ReadTimeout, config.ReadTimeout)
	assert.Equal(t, want.WriteTimeout, config.WriteTimeout)
}

// Asserts that the given routes exist. If you remove an expected route, it'll break.
// It does not break when a new one is added since Fiber automatically adds verbs (such as HEAD) when a GET is defined.
func TestRegisterRoutes(t *testing.T) {
	expectedRoutes := []struct {
		Method string
		Path   string
	}{
		{
			Method: "GET",
			Path:   "/system/health",
		},
		{
			Method: "POST",
			Path:   "/add",
		},
	}

	logger := slog.New(slog.NewTextHandler(nil, nil))

	entry, err := newEntrypoint(logger, gofakeit.URL(), false, vectoria.DBConfig{})
	assert.NoError(t, err)

	entry.registerRoutes()

	routes := entry.app.GetRoutes(true)
	routesMap := make(map[string]bool)
	for _, route := range routes {
		routesMap[route.Method+" "+route.Path] = true
	}

	for _, want := range expectedRoutes {
		assert.Contains(t, routesMap, want.Method+" "+want.Path)
	}
}

func TestAdd(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(nil, nil))

	entry, err := newEntrypoint(logger, gofakeit.URL(), false,
		vectoria.DBConfig{
			Path: "",
			LSH: []vectoria.LSHConfig{{
				IndexName:      "demo",
				NumRounds:      10,
				NumHyperPlanes: 100,
				SpaceDim:       3,
			}},
		},
	)
	assert.NoError(t, err)

	entry.registerRoutes()

	apitest.New().
		HandlerFunc(FiberToHandlerFunc(entry.app)).
		Post("/add").
		Header("Content-Type", "application/json").
		Body(addRequestBody).
		Expect(t).
		Header("Content-Type", "application/json").
		Body(addResponseBody).
		Status(http.StatusOK).
		End()
}

func TestGet(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(nil, nil))

	entry, err := newEntrypoint(logger, gofakeit.URL(), false,
		vectoria.DBConfig{
			Path: "",
			LSH: []vectoria.LSHConfig{{
				IndexName:      "demo",
				NumRounds:      10,
				NumHyperPlanes: 100,
				SpaceDim:       3,
			}},
		},
	)
	assert.NoError(t, err)

	entry.registerRoutes()

	apitest.New().
		HandlerFunc(FiberToHandlerFunc(entry.app)).
		Post("/get").
		Header("Content-Type", "application/json").
		Body(getRequestBody).
		Expect(t).
		Header("Content-Type", "application/json").
		Body(getResponseBody).
		Status(http.StatusOK).
		End()
}

// ---------------------------- INSTRUMENTATION ----------------------------

func FiberToHandlerFunc(app *fiber.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp, err := app.Test(r)
		if err != nil {
			panic(err)
		}

		// copy headers
		for headerName, fullVal := range resp.Header {
			for _, headerVal := range fullVal {
				w.Header().Add(headerName, headerVal)
			}
		}

		w.WriteHeader(resp.StatusCode)

		if _, err := io.Copy(w, resp.Body); err != nil {
			panic(err)
		}
	}
}
