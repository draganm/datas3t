package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"

	"github.com/cucumber/godog"
	"github.com/draganm/datas3t/pkg/server/serverworld"
)

func TestMain(m *testing.M) {

	// Initialize cucumber test suite
	opts := godog.Options{
		Format:        "pretty",
		Paths:         []string{"features"},
		NoColors:      true,
		StopOnFailure: true,
		Strict:        true,
	}

	status := godog.TestSuite{
		Name:                "datas3t",
		ScenarioInitializer: InitializeScenario,
		Options:             &opts,
		TestSuiteInitializer: func(tsc *godog.TestSuiteContext) {
			tsc.ScenarioContext().Before(func(ctx context.Context, sc *godog.Scenario) (context.Context, error) {
				world, err := serverworld.New(ctx)
				if err != nil {
					return ctx, fmt.Errorf("failed to create world: %w", err)
				}
				return serverworld.ToContext(ctx, world), nil
			})

		},
	}.Run()

	// Run the standard Go tests if cucumber tests pass
	if status == 0 {
		os.Exit(m.Run())
	}

	os.Exit(status)
}

func InitializeScenario(ctx *godog.ScenarioContext) {
	ctx.Step(`^I send a PUT request to "([^"]*)"$`, iSendAPUTRequestTo)
	ctx.Step(`^the response status should be (\d+)$`, theResponseStatusShouldBe)
	ctx.Step(`^the dataset with the id "([^"]*)" should exist$`, theDatasetWithTheIdShouldExist)

}

func iSendAPUTRequestTo(ctx context.Context, path string) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	u, err := url.JoinPath(w.ServerURL, path)
	if err != nil {
		return fmt.Errorf("failed to join path: %w", err)
	}

	request, err := http.NewRequest(http.MethodPut, u, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	defer response.Body.Close()

	w.LastResponseStatus = response.StatusCode
	return nil
}

func theResponseStatusShouldBe(ctx context.Context, expected int) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	if w.LastResponseStatus != expected {
		return fmt.Errorf("expected status code %d, got %d", expected, w.LastResponseStatus)
	}
	return nil
}

func theDatasetWithTheIdShouldExist(ctx context.Context, id string) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	u, err := url.JoinPath(w.ServerURL, "api", "v1", "datas3t", id)
	if err != nil {
		return fmt.Errorf("failed to join path: %w", err)
	}

	request, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("expected status code %d, got %d", http.StatusOK, response.StatusCode)
	}

	return nil
}
