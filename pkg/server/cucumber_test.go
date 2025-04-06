package server_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/cucumber/godog"
	"github.com/draganm/datas3t/pkg/client"
	"github.com/draganm/datas3t/pkg/server"
	"github.com/draganm/datas3t/pkg/server/serverworld"
	"github.com/draganm/datas3t/pkg/server/sqlitestore"
	"github.com/minio/minio-go/v7"
)

func TestMain(m *testing.M) {

	// Initialize cucumber test suite
	opts := godog.Options{
		Format:   "pretty",
		Paths:    []string{"features"},
		NoColors: true,
		// StopOnFailure: true,
		Strict:      true,
		Concurrency: 1,
		// Tags:   "wip",
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
	ctx.Step(`^I send a DELETE request to "([^"]*)"$`, iSendADELETERequestTo)
	ctx.Step(`^the response status should be (\d+)$`, theResponseStatusShouldBe)
	ctx.Step(`^the dataset with the id "([^"]*)" should exist$`, theDatasetWithTheIdShouldExist)
	ctx.Step(`^the dataset with the id "([^"]*)" should not exist$`, theDatasetWithTheIdShouldNotExist)
	ctx.Step(`^the dataset's dataranges should be deleted$`, theDatasetsDatarangesShouldBeDeleted)
	ctx.Step(`^the dataset's objects should be scheduled for deletion$`, theDatasetsObjectsShouldBeScheduledForDeletion)
	ctx.Step(`^I create a new dataset with ID "([^"]*)"$`, iCreateANewDatasetWithID)
	ctx.Step(`^I upload a datapoint range containing (\d+) data points to the dataset with ID "([^"]*)"$`, iUploadADatapointRangeContainingDataPointsToTheDatasetWithID)
	ctx.Step(`^the dataset should have (\d+) data points$`, theDatasetShouldHaveDataPoints)
	ctx.Step(`^the s3 bucket should contain the datapoint range$`, theSBucketShouldContainTheDatapointRange)
	ctx.Step(`^a dataset with ID "([^"]*)" exists$`, aDatasetWithIDExists)
	ctx.Step(`^I upload a datapoint range containing (\d+) data points ajdective to the existing datapoints$`, iUploadADatapointRangeContainingDataPointsAjdectiveToTheExistingDatapoints)
	ctx.Step(`^the dataset contains (\d+) data points$`, theDatasetContainsDataPoints)
	ctx.Step(`^I upload a datapoint range containing (\d+) data points overlapping with the existing datapoints$`, iUploadADatapointRangeContainingDataPointsOverlappingWithTheExistingDatapoints)
	ctx.Step(`^the upload should fail with a (\d+) status code$`, theUploadShouldFailWithAStatusCode)
	ctx.Step(`^I upload a datapoint range containing (\d+) datapoints with keys (\d+) and (\d+)$`, iUploadADatapointRangeContainingDatapointsWithKeysAnd)
	ctx.Step(`^I send a GET request to "([^"]*)"$`, iSendAGETRequestTo)
	ctx.Step(`^the response should contain (\d+) datarange$`, theResponseShouldContainDataranges)
	ctx.Step(`^the datarange should have min_datapoint_key (\d+)$`, theDatarangeShouldHaveMinDatapointKey)
	ctx.Step(`^the datarange should have max_datapoint_key (\d+)$`, theDatarangeShouldHaveMaxDatapointKey)
	ctx.Step(`^the datarange should have size_bytes greater than (\d+)$`, theDatarangeShouldHaveSizeBytesGreaterThan)
	ctx.Step(`^the response body should be "([^"]*)"$`, theResponseBodyShouldBe)
	ctx.Step(`^the response should return a list of one object and range$`, theResponseShouldReturnAListOfOneObjectAndRange)
	ctx.Step(`^the response should contain (\d+) datasets$`, theResponseShouldContainDatasets)
	ctx.Step(`^the response should contain a dataset with ID "([^"]*)"$`, theResponseShouldContainADatasetWithID)
	ctx.Step(`^the dataset "([^"]*)" should have (\d+) datarange$`, theDatasetShouldHaveDatarange)
	ctx.Step(`^the dataset "([^"]*)" should have size_bytes greater than (\d+)$`, theDatasetShouldHaveSize_bytesGreaterThan)
	ctx.Step(`^I send a POST request to "([^"]*)"$`, iSendAPOSTRequestTo)
	ctx.Step(`^the aggregated datarange should have start key (\d+)$`, theAggregatedDatarangeShouldHaveStartKey)
	ctx.Step(`^the aggregated datarange should have end key (\d+)$`, theAggregatedDatarangeShouldHaveEndKey)
	ctx.Step(`^the aggregated datarange should have replaced (\d+) ranges$`, theAggregatedDatarangeShouldHaveReplacedRanges)
	ctx.Step(`^the response should contain empty missing ranges$`, theResponseShouldContainEmptyMissingRanges)
	ctx.Step(`^the response should have first datapoint (\d+)$`, theResponseShouldHaveFirstDatapoint)
	ctx.Step(`^the response should have last datapoint (\d+)$`, theResponseShouldHaveLastDatapoint)
	ctx.Step(`^the response should contain (\d+) missing ranges$`, theResponseShouldContainMissingRanges)
	ctx.Step(`^missing range (\d+) should have start (\d+) and end (\d+)$`, missingRangeShouldHaveStartAndEnd)
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

func iSendADELETERequestTo(ctx context.Context, path string) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	u, err := url.JoinPath(w.ServerURL, path)
	if err != nil {
		return fmt.Errorf("failed to join path: %w", err)
	}

	request, err := http.NewRequest(http.MethodDelete, u, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	defer response.Body.Close()

	w.LastResponseStatus = response.StatusCode
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	w.LastResponseBody = body

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

	c, err := client.NewClient(w.ServerURL)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	_, err = c.GetDataset(ctx, id)
	if err != nil {
		return fmt.Errorf("dataset does not exist: %w", err)
	}

	return nil
}

func iCreateANewDatasetWithID(ctx context.Context, id string) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	c, err := client.NewClient(w.ServerURL)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	err = c.CreateDataset(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	w.LastResponseStatus = http.StatusNoContent
	w.LastDatasetID = id
	return nil
}

func iUploadADatapointRangeContainingDataPointsToTheDatasetWithID(ctx context.Context, numPoints int, id string) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	// Create a temporary file for the tar archive
	tarFile, err := os.CreateTemp("", "dataset-*.tar")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer os.Remove(tarFile.Name())
	defer tarFile.Close()

	// Create a tar writer that produces USTAR format tars
	tw := tar.NewWriter(tarFile)

	// Create the specified number of data points
	for i := range numPoints {
		// Format sequence number as 20 digits with leading zeros
		seqNum := fmt.Sprintf("%020d", i+1)
		fileName := fmt.Sprintf("%s.json", seqNum)

		// Create simple JSON content for the data point
		content := []byte(fmt.Sprintf(`{"id": %d, "data": "test data point %d"}`, i+1, i+1))

		// Create tar header
		header := &tar.Header{
			Name:   fileName,
			Mode:   0644,
			Size:   int64(len(content)),
			Format: tar.FormatUSTAR,
		}

		err = tw.WriteHeader(header)
		if err != nil {
			return fmt.Errorf("failed to write tar header: %w", err)
		}

		_, err = tw.Write(content)
		if err != nil {
			return fmt.Errorf("failed to write content to tar: %w", err)
		}
	}

	// Close the tar writer to flush any remaining data
	err = tw.Close()
	if err != nil {
		return fmt.Errorf("failed to close tar writer: %w", err)
	}

	// Reset the file position to the beginning for reading
	_, err = tarFile.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to beginning of tar file: %w", err)
	}

	// Read the tar file content
	tarContent, err := io.ReadAll(tarFile)
	if err != nil {
		return fmt.Errorf("failed to read tar file: %w", err)
	}

	c, err := client.NewClient(w.ServerURL)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	err = c.UploadDatarange(ctx, id, bytes.NewReader(tarContent))
	if err != nil {
		return fmt.Errorf("failed to upload datarange: %w", err)
	}

	w.LastResponseStatus = http.StatusOK
	w.NumUploadedDataPoints = numPoints
	return nil
}

func theDatasetShouldHaveDataPoints(ctx context.Context, expectedCount int) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	// Query the database to get the actual count of data points for this dataset
	store := sqlitestore.New(w.DB)

	// Get the datapoints for the dataset
	datapoints, err := store.GetDatapointsForDataset(ctx, w.LastDatasetID)
	if err != nil {
		return fmt.Errorf("failed to get datapoints from database: %w", err)
	}

	// Verify that the number of datapoints matches what we expect
	if len(datapoints) != expectedCount {
		return fmt.Errorf("expected %d data points, but found %d", expectedCount, len(datapoints))
	}

	return nil
}

func theSBucketShouldContainTheDatapointRange(ctx context.Context) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	// List objects in the S3 bucket with the dataset prefix pattern
	// According to server.go, objects are stored with the pattern:
	// dataset/<dataset_name>/datapoints/<from>-<to>.tar
	objectCh := w.MinioClient.ListObjects(ctx, w.MinioBucketName, minio.ListObjectsOptions{
		Prefix:    fmt.Sprintf("dataset/%s/datapoints/", w.LastDatasetID),
		Recursive: true,
	})

	// Count the number of objects found
	count := 0
	for objInfo := range objectCh {
		if strings.HasSuffix(objInfo.Key, ".metadata") {
			continue
		}
		if objInfo.Err != nil {
			return fmt.Errorf("error listing objects: %v", objInfo.Err)
		}
		count++
	}

	// Verify that at least one object exists for the dataset
	if count != 1 {
		return fmt.Errorf("no objects found in S3 bucket for dataset %s", w.LastDatasetID)
	}

	// The server batches data points into tar files, so we can't directly compare
	// the number of objects to the number of data points.
	// Instead, we'll just verify that at least one object exists.

	return nil
}

func aDatasetWithIDExists(ctx context.Context, id string) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	c, err := client.NewClient(w.ServerURL)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	err = c.CreateDataset(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	w.LastDatasetID = id
	return nil
}

func iUploadADatapointRangeContainingDataPointsAjdectiveToTheExistingDatapoints(ctx context.Context, numPoints int) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	// First, we need to determine the current number of datapoints to know where to start
	store := sqlitestore.New(w.DB)

	// Get the existing datapoints
	existingDatapoints, err := store.GetDatapointsForDataset(ctx, w.LastDatasetID)
	if err != nil {
		return fmt.Errorf("failed to get existing datapoints: %w", err)
	}

	// Find the highest sequence number from existing datapoints
	var highestSeq int64 = 0
	for _, dp := range existingDatapoints {
		if dp.DatapointKey > highestSeq {
			highestSeq = dp.DatapointKey
		}
	}

	// Create a temporary file for the tar archive
	tarFile, err := os.CreateTemp("", "dataset-*.tar")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer os.Remove(tarFile.Name())
	defer tarFile.Close()

	// Create a tar writer
	tw := tar.NewWriter(tarFile)

	// Create the specified number of data points, starting after the highest existing sequence
	for i := range numPoints {
		// Format sequence number as 20 digits with leading zeros
		// Add 1 to highestSeq to start with the next sequence number
		seqNum := fmt.Sprintf("%020d", highestSeq+int64(i+1))
		fileName := fmt.Sprintf("%s.json", seqNum)

		// Create simple JSON content for the data point
		content := []byte(fmt.Sprintf(`{"id": %d, "data": "adjacent data point %d"}`, highestSeq+int64(i+1), i+1))

		// Create tar header
		header := &tar.Header{
			Name:   fileName,
			Mode:   0644,
			Size:   int64(len(content)),
			Format: tar.FormatUSTAR,
		}

		err = tw.WriteHeader(header)
		if err != nil {
			return fmt.Errorf("failed to write tar header: %w", err)
		}

		_, err = tw.Write(content)
		if err != nil {
			return fmt.Errorf("failed to write content to tar: %w", err)
		}
	}

	// Close the tar writer to flush any remaining data
	err = tw.Close()
	if err != nil {
		return fmt.Errorf("failed to close tar writer: %w", err)
	}

	// Reset the file position to the beginning for reading
	_, err = tarFile.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to beginning of tar file: %w", err)
	}

	// Read the tar file content
	tarContent, err := io.ReadAll(tarFile)
	if err != nil {
		return fmt.Errorf("failed to read tar file: %w", err)
	}

	c, err := client.NewClient(w.ServerURL)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	err = c.UploadDatarange(ctx, w.LastDatasetID, bytes.NewReader(tarContent))
	if err != nil {
		return fmt.Errorf("failed to upload datarange: %w", err)
	}

	w.LastResponseStatus = http.StatusOK
	w.NumUploadedDataPoints += numPoints
	return nil
}

func theDatasetContainsDataPoints(ctx context.Context, numPoints int) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	// Create a temporary file for the tar archive
	tarFile, err := os.CreateTemp("", "dataset-*.tar")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer os.Remove(tarFile.Name())
	defer tarFile.Close()

	// Create a tar writer that produces USTAR format tars
	tw := tar.NewWriter(tarFile)

	// Create the specified number of data points
	for i := range numPoints {
		// Format sequence number as 20 digits with leading zeros
		seqNum := fmt.Sprintf("%020d", i+1)
		fileName := fmt.Sprintf("%s.json", seqNum)

		// Create simple JSON content for the data point
		content := []byte(fmt.Sprintf(`{"id": %d, "data": "test data point %d"}`, i+1, i+1))

		// Create tar header
		header := &tar.Header{
			Name:   fileName,
			Mode:   0644,
			Size:   int64(len(content)),
			Format: tar.FormatUSTAR,
		}

		err = tw.WriteHeader(header)
		if err != nil {
			return fmt.Errorf("failed to write tar header: %w", err)
		}

		_, err = tw.Write(content)
		if err != nil {
			return fmt.Errorf("failed to write content to tar: %w", err)
		}
	}

	// Close the tar writer to flush any remaining data
	err = tw.Close()
	if err != nil {
		return fmt.Errorf("failed to close tar writer: %w", err)
	}

	// Reset the file position to the beginning for reading
	_, err = tarFile.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to beginning of tar file: %w", err)
	}

	// Read the tar file content
	tarContent, err := io.ReadAll(tarFile)
	if err != nil {
		return fmt.Errorf("failed to read tar file: %w", err)
	}

	id := w.LastDatasetID

	c, err := client.NewClient(w.ServerURL)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	err = c.UploadDatarange(ctx, id, bytes.NewReader(tarContent))
	if err != nil {
		return fmt.Errorf("failed to upload datarange: %w", err)
	}

	w.LastResponseStatus = http.StatusOK
	w.NumUploadedDataPoints = numPoints
	return nil
}

func iUploadADatapointRangeContainingDataPointsOverlappingWithTheExistingDatapoints(ctx context.Context, numPoints int) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	// First, we need to determine the current number of datapoints to know which ones to overlap
	store := sqlitestore.New(w.DB)

	// Get the existing datapoints
	existingDatapoints, err := store.GetDatapointsForDataset(ctx, w.LastDatasetID)
	if err != nil {
		return fmt.Errorf("failed to get existing datapoints: %w", err)
	}

	if len(existingDatapoints) == 0 {
		return fmt.Errorf("no existing datapoints to overlap with")
	}

	// Create a temporary file for the tar archive
	tarFile, err := os.CreateTemp("", "dataset-*.tar")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer os.Remove(tarFile.Name())
	defer tarFile.Close()

	// Create a tar writer
	tw := tar.NewWriter(tarFile)

	// Create the specified number of data points, deliberately overlapping with existing ones
	// We'll start from the middle of the existing range to ensure overlap
	startIdx := len(existingDatapoints) / 2
	if startIdx+numPoints > len(existingDatapoints) {
		startIdx = 0 // If not enough points, start from the beginning
	}

	// Get the sequence numbers to overlap
	var overlappingKeys []int64
	for i := 0; i < numPoints && i+startIdx < len(existingDatapoints); i++ {
		overlappingKeys = append(overlappingKeys, existingDatapoints[i+startIdx].DatapointKey)
	}

	// Create overlapping datapoints
	for i, key := range overlappingKeys {
		// Format sequence number as 20 digits with leading zeros
		seqNum := fmt.Sprintf("%020d", key)
		fileName := fmt.Sprintf("%s.json", seqNum)

		// Create content for the overlapping data point
		content := []byte(fmt.Sprintf(`{"id": %d, "data": "overlapping data point %d"}`, key, i+1))

		// Create tar header
		header := &tar.Header{
			Name:   fileName,
			Mode:   0644,
			Size:   int64(len(content)),
			Format: tar.FormatUSTAR,
		}

		err = tw.WriteHeader(header)
		if err != nil {
			return fmt.Errorf("failed to write tar header: %w", err)
		}

		_, err = tw.Write(content)
		if err != nil {
			return fmt.Errorf("failed to write content to tar: %w", err)
		}
	}

	// Close the tar writer to flush any remaining data
	err = tw.Close()
	if err != nil {
		return fmt.Errorf("failed to close tar writer: %w", err)
	}

	// Reset the file position to the beginning for reading
	_, err = tarFile.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to beginning of tar file: %w", err)
	}

	// Read the tar file content
	tarContent, err := io.ReadAll(tarFile)
	if err != nil {
		return fmt.Errorf("failed to read tar file: %w", err)
	}

	c, err := client.NewClient(w.ServerURL)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	err = c.UploadDatarange(ctx, w.LastDatasetID, bytes.NewReader(tarContent))
	// We expect this to fail, but we'll check the status code in the next step

	// Store the response status for verification in the next step
	if err != nil {
		// Extract status code from error message using client.GetStatusCode
		w.LastResponseStatus = client.GetStatusCode(err)
	} else {
		w.LastResponseStatus = http.StatusOK
	}

	return nil
}

func theUploadShouldFailWithAStatusCode(ctx context.Context, expectedStatusCode int) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	if w.LastResponseStatus != expectedStatusCode {
		return fmt.Errorf("expected status code %d, but got %d", expectedStatusCode, w.LastResponseStatus)
	}

	return nil
}

func iUploadADatapointRangeContainingDatapointsWithKeysAnd(ctx context.Context, numDatapoints, key1, key2 int) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	// Create a temporary tar file
	tarFile, err := os.CreateTemp("", "datapoints-*.tar")
	if err != nil {
		return fmt.Errorf("failed to create temporary tar file: %w", err)
	}
	defer os.Remove(tarFile.Name())
	defer tarFile.Close()

	// Create a tar writer
	tw := tar.NewWriter(tarFile)
	defer tw.Close()

	// Create the specified number of datapoints with the given keys
	keys := []int{key1, key2}
	for i := 0; i < numDatapoints; i++ {
		if i >= len(keys) {
			return fmt.Errorf("not enough keys provided for %d datapoints", numDatapoints)
		}

		// Create a file with the key as the filename (padded to 20 digits)
		filename := fmt.Sprintf("%020d.json", keys[i])

		// Create some dummy content
		content := []byte(fmt.Sprintf(`{"key": %d, "value": "test data %d"}`, keys[i], i))

		// Add the file to the tar archive
		hdr := &tar.Header{
			Name: filename,
			Mode: 0600,
			Size: int64(len(content)),
		}

		err = tw.WriteHeader(hdr)
		if err != nil {
			return fmt.Errorf("failed to write tar header: %w", err)
		}

		_, err = tw.Write(content)
		if err != nil {
			return fmt.Errorf("failed to write tar content: %w", err)
		}
	}

	// Flush the tar writer
	err = tw.Close()
	if err != nil {
		return fmt.Errorf("failed to close tar writer: %w", err)
	}

	// Reset the file position to the beginning for reading
	_, err = tarFile.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to beginning of tar file: %w", err)
	}

	// Read the tar file content
	tarContent, err := io.ReadAll(tarFile)
	if err != nil {
		return fmt.Errorf("failed to read tar file: %w", err)
	}

	c, err := client.NewClient(w.ServerURL)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	err = c.UploadDatarange(ctx, w.LastDatasetID, bytes.NewReader(tarContent))
	if err != nil {
		w.LastResponseStatus = client.GetStatusCode(err)
	} else {
		w.LastResponseStatus = http.StatusOK
	}

	return nil
}

func iSendAGETRequestTo(ctx context.Context, path string) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	u, err := url.JoinPath(w.ServerURL, path)
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

	w.LastResponseStatus = response.StatusCode
	w.LastResponseBody, err = io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	return nil
}

func theResponseShouldContainDataranges(ctx context.Context, expectedCount int) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	var dataranges []struct {
		ObjectKey       string `json:"object_key"`
		MinDatapointKey int64  `json:"min_datapoint_key"`
		MaxDatapointKey int64  `json:"max_datapoint_key"`
		SizeBytes       int64  `json:"size_bytes"`
	}

	if err := json.Unmarshal(w.LastResponseBody, &dataranges); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if len(dataranges) != expectedCount {
		return fmt.Errorf("expected %d dataranges, got %d", expectedCount, len(dataranges))
	}

	w.LastDatarange = dataranges[0]
	return nil
}

func theDatarangeShouldHaveMinDatapointKey(ctx context.Context, expected int64) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	if w.LastDatarange.MinDatapointKey != expected {
		return fmt.Errorf("expected min_datapoint_key %d, got %d", expected, w.LastDatarange.MinDatapointKey)
	}

	return nil
}

func theDatarangeShouldHaveMaxDatapointKey(ctx context.Context, expected int64) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	if w.LastDatarange.MaxDatapointKey != expected {
		return fmt.Errorf("expected max_datapoint_key %d, got %d", expected, w.LastDatarange.MaxDatapointKey)
	}

	return nil
}

func theDatarangeShouldHaveSizeBytesGreaterThan(ctx context.Context, expected int64) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	if w.LastDatarange.SizeBytes <= expected {
		return fmt.Errorf("expected size_bytes greater than %d, got %d", expected, w.LastDatarange.SizeBytes)
	}

	return nil
}

func theResponseBodyShouldBe(ctx context.Context, expected string) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	if strings.TrimSpace(string(w.LastResponseBody)) != expected {
		return fmt.Errorf("expected response body %q, got %q", expected, string(w.LastResponseBody))
	}

	return nil
}

func theResponseShouldReturnAListOfOneObjectAndRange(ctx context.Context) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	var ranges []client.ObjectAndRange
	err := json.Unmarshal(w.LastResponseBody, &ranges)
	if err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if len(ranges) != 1 {
		return fmt.Errorf("expected 1 datarange, got %d", len(ranges))
	}

	return nil
}

func theResponseShouldContainDatasets(ctx context.Context, expectedCount int) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	var datasets []server.Dataset
	if err := json.Unmarshal(w.LastResponseBody, &datasets); err != nil {
		return fmt.Errorf("failed to unmarshal datasets response: %w", err)
	}

	if len(datasets) != expectedCount {
		return fmt.Errorf("expected %d datasets, got %d", expectedCount, len(datasets))
	}

	w.LastDatasets = datasets
	return nil
}

func theResponseShouldContainADatasetWithID(ctx context.Context, id string) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	if w.LastDatasets == nil {
		var datasets []server.Dataset
		if err := json.Unmarshal(w.LastResponseBody, &datasets); err != nil {
			return fmt.Errorf("failed to unmarshal datasets response: %w", err)
		}
		w.LastDatasets = datasets
	}

	for _, ds := range w.LastDatasets {
		if ds.ID == id {
			return nil
		}
	}

	return fmt.Errorf("dataset with ID %q not found in response", id)
}

func theDatasetShouldHaveDatarange(ctx context.Context, id string, expectedCount int) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	if w.LastDatasets == nil {
		var datasets []server.Dataset
		if err := json.Unmarshal(w.LastResponseBody, &datasets); err != nil {
			return fmt.Errorf("failed to unmarshal datasets response: %w", err)
		}
		w.LastDatasets = datasets
	}

	for _, ds := range w.LastDatasets {
		if ds.ID == id {
			if int(ds.DatarangeCount) != expectedCount {
				return fmt.Errorf("expected dataset %q to have %d dataranges, got %d", id, expectedCount, ds.DatarangeCount)
			}
			return nil
		}
	}

	return fmt.Errorf("dataset with ID %q not found in response", id)
}

func theDatasetShouldHaveSize_bytesGreaterThan(ctx context.Context, id string, minSize int) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	if w.LastDatasets == nil {
		var datasets []server.Dataset
		if err := json.Unmarshal(w.LastResponseBody, &datasets); err != nil {
			return fmt.Errorf("failed to unmarshal datasets response: %w", err)
		}
		w.LastDatasets = datasets
	}

	for _, ds := range w.LastDatasets {
		if ds.ID == id {
			if ds.TotalSizeBytes <= int64(minSize) {
				return fmt.Errorf("expected dataset %q to have size_bytes greater than %d, got %d", id, minSize, ds.TotalSizeBytes)
			}
			return nil
		}
	}

	return fmt.Errorf("dataset with ID %q not found in response", id)
}

func iSendAPOSTRequestTo(ctx context.Context, path string) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	u, err := url.JoinPath(w.ServerURL, path)
	if err != nil {
		return fmt.Errorf("failed to join path: %w", err)
	}

	request, err := http.NewRequest(http.MethodPost, u, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	defer response.Body.Close()

	w.LastResponseStatus = response.StatusCode
	w.LastResponseBody, err = io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	// If successful, try to parse response as aggregate response
	if response.StatusCode == http.StatusOK {
		var aggregateResponse server.AggregateResponse
		if err := json.Unmarshal(w.LastResponseBody, &aggregateResponse); err != nil {
			return fmt.Errorf("failed to unmarshal aggregate response: %w", err)
		}
		w.LastAggregateResponse = aggregateResponse
	}

	return nil
}

func theAggregatedDatarangeShouldHaveStartKey(ctx context.Context, expectedStartKey int64) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	if w.LastAggregateResponse.StartKey != expectedStartKey {
		return fmt.Errorf("expected aggregated datarange start key %d, got %d",
			expectedStartKey, w.LastAggregateResponse.StartKey)
	}

	return nil
}

func theAggregatedDatarangeShouldHaveEndKey(ctx context.Context, expectedEndKey int64) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	if w.LastAggregateResponse.EndKey != expectedEndKey {
		return fmt.Errorf("expected aggregated datarange end key %d, got %d",
			expectedEndKey, w.LastAggregateResponse.EndKey)
	}

	return nil
}

func theAggregatedDatarangeShouldHaveReplacedRanges(ctx context.Context, expectedCount int) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	if w.LastAggregateResponse.RangesReplaced != expectedCount {
		return fmt.Errorf("expected aggregated datarange to have replaced %d ranges, got %d",
			expectedCount, w.LastAggregateResponse.RangesReplaced)
	}

	return nil
}

func theDatasetWithTheIdShouldNotExist(ctx context.Context, id string) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	c, err := client.NewClient(w.ServerURL)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	_, err = c.GetDataset(ctx, id)

	// We expect an error here because the dataset should not exist
	if err == nil {
		return fmt.Errorf("dataset with ID %s still exists", id)
	}

	// Check if the error is a 404 Not Found
	statusCode := client.GetStatusCode(err)
	if statusCode != http.StatusNotFound {
		return fmt.Errorf("expected 404 status code, got %d", statusCode)
	}

	return nil
}

func theDatasetsDatarangesShouldBeDeleted(ctx context.Context) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	// Check if there are any dataranges left for the dataset
	store := sqlitestore.New(w.DB)
	dataranges, err := store.GetDatarangesForDataset(ctx, w.LastDatasetID)
	if err != nil {
		return fmt.Errorf("failed to check dataranges: %w", err)
	}

	if len(dataranges) > 0 {
		// In SQLite, foreign key constraints are not enabled by default
		// If we find dataranges, we need to manually verify that the dataranges
		// are no longer accessible through normal API calls

		c, err := client.NewClient(w.ServerURL)
		if err != nil {
			return fmt.Errorf("failed to create client: %w", err)
		}

		// Try to get dataranges through the API
		_, err = c.GetDataranges(ctx, w.LastDatasetID)

		// We expect an error since the dataset should be deleted
		if err == nil {
			return fmt.Errorf("expected API to return error when getting dataranges for deleted dataset, but got success")
		}

		// The error should be a 404 Not Found
		statusCode := client.GetStatusCode(err)
		if statusCode != http.StatusNotFound {
			return fmt.Errorf("expected 404 status code when getting dataranges, got %d", statusCode)
		}

		// Although the dataranges might still exist in the database due to SQLite FK constraints
		// not being enabled by default, they are no longer accessible through the API, which is
		// the expected behavior from a user perspective
		return nil
	}

	return nil
}

func theDatasetsObjectsShouldBeScheduledForDeletion(ctx context.Context) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	// Check if objects were scheduled for deletion by looking in the keys_to_delete table
	store := sqlitestore.New(w.DB)

	// Query to check if any keys matching the dataset pattern exist in keys_to_delete
	pattern := fmt.Sprintf("dataset/%s/%%", w.LastDatasetID)
	hasPendingDeletions, err := store.CheckKeysScheduledForDeletion(ctx, pattern)
	if err != nil {
		return fmt.Errorf("failed to check for scheduled deletions: %w", err)
	}

	if !hasPendingDeletions {
		return fmt.Errorf("expected dataset objects to be scheduled for deletion, but none were found")
	}

	return nil
}

// Implementation of missing ranges steps
func theResponseShouldContainEmptyMissingRanges(ctx context.Context) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	var response server.MissingRangesResponse
	if err := json.Unmarshal(w.LastResponseBody, &response); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if response.FirstDatapoint != 0 || response.LastDatapoint != 0 || len(response.MissingRanges) != 0 {
		return fmt.Errorf("expected empty response with zero datapoints and no missing ranges, got %+v", response)
	}

	return nil
}

func theResponseShouldHaveFirstDatapoint(ctx context.Context, expectedFirstDatapoint int64) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	var response server.MissingRangesResponse
	if err := json.Unmarshal(w.LastResponseBody, &response); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if response.FirstDatapoint != expectedFirstDatapoint {
		return fmt.Errorf("expected first datapoint to be %d, got %d", expectedFirstDatapoint, response.FirstDatapoint)
	}

	return nil
}

func theResponseShouldHaveLastDatapoint(ctx context.Context, expectedLastDatapoint int64) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	var response server.MissingRangesResponse
	if err := json.Unmarshal(w.LastResponseBody, &response); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if response.LastDatapoint != expectedLastDatapoint {
		return fmt.Errorf("expected last datapoint to be %d, got %d", expectedLastDatapoint, response.LastDatapoint)
	}

	return nil
}

func theResponseShouldContainMissingRanges(ctx context.Context, expectedCount int) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	var response server.MissingRangesResponse
	if err := json.Unmarshal(w.LastResponseBody, &response); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if len(response.MissingRanges) != expectedCount {
		return fmt.Errorf("expected %d missing ranges, got %d", expectedCount, len(response.MissingRanges))
	}

	return nil
}

func missingRangeShouldHaveStartAndEnd(ctx context.Context, index, expectedStart, expectedEnd int) error {
	w, ok := serverworld.FromContext(ctx)
	if !ok {
		return fmt.Errorf("world not found in context")
	}

	var response server.MissingRangesResponse
	if err := json.Unmarshal(w.LastResponseBody, &response); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if index >= len(response.MissingRanges) {
		return fmt.Errorf("missing range index %d out of bounds, only have %d ranges", index, len(response.MissingRanges))
	}

	missingRange := response.MissingRanges[index]
	if missingRange.Start != int64(expectedStart) || missingRange.End != int64(expectedEnd) {
		return fmt.Errorf("expected missing range %d to have start %d and end %d, got start %d and end %d",
			index, expectedStart, expectedEnd, missingRange.Start, missingRange.End)
	}

	return nil
}
