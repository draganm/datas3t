package datas3t_test

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/draganm/datas3t"
	"github.com/draganm/datas3t/tarindex"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	miniogo "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	tc_postgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Helper functions for HTTP requests (kept for potential future use)
func httpPost(url string, body interface{}) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reqBody = bytes.NewReader(jsonData)
	}

	req, err := http.NewRequest("POST", url, reqBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	return client.Do(req)
}

func httpPut(url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	return client.Do(req)
}

// createTestTarWithIndex creates a TAR archive with correctly named files and returns both the tar data and index
func createTestTarWithIndex(numFiles int, startIndex int64) ([]byte, []byte) {
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	// Create files with proper %020d.<extension> naming
	// Target 20MB total per datarange, calculate file size accordingly
	const targetDatarangeSize = 20 * 1024 * 1024 // 20MB per datarange
	fileSize := targetDatarangeSize / numFiles   // Dynamic file size based on number of files

	// Ensure minimum file size for meaningful content
	if fileSize < 100 {
		fileSize = 100
	}

	for i := 0; i < numFiles; i++ {
		filename := fmt.Sprintf("%020d.txt", startIndex+int64(i))

		// Create content sized appropriately for the target
		baseContent := fmt.Sprintf("Content of file %d - ", startIndex+int64(i))

		// For small files, create simpler content
		var content string
		if fileSize < 1000 {
			// Small files: simple repeated pattern
			repetitions := fileSize / len(baseContent)
			if repetitions < 1 {
				repetitions = 1
			}
			content = strings.Repeat(baseContent, repetitions)
			// Pad to exact size
			if len(content) < fileSize {
				content += strings.Repeat("X", fileSize-len(content))
			} else if len(content) > fileSize {
				content = content[:fileSize]
			}
		} else {
			// Larger files: more complex content (original logic)
			repetitions := fileSize / len(baseContent)

			var contentBuilder strings.Builder
			contentBuilder.Grow(fileSize) // Pre-allocate space

			for j := 0; j < repetitions; j++ {
				contentBuilder.WriteString(baseContent)
				// Add some variation to make content unique and compressible
				contentBuilder.WriteString(fmt.Sprintf("line %d of %d, ", j+1, repetitions))
			}

			// Fill remaining space to reach exact target size
			content = contentBuilder.String()
			if len(content) < fileSize {
				padding := strings.Repeat("X", fileSize-len(content))
				content += padding
			} else if len(content) > fileSize {
				content = content[:fileSize]
			}
		}

		header := &tar.Header{
			Name: filename,
			Size: int64(len(content)),
			Mode: 0644,
		}

		err := tw.WriteHeader(header)
		if err != nil {
			panic(fmt.Sprintf("Failed to write tar header: %v", err))
		}

		_, err = tw.Write([]byte(content))
		if err != nil {
			panic(fmt.Sprintf("Failed to write tar content: %v", err))
		}
	}

	err := tw.Close()
	if err != nil {
		panic(fmt.Sprintf("Failed to close tar writer: %v", err))
	}

	// Create the tar index
	tarReader := bytes.NewReader(tarBuf.Bytes())
	indexData, err := tarindex.IndexTar(tarReader)
	if err != nil {
		panic(fmt.Sprintf("Failed to create tar index: %v", err))
	}

	return tarBuf.Bytes(), indexData
}

// validateTarSegment checks if the downloaded segment data contains valid tar entries
// This is used for partial tar data (segments) that don't need to have complete tar structure
func validateTarSegment(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("tar segment is empty")
	}

	// Try to parse the tar segment - segments may not have proper tar endings
	reader := tar.NewReader(bytes.NewReader(data))
	fileCount := 0

	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading tar header: %w", err)
		}

		fileCount++

		// Validate header fields
		if header.Name == "" {
			return fmt.Errorf("file %d has empty name", fileCount)
		}

		// Validate file size is reasonable
		if header.Size < 0 {
			return fmt.Errorf("file %s has negative size: %d", header.Name, header.Size)
		}

		// Validate mode is reasonable (not zero or invalid)
		if header.Mode == 0 {
			return fmt.Errorf("file %s has zero mode", header.Name)
		}

		// Validate filename format matches expected pattern (20-digit zero-padded number)
		if !strings.HasSuffix(header.Name, ".txt") {
			return fmt.Errorf("file %s does not have .txt extension", header.Name)
		}

		baseName := strings.TrimSuffix(header.Name, ".txt")
		if len(baseName) != 20 {
			return fmt.Errorf("file %s does not have 20-character base name: got %d characters", header.Name, len(baseName))
		}

		// Check if name contains only digits
		for _, c := range baseName {
			if c < '0' || c > '9' {
				return fmt.Errorf("file %s contains non-digit characters in base name", header.Name)
			}
		}

		// Read and validate file content
		content, err := io.ReadAll(reader)
		if err != nil {
			return fmt.Errorf("error reading file %s content: %w", header.Name, err)
		}

		// Verify actual content size matches header
		if int64(len(content)) != header.Size {
			return fmt.Errorf("file %s content size mismatch: header says %d, actual %d",
				header.Name, header.Size, len(content))
		}

		// Validate content matches expected pattern
		fileNum := strings.TrimLeft(baseName, "0")
		if fileNum == "" {
			fileNum = "0" // Handle case where filename is all zeros
		}
		expectedContentPrefix := fmt.Sprintf("Content of file %s - ", fileNum)
		if !strings.HasPrefix(string(content), expectedContentPrefix) {
			return fmt.Errorf("file %s has unexpected content prefix: got %q, expected prefix %q",
				header.Name, string(content[:min(50, len(content))]), expectedContentPrefix)
		}

		// Validate that content is valid UTF-8 text
		if !isValidUTF8(content) {
			return fmt.Errorf("file %s contains invalid UTF-8 content", header.Name)
		}
	}

	if fileCount == 0 {
		return fmt.Errorf("tar segment contains no files")
	}

	return nil
}

// validateTarArchive checks if the downloaded data is a valid tar archive with comprehensive format validation
func validateTarArchive(data []byte) error {
	if len(data) < 1024 {
		return fmt.Errorf("data too short to be a valid tar archive: got %d bytes, minimum 1024 required", len(data))
	}

	// Check for proper tar ending (two 512-byte zero blocks)
	expectedEnd := make([]byte, 1024)
	actualEnd := data[len(data)-1024:]
	if !bytes.Equal(actualEnd, expectedEnd) {
		return fmt.Errorf("tar archive does not end with two zero blocks")
	}

	// Validate tar magic numbers and format
	reader := tar.NewReader(bytes.NewReader(data))
	fileCount := 0
	totalSizeFromHeaders := int64(0)

	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading tar header: %w", err)
		}

		fileCount++

		// Validate header fields
		if header.Name == "" {
			return fmt.Errorf("file %d has empty name", fileCount)
		}

		// Validate file size is reasonable
		if header.Size < 0 {
			return fmt.Errorf("file %s has negative size: %d", header.Name, header.Size)
		}

		// Validate mode is reasonable (not zero or invalid)
		if header.Mode == 0 {
			return fmt.Errorf("file %s has zero mode", header.Name)
		}

		// Validate filename format matches expected pattern (20-digit zero-padded number)
		if !strings.HasSuffix(header.Name, ".txt") {
			return fmt.Errorf("file %s does not have .txt extension", header.Name)
		}

		baseName := strings.TrimSuffix(header.Name, ".txt")
		if len(baseName) != 20 {
			return fmt.Errorf("file %s does not have 20-character base name: got %d characters", header.Name, len(baseName))
		}

		// Check if name contains only digits
		for _, c := range baseName {
			if c < '0' || c > '9' {
				return fmt.Errorf("file %s contains non-digit characters in base name", header.Name)
			}
		}

		totalSizeFromHeaders += header.Size

		// Read and validate file content
		content, err := io.ReadAll(reader)
		if err != nil {
			return fmt.Errorf("error reading file %s content: %w", header.Name, err)
		}

		// Verify actual content size matches header
		if int64(len(content)) != header.Size {
			return fmt.Errorf("file %s content size mismatch: header says %d, actual %d",
				header.Name, header.Size, len(content))
		}

		// Validate content matches expected pattern
		fileNum := strings.TrimLeft(baseName, "0")
		if fileNum == "" {
			fileNum = "0" // Handle case where filename is all zeros
		}
		expectedContentPrefix := fmt.Sprintf("Content of file %s - ", fileNum)
		if !strings.HasPrefix(string(content), expectedContentPrefix) {
			return fmt.Errorf("file %s has unexpected content prefix: got %q, expected prefix %q",
				header.Name, string(content[:min(50, len(content))]), expectedContentPrefix)
		}

		// Validate that content is valid UTF-8 text
		if !isValidUTF8(content) {
			return fmt.Errorf("file %s contains invalid UTF-8 content", header.Name)
		}
	}

	if fileCount == 0 {
		return fmt.Errorf("tar archive contains no files")
	}

	// Validate total archive structure makes sense
	expectedMinSize := totalSizeFromHeaders + int64(fileCount*512) + 1024 // content + headers + end blocks
	if int64(len(data)) < expectedMinSize {
		return fmt.Errorf("tar archive size %d is smaller than expected minimum %d", len(data), expectedMinSize)
	}

	return nil
}

// isValidUTF8 checks if the byte slice contains valid UTF-8 encoded text
func isValidUTF8(data []byte) bool {
	return len(data) == 0 || strings.ToValidUTF8(string(data), "") == string(data)
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// runCLICommand executes a CLI command and returns the output
func runCLICommand(cliPath string, args ...string) error {
	cmd := exec.Command(cliPath, args...)
	cmd.Stdout = GinkgoWriter
	cmd.Stderr = GinkgoWriter
	return cmd.Run()
}

// extractFilesFromTar extracts all files from TAR data into a map[filename]content
func extractFilesFromTar(tarData []byte, filesMap map[string][]byte) error {
	reader := tar.NewReader(bytes.NewReader(tarData))

	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading TAR header: %w", err)
		}

		// Read the file content
		content, err := io.ReadAll(reader)
		if err != nil {
			return fmt.Errorf("error reading file content for %s: %w", header.Name, err)
		}

		// Store the file content in the map
		filesMap[header.Name] = content
	}

	return nil
}

var _ = Describe("End-to-End Server Test", func() {
	var (
		pgContainer          *tc_postgres.PostgresContainer
		minioContainer       *minio.MinioContainer
		serverCmd            *exec.Cmd
		serverBaseURL        string
		minioEndpoint        string
		minioHost            string
		minioAccessKey       string
		minioSecretKey       string
		testBucketName       string
		testBucketConfigName string
		testDatas3tName      string
		logger               *slog.Logger
		tempDir              string
		cliPath              string
	)

	BeforeEach(func(ctx SpecContext) {
		// Create logger that writes to GinkgoWriter for test visibility
		logger = slog.New(slog.NewTextHandler(GinkgoWriter, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		var err error

		// Create temporary directory for cache and builds
		tempDir = GinkgoT().TempDir()

		// Start PostgreSQL container
		pgContainer, err = tc_postgres.Run(ctx,
			"postgres:16-alpine",
			tc_postgres.WithDatabase("testdb"),
			tc_postgres.WithUsername("testuser"),
			tc_postgres.WithPassword("testpass"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(30*time.Second),
			),
			testcontainers.WithLogger(log.New(GinkgoWriter, "", 0)),
		)
		Expect(err).NotTo(HaveOccurred())

		// Start MinIO container
		minioContainer, err = minio.Run(
			ctx,
			"minio/minio:RELEASE.2024-01-16T16-07-38Z",
			minio.WithUsername("minioadmin"),
			minio.WithPassword("minioadmin"),
			testcontainers.WithLogger(log.New(GinkgoWriter, "", 0)),
		)
		Expect(err).NotTo(HaveOccurred())

		// Get MinIO connection details
		minioEndpoint, err = minioContainer.ConnectionString(ctx)
		Expect(err).NotTo(HaveOccurred())

		// Extract host:port from the full URL
		minioHost = strings.TrimPrefix(minioEndpoint, "http://")
		minioHost = strings.TrimPrefix(minioHost, "https://")
		minioEndpoint = minioHost

		minioAccessKey = "minioadmin"
		minioSecretKey = "minioadmin"
		testBucketName = "test-bucket"
		testBucketConfigName = "test-bucket-config"
		testDatas3tName = "test-datas3t"

		// Create test bucket in MinIO
		minioClient, err := miniogo.New(minioHost, &miniogo.Options{
			Creds:  miniocreds.NewStaticV4(minioAccessKey, minioSecretKey, ""),
			Secure: false,
		})
		Expect(err).NotTo(HaveOccurred())

		err = minioClient.MakeBucket(ctx, testBucketName, miniogo.MakeBucketOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Get PostgreSQL connection string
		connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		Expect(err).NotTo(HaveOccurred())

		// Ensure connection string starts with postgresql: for server compatibility
		if strings.HasPrefix(connStr, "postgres://") {
			connStr = strings.Replace(connStr, "postgres://", "postgresql://", 1)
		}

		// Run migrations
		m, err := migrate.New(
			"file://postgresstore/migrations",
			connStr)
		Expect(err).NotTo(HaveOccurred())

		err = m.Up()
		if err != nil && err != migrate.ErrNoChange {
			Expect(err).NotTo(HaveOccurred())
		}

		// Compile the CLI (includes server functionality)
		cliPath = filepath.Join(tempDir, "datas3t")
		buildCmd := exec.Command("go", "build", "-o", cliPath, "./cmd/datas3t")
		buildCmd.Dir = "."
		buildOutput, err := buildCmd.CombinedOutput()
		if err != nil {
			logger.Error("Failed to build CLI", "error", err, "output", string(buildOutput))
		}
		Expect(err).NotTo(HaveOccurred())

		// Find available port for server
		listener, err := net.Listen("tcp", ":0")
		Expect(err).NotTo(HaveOccurred())
		serverPort := listener.Addr().(*net.TCPAddr).Port
		listener.Close()

		serverAddr := fmt.Sprintf(":%d", serverPort)
		serverBaseURL = fmt.Sprintf("http://localhost%s", serverAddr)
		cacheDir := filepath.Join(tempDir, "cache")

		// Start the server
		serverCmd = exec.Command(cliPath, "server",
			"--addr", serverAddr,
			"--db-url", connStr,
			"--cache-dir", cacheDir,
			"--max-cache-size", "1073741824", // 1GB
			"--encryption-key", "dGVzdC1rZXktMzItYnl0ZXMtZm9yLXRlc3RpbmchIQ==", // test-key-32-bytes-for-testing!!
		)
		serverCmd.Stdout = GinkgoWriter
		serverCmd.Stderr = GinkgoWriter

		err = serverCmd.Start()
		Expect(err).NotTo(HaveOccurred())

		// Wait for server to be ready
		Eventually(func() error {
			_, err := http.Get(serverBaseURL + "/api/v1/buckets")
			return err
		}, 30*time.Second, 1*time.Second).Should(Succeed())

		logger.Info("Server started successfully", "url", serverBaseURL)

		// CLI will use the server URL through environment variable
		os.Setenv("DATAS3T_SERVER_URL", serverBaseURL)
	})

	AfterEach(func(ctx SpecContext) {
		if serverCmd != nil && serverCmd.Process != nil {
			serverCmd.Process.Signal(syscall.SIGTERM)
			serverCmd.Wait()
		}
		if pgContainer != nil {
			err := pgContainer.Terminate(ctx)
			Expect(err).NotTo(HaveOccurred())
		}
		if minioContainer != nil {
			err := minioContainer.Terminate(ctx)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	It("should complete full end-to-end workflow", func(ctx SpecContext) {
		// Step 1: Add bucket configuration using CLI
		logger.Info("Step 1: Adding bucket configuration using CLI")
		err := runCLICommand(cliPath, "bucket", "add",
			"--name", testBucketConfigName,
			"--endpoint", "http://"+minioEndpoint,
			"--bucket", testBucketName,
			"--access-key", minioAccessKey,
			"--secret-key", minioSecretKey,
		)
		Expect(err).NotTo(HaveOccurred())

		// Step 2: Add datas3t using CLI
		logger.Info("Step 2: Adding datas3t using CLI")
		err = runCLICommand(cliPath, "datas3t", "add",
			"--name", testDatas3tName,
			"--bucket", testBucketConfigName,
		)
		Expect(err).NotTo(HaveOccurred())

		// Step 3: Upload first datarange (files 0-17999)
		logger.Info("Step 3: Uploading first datarange with 18,000 datapoints")
		testData1, _ := createTestTarWithIndex(18000, 0) // files 0-17999, ~20MB total

		logger.Info("Created large test TAR file", "datapoints", 18000, "size_mb", len(testData1)/(1024*1024), "size_bytes", len(testData1), "avg_file_size_bytes", len(testData1)/18000)

		// Create temporary file for upload
		tarFile1 := filepath.Join(tempDir, "test1.tar")
		err = os.WriteFile(tarFile1, testData1, 0644)
		Expect(err).NotTo(HaveOccurred())

		// Upload using CLI
		err = runCLICommand(cliPath, "datarange", "upload-tar",
			"--datas3t", testDatas3tName,
			"--file", tarFile1,
		)
		Expect(err).NotTo(HaveOccurred())

		// Step 4: Upload second datarange (files 20000-37999) - gap between 18000-19999
		logger.Info("Step 4: Uploading second datarange with 18,000 datapoints")
		testData2, _ := createTestTarWithIndex(18000, 20000) // files 20000-37999, ~20MB total

		logger.Info("Created second large test TAR file", "datapoints", 18000, "size_mb", len(testData2)/(1024*1024), "size_bytes", len(testData2), "avg_file_size_bytes", len(testData2)/18000)

		// Create temporary file for upload
		tarFile2 := filepath.Join(tempDir, "test2.tar")
		err = os.WriteFile(tarFile2, testData2, 0644)
		Expect(err).NotTo(HaveOccurred())

		// Upload using CLI
		err = runCLICommand(cliPath, "datarange", "upload-tar",
			"--datas3t", testDatas3tName,
			"--file", tarFile2,
		)
		Expect(err).NotTo(HaveOccurred())

		// Step 5: Download tar file containing both dataranges using CLI
		logger.Info("Step 5: Downloading subset spanning both dataranges as TAR file using CLI")

		// Download partial range (files 17990-20010) to span across both dataranges and the gap using CLI
		partialTarPath := filepath.Join(tempDir, "partial_download.tar")
		err = runCLICommand(cliPath, "datarange", "download-tar",
			"--datas3t", testDatas3tName,
			"--first-datapoint", "17990",
			"--last-datapoint", "20010",
			"--output", partialTarPath,
		)
		Expect(err).NotTo(HaveOccurred())

		// Read and validate the downloaded TAR file
		partialTarData, err := os.ReadFile(partialTarPath)
		Expect(err).NotTo(HaveOccurred())

		logger.Info("Validating partial TAR archive", "size_mb", len(partialTarData)/(1024*1024), "size_bytes", len(partialTarData))

		// Use our comprehensive TAR archive validation
		err = validateTarArchive(partialTarData)
		if err != nil {
			logger.Error("Partial TAR archive validation failed", "error", err)
		}
		Expect(err).NotTo(HaveOccurred())

		// Step 5.1: Compare downloaded TAR file directly with expected TAR reconstruction
		logger.Info("Step 5.1: Performing byte-for-byte comparison of downloaded TAR vs expected TAR reconstruction")

		// Create expected TAR file for the partial range (17990-20010)
		// This includes files 17990-17999 from first datarange and 20000-20010 from second datarange
		expectedPartialTarData, _ := createTestTarWithIndex(21, 17990) // 21 files: 17990-17999 (10) + gap + 20000-20010 (11)

		// However, since there's a gap (18000-19999 don't exist), we need to create the expected TAR more carefully
		var expectedTarBuf bytes.Buffer
		tw := tar.NewWriter(&expectedTarBuf)

		// Files from first datarange (17990-17999)
		const targetFileSize = 20 * 1024 * 1024 / 18000 // Same size as original files
		for i := 17990; i <= 17999; i++ {
			filename := fmt.Sprintf("%020d.txt", i)
			baseContent := fmt.Sprintf("Content of file %d - ", i)

			var content string
			if targetFileSize < 1000 {
				repetitions := targetFileSize / len(baseContent)
				if repetitions < 1 {
					repetitions = 1
				}
				content = strings.Repeat(baseContent, repetitions)
				if len(content) < targetFileSize {
					content += strings.Repeat("X", targetFileSize-len(content))
				} else if len(content) > targetFileSize {
					content = content[:targetFileSize]
				}
			} else {
				repetitions := targetFileSize / len(baseContent)
				var contentBuilder strings.Builder
				contentBuilder.Grow(targetFileSize)
				for j := 0; j < repetitions; j++ {
					contentBuilder.WriteString(baseContent)
					contentBuilder.WriteString(fmt.Sprintf("line %d of %d, ", j+1, repetitions))
				}
				content = contentBuilder.String()
				if len(content) < targetFileSize {
					padding := strings.Repeat("X", targetFileSize-len(content))
					content += padding
				} else if len(content) > targetFileSize {
					content = content[:targetFileSize]
				}
			}

			header := &tar.Header{
				Name: filename,
				Size: int64(len(content)),
				Mode: 0644,
			}

			err := tw.WriteHeader(header)
			Expect(err).NotTo(HaveOccurred())

			_, err = tw.Write([]byte(content))
			Expect(err).NotTo(HaveOccurred())
		}

		// Files from second datarange (20000-20010)
		for i := 20000; i <= 20010; i++ {
			filename := fmt.Sprintf("%020d.txt", i)
			baseContent := fmt.Sprintf("Content of file %d - ", i)

			var content string
			if targetFileSize < 1000 {
				repetitions := targetFileSize / len(baseContent)
				if repetitions < 1 {
					repetitions = 1
				}
				content = strings.Repeat(baseContent, repetitions)
				if len(content) < targetFileSize {
					content += strings.Repeat("X", targetFileSize-len(content))
				} else if len(content) > targetFileSize {
					content = content[:targetFileSize]
				}
			} else {
				repetitions := targetFileSize / len(baseContent)
				var contentBuilder strings.Builder
				contentBuilder.Grow(targetFileSize)
				for j := 0; j < repetitions; j++ {
					contentBuilder.WriteString(baseContent)
					contentBuilder.WriteString(fmt.Sprintf("line %d of %d, ", j+1, repetitions))
				}
				content = contentBuilder.String()
				if len(content) < targetFileSize {
					padding := strings.Repeat("X", targetFileSize-len(content))
					content += padding
				} else if len(content) > targetFileSize {
					content = content[:targetFileSize]
				}
			}

			header := &tar.Header{
				Name: filename,
				Size: int64(len(content)),
				Mode: 0644,
			}

			err := tw.WriteHeader(header)
			Expect(err).NotTo(HaveOccurred())

			_, err = tw.Write([]byte(content))
			Expect(err).NotTo(HaveOccurred())
		}

		err = tw.Close()
		Expect(err).NotTo(HaveOccurred())

		expectedPartialTarData = expectedTarBuf.Bytes()

		// Compare the downloaded TAR file byte-for-byte with expected TAR file
		logger.Info("Comparing partial download TAR file",
			"downloaded_size", len(partialTarData),
			"expected_size", len(expectedPartialTarData))

		Expect(partialTarData).To(Equal(expectedPartialTarData), "Downloaded partial TAR should match expected TAR exactly")

		logger.Info("Partial TAR byte-for-byte comparison passed", "size_bytes", len(partialTarData))

		// Step 6: Download complete archive with all available files using CLI
		logger.Info("Step 6: Downloading complete archive with all 36,000 datapoints as TAR file using CLI")

		completeTarPath := filepath.Join(tempDir, "complete_download.tar")
		err = runCLICommand(cliPath, "datarange", "download-tar",
			"--datas3t", testDatas3tName,
			"--first-datapoint", "0",
			"--last-datapoint", "37999",
			"--output", completeTarPath,
		)
		Expect(err).NotTo(HaveOccurred())

		// Read and validate the complete TAR file
		completeTarData, err := os.ReadFile(completeTarPath)
		Expect(err).NotTo(HaveOccurred())

		logger.Info("Validating complete TAR archive", "size_mb", len(completeTarData)/(1024*1024), "size_bytes", len(completeTarData))

		// Use our comprehensive TAR archive validation
		err = validateTarArchive(completeTarData)
		if err != nil {
			logger.Error("Complete TAR archive validation failed", "error", err)
		}
		Expect(err).NotTo(HaveOccurred())

		// Step 6.1: Compare complete downloaded TAR file directly with expected TAR reconstruction
		logger.Info("Step 6.1: Performing byte-for-byte comparison of complete downloaded TAR vs expected TAR reconstruction")

		// Create expected complete TAR file by combining both dataranges
		var expectedCompleteTarBuf bytes.Buffer
		completeTw := tar.NewWriter(&expectedCompleteTarBuf)

		// Files from first datarange (0-17999)
		for i := 0; i < 18000; i++ {
			filename := fmt.Sprintf("%020d.txt", i)
			baseContent := fmt.Sprintf("Content of file %d - ", i)

			var content string
			if targetFileSize < 1000 {
				repetitions := targetFileSize / len(baseContent)
				if repetitions < 1 {
					repetitions = 1
				}
				content = strings.Repeat(baseContent, repetitions)
				if len(content) < targetFileSize {
					content += strings.Repeat("X", targetFileSize-len(content))
				} else if len(content) > targetFileSize {
					content = content[:targetFileSize]
				}
			} else {
				repetitions := targetFileSize / len(baseContent)
				var contentBuilder strings.Builder
				contentBuilder.Grow(targetFileSize)
				for j := 0; j < repetitions; j++ {
					contentBuilder.WriteString(baseContent)
					contentBuilder.WriteString(fmt.Sprintf("line %d of %d, ", j+1, repetitions))
				}
				content = contentBuilder.String()
				if len(content) < targetFileSize {
					padding := strings.Repeat("X", targetFileSize-len(content))
					content += padding
				} else if len(content) > targetFileSize {
					content = content[:targetFileSize]
				}
			}

			header := &tar.Header{
				Name: filename,
				Size: int64(len(content)),
				Mode: 0644,
			}

			err := completeTw.WriteHeader(header)
			Expect(err).NotTo(HaveOccurred())

			_, err = completeTw.Write([]byte(content))
			Expect(err).NotTo(HaveOccurred())
		}

		// Files from second datarange (20000-37999)
		for i := 20000; i < 38000; i++ {
			filename := fmt.Sprintf("%020d.txt", i)
			baseContent := fmt.Sprintf("Content of file %d - ", i)

			var content string
			if targetFileSize < 1000 {
				repetitions := targetFileSize / len(baseContent)
				if repetitions < 1 {
					repetitions = 1
				}
				content = strings.Repeat(baseContent, repetitions)
				if len(content) < targetFileSize {
					content += strings.Repeat("X", targetFileSize-len(content))
				} else if len(content) > targetFileSize {
					content = content[:targetFileSize]
				}
			} else {
				repetitions := targetFileSize / len(baseContent)
				var contentBuilder strings.Builder
				contentBuilder.Grow(targetFileSize)
				for j := 0; j < repetitions; j++ {
					contentBuilder.WriteString(baseContent)
					contentBuilder.WriteString(fmt.Sprintf("line %d of %d, ", j+1, repetitions))
				}
				content = contentBuilder.String()
				if len(content) < targetFileSize {
					padding := strings.Repeat("X", targetFileSize-len(content))
					content += padding
				} else if len(content) > targetFileSize {
					content = content[:targetFileSize]
				}
			}

			header := &tar.Header{
				Name: filename,
				Size: int64(len(content)),
				Mode: 0644,
			}

			err := completeTw.WriteHeader(header)
			Expect(err).NotTo(HaveOccurred())

			_, err = completeTw.Write([]byte(content))
			Expect(err).NotTo(HaveOccurred())
		}

		err = completeTw.Close()
		Expect(err).NotTo(HaveOccurred())

		expectedCompleteTarData := expectedCompleteTarBuf.Bytes()

		// Compare the complete downloaded TAR file byte-for-byte with expected TAR file
		logger.Info("Comparing complete download TAR file",
			"downloaded_size", len(completeTarData),
			"expected_size", len(expectedCompleteTarData))

		Expect(completeTarData).To(Equal(expectedCompleteTarData), "Downloaded complete TAR should match expected TAR exactly")

		// Step 7: Validate DatapointIterator streaming functionality
		logger.Info("Step 7: Testing DatapointIterator streaming with payload validation")

		// Create a client to use the DatapointIterator
		client := datas3t.NewClient(serverBaseURL)

		// Use DatapointIterator for the same partial range (17990-20010) to validate streaming
		datapointCount := 0
		expectedDatapoints := []int{}

		// Build list of expected datapoint indices (17990-17999 and 20000-20010)
		for i := 17990; i <= 17999; i++ {
			expectedDatapoints = append(expectedDatapoints, i)
		}
		for i := 20000; i <= 20010; i++ {
			expectedDatapoints = append(expectedDatapoints, i)
		}

		datapointIndex := 0
		for content, err := range client.DatapointIterator(ctx, testDatas3tName, 17990, 20010) {
			if err != nil {
				logger.Error("DatapointIterator error", "error", err)
				Expect(err).NotTo(HaveOccurred())
			}

			// Validate content is not empty
			Expect(content).NotTo(BeEmpty(), "Datapoint content should not be empty")

			// Validate content matches expected pattern for this datapoint
			expectedDatapointNum := expectedDatapoints[datapointIndex]
			expectedContentPrefix := fmt.Sprintf("Content of file %d - ", expectedDatapointNum)

			contentStr := string(content)
			Expect(contentStr).To(HavePrefix(expectedContentPrefix),
				"Datapoint %d content should have expected prefix", expectedDatapointNum)

			// Validate content size matches expected size (same as original files)
			expectedSize := targetFileSize
			Expect(len(content)).To(Equal(expectedSize),
				"Datapoint %d content size should match expected size", expectedDatapointNum)

			// Validate content is valid UTF-8
			Expect(isValidUTF8(content)).To(BeTrue(),
				"Datapoint %d content should be valid UTF-8", expectedDatapointNum)

			datapointCount++
			datapointIndex++
		}

		// Validate we got exactly the expected number of datapoints
		expectedCount := len(expectedDatapoints) // 21 datapoints (10 from first range + 11 from second range)
		Expect(datapointCount).To(Equal(expectedCount),
			"Should receive exactly %d datapoints via iterator", expectedCount)

		logger.Info("DatapointIterator validation completed successfully",
			"datapoints_processed", datapointCount,
			"expected_datapoints", expectedCount)

		// Step 8: Test GetDatapointsBitmap functionality
		logger.Info("Step 8: Testing GetDatapointsBitmap functionality")

		// Get the datapoints bitmap using the client
		bitmap, err := client.GetDatapointsBitmap(ctx, testDatas3tName)
		Expect(err).NotTo(HaveOccurred())
		Expect(bitmap).NotTo(BeNil())

		// Validate the bitmap has the correct total cardinality
		// Should be 36,000 datapoints (18,000 from first range + 18,000 from second range)
		expectedCardinality := uint64(36000)
		actualCardinality := bitmap.GetCardinality()
		Expect(actualCardinality).To(Equal(expectedCardinality),
			"Bitmap should contain exactly %d datapoints", expectedCardinality)

		logger.Info("Bitmap cardinality validation passed",
			"expected_cardinality", expectedCardinality,
			"actual_cardinality", actualCardinality)

		// Validate specific datapoints are set correctly
		// Test first datarange boundaries (0-17999)
		Expect(bitmap.Contains(0)).To(BeTrue(), "Datapoint 0 should be set")
		Expect(bitmap.Contains(1)).To(BeTrue(), "Datapoint 1 should be set")
		Expect(bitmap.Contains(17998)).To(BeTrue(), "Datapoint 17998 should be set")
		Expect(bitmap.Contains(17999)).To(BeTrue(), "Datapoint 17999 should be set")

		// Test gap datapoints are NOT set (18000-19999)
		Expect(bitmap.Contains(18000)).To(BeFalse(), "Datapoint 18000 should NOT be set (gap)")
		Expect(bitmap.Contains(18500)).To(BeFalse(), "Datapoint 18500 should NOT be set (gap)")
		Expect(bitmap.Contains(19999)).To(BeFalse(), "Datapoint 19999 should NOT be set (gap)")

		// Test second datarange boundaries (20000-37999)
		Expect(bitmap.Contains(20000)).To(BeTrue(), "Datapoint 20000 should be set")
		Expect(bitmap.Contains(20001)).To(BeTrue(), "Datapoint 20001 should be set")
		Expect(bitmap.Contains(37998)).To(BeTrue(), "Datapoint 37998 should be set")
		Expect(bitmap.Contains(37999)).To(BeTrue(), "Datapoint 37999 should be set")

		// Test datapoints beyond the ranges are NOT set
		Expect(bitmap.Contains(38000)).To(BeFalse(), "Datapoint 38000 should NOT be set (beyond range)")
		Expect(bitmap.Contains(100000)).To(BeFalse(), "Datapoint 100000 should NOT be set (beyond range)")

		logger.Info("Bitmap individual datapoint validation passed")

		// Validate that the bitmap contains exactly the expected ranges
		// Check a sample of datapoints from each range
		for i := uint64(0); i < 100; i++ {
			Expect(bitmap.Contains(i)).To(BeTrue(), "Datapoint %d should be set (first range)", i)
		}
		for i := uint64(17900); i < 18000; i++ {
			Expect(bitmap.Contains(i)).To(BeTrue(), "Datapoint %d should be set (first range end)", i)
		}
		for i := uint64(20000); i < 20100; i++ {
			Expect(bitmap.Contains(i)).To(BeTrue(), "Datapoint %d should be set (second range start)", i)
		}
		for i := uint64(37900); i < 38000; i++ {
			Expect(bitmap.Contains(i)).To(BeTrue(), "Datapoint %d should be set (second range end)", i)
		}

		// Test bitmap operations
		// Create a test bitmap with some overlapping datapoints
		testBitmap := roaring64.New()
		testBitmap.AddRange(17990, 20011) // AddRange uses exclusive upper bound, so this adds 17990-20010

		// Intersection should only contain the datapoints that exist in both bitmaps
		intersection := roaring64.And(bitmap, testBitmap)
		expectedIntersectionCardinality := uint64(21) // 10 from first range (17990-17999) + 11 from second range (20000-20010)
		Expect(intersection.GetCardinality()).To(Equal(expectedIntersectionCardinality),
			"Intersection should contain exactly %d datapoints", expectedIntersectionCardinality)

		// Validate the intersection contains the expected datapoints
		for i := uint64(17990); i <= 17999; i++ {
			Expect(intersection.Contains(i)).To(BeTrue(), "Intersection should contain datapoint %d", i)
		}
		for i := uint64(18000); i <= 19999; i++ {
			Expect(intersection.Contains(i)).To(BeFalse(), "Intersection should NOT contain datapoint %d (gap)", i)
		}
		for i := uint64(20000); i <= 20010; i++ {
			Expect(intersection.Contains(i)).To(BeTrue(), "Intersection should contain datapoint %d", i)
		}

		logger.Info("Bitmap operations validation passed",
			"intersection_cardinality", intersection.GetCardinality(),
			"expected_intersection_cardinality", expectedIntersectionCardinality)

		// Test error cases - try to get bitmap for non-existent datas3t
		nonExistentBitmap, err := client.GetDatapointsBitmap(ctx, "non-existent-datas3t")
		Expect(err).NotTo(HaveOccurred()) // Should not error for non-existent datas3t
		Expect(nonExistentBitmap).NotTo(BeNil())
		Expect(nonExistentBitmap.GetCardinality()).To(Equal(uint64(0)), "Non-existent datas3t should return empty bitmap")

		// Get bitmap size in bytes for reporting
		bitmapBytes, err := bitmap.MarshalBinary()
		Expect(err).NotTo(HaveOccurred())

		logger.Info("GetDatapointsBitmap validation completed successfully",
			"total_datapoints_in_bitmap", actualCardinality,
			"bitmap_size_bytes", len(bitmapBytes),
			"intersection_test_passed", true,
			"error_case_test_passed", true)

		logger.Info("End-to-end test with CLI completed successfully",
			"partial_tar_size_mb", len(partialTarData)/(1024*1024),
			"complete_tar_size_mb", len(completeTarData)/(1024*1024),
			"partial_tar_size_bytes", len(partialTarData),
			"complete_tar_size_bytes", len(completeTarData),
			"total_data_processed_mb", (len(partialTarData)+len(completeTarData))/(1024*1024),
			"total_datapoints_processed", 36000,
			"iterator_datapoints_validated", datapointCount)
	})
})
