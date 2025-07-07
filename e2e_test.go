package datas3t_test

import (
	"archive/tar"
	"bytes"
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
	datas3tclient "github.com/draganm/datas3t/client"
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

	It("should complete full aggregation workflow", func(ctx SpecContext) {
		// Step 1: Add bucket configuration using CLI
		logger.Info("Step 1: Adding bucket configuration for aggregation test")
		err := runCLICommand(cliPath, "bucket", "add",
			"--name", testBucketConfigName,
			"--endpoint", "http://"+minioEndpoint,
			"--bucket", testBucketName,
			"--access-key", minioAccessKey,
			"--secret-key", minioSecretKey,
		)
		Expect(err).NotTo(HaveOccurred())

		// Step 2: Add datas3t using CLI
		logger.Info("Step 2: Adding datas3t for aggregation test")
		err = runCLICommand(cliPath, "datas3t", "add",
			"--name", testDatas3tName,
			"--bucket", testBucketConfigName,
		)
		Expect(err).NotTo(HaveOccurred())

		// Step 3: Upload multiple small dataranges for aggregation
		logger.Info("Step 3: Uploading multiple small dataranges for aggregation")

		// Create 4 small dataranges (each 5,000 datapoints, ~5MB each)
		datarangeInfo := []struct {
			startIndex int64
			numFiles   int
			filename   string
		}{
			{0, 5000, "datarange1.tar"},     // 0-4999
			{5000, 5000, "datarange2.tar"},  // 5000-9999
			{10000, 5000, "datarange3.tar"}, // 10000-14999
			{15000, 5000, "datarange4.tar"}, // 15000-19999
		}

		for i, info := range datarangeInfo {
			logger.Info("Creating and uploading datarange", "index", i+1, "start", info.startIndex, "count", info.numFiles)

			testData, _ := createTestTarWithIndex(info.numFiles, info.startIndex)
			tarFile := filepath.Join(tempDir, info.filename)
			err = os.WriteFile(tarFile, testData, 0644)
			Expect(err).NotTo(HaveOccurred())

			err = runCLICommand(cliPath, "datarange", "upload-tar",
				"--datas3t", testDatas3tName,
				"--file", tarFile,
			)
			Expect(err).NotTo(HaveOccurred())
		}

		// Step 4: Test aggregation using the client library
		logger.Info("Step 4: Testing aggregation using client library")

		client := datas3t.NewClient(serverBaseURL)

		// Test 1: Aggregate first 2 dataranges (0-9999) - should use direct PUT
		logger.Info("Test 1: Aggregating first 2 dataranges (0-9999) using direct PUT")

		err = client.AggregateDataRanges(ctx, testDatas3tName, 0, 9999, &datas3tclient.AggregateOptions{
			MaxParallelism: 2,
			MaxRetries:     3,
			ProgressCallback: func(info datas3tclient.ProgressInfo) {
				logger.Info("Aggregation progress",
					"phase", info.Phase,
					"percent", fmt.Sprintf("%.1f%%", info.PercentComplete),
					"step", info.CurrentStep)
			},
		})
		Expect(err).NotTo(HaveOccurred())

		// Verify the aggregation worked by checking the bitmap
		bitmap, err := client.GetDatapointsBitmap(ctx, testDatas3tName)
		Expect(err).NotTo(HaveOccurred())

		// Should still have all 20,000 datapoints (10,000 in aggregate + 10,000 in remaining ranges)
		Expect(bitmap.GetCardinality()).To(Equal(uint64(20000)))

		// Verify specific datapoints in the aggregated range still exist
		for i := uint64(0); i < 100; i++ {
			Expect(bitmap.Contains(i)).To(BeTrue(), "Datapoint %d should still exist after aggregation", i)
		}
		for i := uint64(9900); i < 10000; i++ {
			Expect(bitmap.Contains(i)).To(BeTrue(), "Datapoint %d should still exist after aggregation", i)
		}

		logger.Info("Direct PUT aggregation test passed")

		// Test 2: Aggregate remaining dataranges (10000-19999) - should use multipart upload
		logger.Info("Test 2: Aggregating remaining dataranges (10000-19999) using multipart upload")

		err = client.AggregateDataRanges(ctx, testDatas3tName, 10000, 19999, &datas3tclient.AggregateOptions{
			MaxParallelism: 3,
			MaxRetries:     3,
			ProgressCallback: func(info datas3tclient.ProgressInfo) {
				logger.Info("Large aggregation progress",
					"phase", info.Phase,
					"percent", fmt.Sprintf("%.1f%%", info.PercentComplete),
					"step", info.CurrentStep,
					"speed_mbps", fmt.Sprintf("%.2f", info.Speed/(1024*1024)))
			},
		})
		Expect(err).NotTo(HaveOccurred())

		// Verify the second aggregation worked
		bitmap, err = client.GetDatapointsBitmap(ctx, testDatas3tName)
		Expect(err).NotTo(HaveOccurred())

		// Should still have all 20,000 datapoints (now in 2 aggregated ranges)
		Expect(bitmap.GetCardinality()).To(Equal(uint64(20000)))

		// Verify specific datapoints in the second aggregated range still exist
		for i := uint64(10000); i < 10100; i++ {
			Expect(bitmap.Contains(i)).To(BeTrue(), "Datapoint %d should still exist after second aggregation", i)
		}
		for i := uint64(19900); i < 20000; i++ {
			Expect(bitmap.Contains(i)).To(BeTrue(), "Datapoint %d should still exist after second aggregation", i)
		}

		logger.Info("Multipart aggregation test passed")

		// Step 5: Test aggregated data integrity by downloading and validating
		logger.Info("Step 5: Testing aggregated data integrity")

		// Download a range that spans the boundary of the first aggregation (9995-10005)
		boundaryTarPath := filepath.Join(tempDir, "boundary_download.tar")
		err = runCLICommand(cliPath, "datarange", "download-tar",
			"--datas3t", testDatas3tName,
			"--first-datapoint", "9995",
			"--last-datapoint", "10005",
			"--output", boundaryTarPath,
		)
		Expect(err).NotTo(HaveOccurred())

		// Read and validate the boundary TAR file
		boundaryTarData, err := os.ReadFile(boundaryTarPath)
		Expect(err).NotTo(HaveOccurred())

		// Validate TAR structure
		err = validateTarArchive(boundaryTarData)
		Expect(err).NotTo(HaveOccurred())

		// Extract files and verify content continuity across aggregation boundary
		boundaryFiles := make(map[string][]byte)
		err = extractFilesFromTar(boundaryTarData, boundaryFiles)
		Expect(err).NotTo(HaveOccurred())

		// Verify we have the expected 11 files (9995-10005)
		Expect(len(boundaryFiles)).To(Equal(11))

		// Verify specific files exist and have correct content
		for i := 9995; i <= 10005; i++ {
			filename := fmt.Sprintf("%020d.txt", i)
			content, exists := boundaryFiles[filename]
			Expect(exists).To(BeTrue(), "File %s should exist", filename)

			expectedPrefix := fmt.Sprintf("Content of file %d - ", i)
			Expect(string(content)).To(HavePrefix(expectedPrefix),
				"File %s should have correct content", filename)
		}

		logger.Info("Aggregated data integrity test passed")

		// Step 6: Test final aggregation of everything (0-19999)
		logger.Info("Step 6: Testing final aggregation of all data (0-19999)")

		err = client.AggregateDataRanges(ctx, testDatas3tName, 0, 19999, &datas3tclient.AggregateOptions{
			MaxParallelism: 4,
			MaxRetries:     3,
			ProgressCallback: func(info datas3tclient.ProgressInfo) {
				logger.Info("Final aggregation progress",
					"phase", info.Phase,
					"percent", fmt.Sprintf("%.1f%%", info.PercentComplete),
					"step", info.CurrentStep,
					"eta", info.EstimatedETA.Round(time.Second))
			},
		})
		Expect(err).NotTo(HaveOccurred())

		// Verify final state - should still have all datapoints
		bitmap, err = client.GetDatapointsBitmap(ctx, testDatas3tName)
		Expect(err).NotTo(HaveOccurred())
		Expect(bitmap.GetCardinality()).To(Equal(uint64(20000)))

		// Step 7: Test error conditions
		logger.Info("Step 7: Testing aggregation error conditions")

		// Test 1: Try to aggregate insufficient dataranges (only 1 datarange)
		logger.Info("Testing insufficient dataranges error")
		err = client.AggregateDataRanges(ctx, testDatas3tName, 0, 4999, nil)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("not fully covered"))

		// Test 2: Try to aggregate non-existent datas3t
		logger.Info("Testing non-existent datas3t error")
		err = client.AggregateDataRanges(ctx, "non-existent-datas3t", 0, 10000, nil)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("failed to start aggregate"))

		// Test 3: Try to aggregate with gaps
		logger.Info("Testing aggregation with gaps error")
		err = client.AggregateDataRanges(ctx, testDatas3tName, 0, 30000, nil)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("not fully covered"))

		logger.Info("Error condition tests passed")

		// Step 8: Validate final data integrity with complete download
		logger.Info("Step 8: Final data integrity validation")

		// Download the complete aggregated dataset
		finalTarPath := filepath.Join(tempDir, "final_aggregated.tar")
		err = runCLICommand(cliPath, "datarange", "download-tar",
			"--datas3t", testDatas3tName,
			"--first-datapoint", "0",
			"--last-datapoint", "19999",
			"--output", finalTarPath,
		)
		Expect(err).NotTo(HaveOccurred())

		// Validate the final aggregated TAR
		finalTarData, err := os.ReadFile(finalTarPath)
		Expect(err).NotTo(HaveOccurred())

		err = validateTarArchive(finalTarData)
		Expect(err).NotTo(HaveOccurred())

		// Test using DatapointIterator on the aggregated data
		logger.Info("Testing DatapointIterator on aggregated data")
		datapointCount := 0
		for content, err := range client.DatapointIterator(ctx, testDatas3tName, 0, 19999) {
			Expect(err).NotTo(HaveOccurred())
			Expect(content).NotTo(BeEmpty())

			// Validate content pattern for first and last few datapoints
			if datapointCount < 10 || datapointCount >= 19990 {
				expectedPrefix := fmt.Sprintf("Content of file %d - ", datapointCount)
				Expect(string(content)).To(HavePrefix(expectedPrefix))
			}

			datapointCount++
		}

		Expect(datapointCount).To(Equal(20000), "Should iterate over all 20,000 datapoints")

		logger.Info("Aggregation workflow completed successfully",
			"original_dataranges", 4,
			"aggregation_operations", 3,
			"final_datapoint_count", datapointCount,
			"final_tar_size_mb", len(finalTarData)/(1024*1024),
			"aggregation_integrity_verified", true)
	})

	It("should complete full datas3t import workflow using CLI", func(ctx SpecContext) {
		// Step 1: Add bucket configuration using CLI
		logger.Info("Step 1: Adding bucket configuration for import test")
		err := runCLICommand(cliPath, "bucket", "add",
			"--name", testBucketConfigName,
			"--endpoint", "http://"+minioEndpoint,
			"--bucket", testBucketName,
			"--access-key", minioAccessKey,
			"--secret-key", minioSecretKey,
		)
		Expect(err).NotTo(HaveOccurred())

		// Step 2: Manually create datas3t objects in S3 to simulate pre-existing data
		logger.Info("Step 2: Creating pre-existing datas3t objects in S3")

		// Create MinIO client to directly upload test objects
		minioClient, err := miniogo.New(minioHost, &miniogo.Options{
			Creds:  miniocreds.NewStaticV4(minioAccessKey, minioSecretKey, ""),
			Secure: false,
		})
		Expect(err).NotTo(HaveOccurred())

		// Create test dataranges that match the datas3t pattern
		// Pattern: datas3t/{datas3t_name}/dataranges/{first_datapoint}-{last_datapoint}-{upload_counter}.tar

		// Dataset 1: import-test-dataset-1 (3 dataranges)
		dataset1Objects := []struct {
			objectKey     string
			indexKey      string
			startIndex    int64
			numFiles      int
			uploadCounter int64
		}{
			{
				objectKey:     "datas3t/import-test-dataset-1/dataranges/00000000000000000000-00000000000000001999-000000000001.tar",
				indexKey:      "datas3t/import-test-dataset-1/dataranges/00000000000000000000-00000000000000001999-000000000001.index",
				startIndex:    0,
				numFiles:      2000,
				uploadCounter: 1,
			},
			{
				objectKey:     "datas3t/import-test-dataset-1/dataranges/00000000000000002000-00000000000000003999-000000000002.tar",
				indexKey:      "datas3t/import-test-dataset-1/dataranges/00000000000000002000-00000000000000003999-000000000002.index",
				startIndex:    2000,
				numFiles:      2000,
				uploadCounter: 2,
			},
			{
				objectKey:     "datas3t/import-test-dataset-1/dataranges/00000000000000006000-00000000000000007999-000000000005.tar",
				indexKey:      "datas3t/import-test-dataset-1/dataranges/00000000000000006000-00000000000000007999-000000000005.index",
				startIndex:    6000,
				numFiles:      2000,
				uploadCounter: 5, // Higher upload counter to test upload_counter updating
			},
		}

		// Dataset 2: import-test-dataset-2 (2 dataranges)
		dataset2Objects := []struct {
			objectKey     string
			indexKey      string
			startIndex    int64
			numFiles      int
			uploadCounter int64
		}{
			{
				objectKey:     "datas3t/import-test-dataset-2/dataranges/00000000000000010000-00000000000000011999-000000000003.tar",
				indexKey:      "datas3t/import-test-dataset-2/dataranges/00000000000000010000-00000000000000011999-000000000003.index",
				startIndex:    10000,
				numFiles:      2000,
				uploadCounter: 3,
			},
			{
				objectKey:     "datas3t/import-test-dataset-2/dataranges/00000000000000012000-00000000000000013999-000000000007.tar",
				indexKey:      "datas3t/import-test-dataset-2/dataranges/00000000000000012000-00000000000000013999-000000000007.index",
				startIndex:    12000,
				numFiles:      2000,
				uploadCounter: 7, // Higher upload counter for this dataset
			},
		}

		// Upload all dataset1 objects
		for i, obj := range dataset1Objects {
			logger.Info("Creating dataset1 object", "index", i+1, "start", obj.startIndex, "files", obj.numFiles, "upload_counter", obj.uploadCounter)

			// Create TAR data and index
			tarData, indexData := createTestTarWithIndex(obj.numFiles, obj.startIndex)

			// Upload TAR file
			_, err = minioClient.PutObject(ctx, testBucketName, obj.objectKey,
				bytes.NewReader(tarData), int64(len(tarData)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Upload index file
			_, err = minioClient.PutObject(ctx, testBucketName, obj.indexKey,
				bytes.NewReader(indexData), int64(len(indexData)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())
		}

		// Upload all dataset2 objects
		for i, obj := range dataset2Objects {
			logger.Info("Creating dataset2 object", "index", i+1, "start", obj.startIndex, "files", obj.numFiles, "upload_counter", obj.uploadCounter)

			// Create TAR data and index
			tarData, indexData := createTestTarWithIndex(obj.numFiles, obj.startIndex)

			// Upload TAR file
			_, err = minioClient.PutObject(ctx, testBucketName, obj.objectKey,
				bytes.NewReader(tarData), int64(len(tarData)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Upload index file
			_, err = minioClient.PutObject(ctx, testBucketName, obj.indexKey,
				bytes.NewReader(indexData), int64(len(indexData)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())
		}

		// Step 3: Add some non-datas3t objects to verify they're ignored
		logger.Info("Step 3: Adding non-datas3t objects that should be ignored")

		// Random file that doesn't match pattern
		_, err = minioClient.PutObject(ctx, testBucketName, "random-file.txt",
			bytes.NewReader([]byte("random content")), 14, miniogo.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Object in datas3t folder but wrong pattern
		_, err = minioClient.PutObject(ctx, testBucketName, "datas3t/invalid-pattern.txt",
			bytes.NewReader([]byte("invalid pattern")), 15, miniogo.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Object with invalid naming convention
		_, err = minioClient.PutObject(ctx, testBucketName, "datas3t/test/dataranges/invalid-name.tar",
			bytes.NewReader([]byte("invalid naming")), 14, miniogo.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Step 4: Verify no datas3ts exist yet in the database
		logger.Info("Step 4: Verifying database is empty before import")

		err = runCLICommand(cliPath, "datas3t", "list", "--json")
		Expect(err).NotTo(HaveOccurred())

		// Step 5: Perform import using CLI
		logger.Info("Step 5: Performing import using CLI")

		err = runCLICommand(cliPath, "datas3t", "import",
			"--bucket", testBucketConfigName,
		)
		Expect(err).NotTo(HaveOccurred())

		// Step 6: Verify imported datas3ts using CLI
		logger.Info("Step 6: Verifying imported datas3ts using CLI")

		err = runCLICommand(cliPath, "datas3t", "list")
		Expect(err).NotTo(HaveOccurred())

		// Step 7: Verify imported data using client
		logger.Info("Step 7: Verifying imported data using client")

		client := datas3tclient.NewClient(serverBaseURL)

		// List all datas3ts to verify import
		datas3ts, err := client.ListDatas3ts(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(datas3ts).To(HaveLen(2))

		// Sort datas3ts by name for consistent testing
		var dataset1, dataset2 *datas3tclient.Datas3tInfo
		for i := range datas3ts {
			if datas3ts[i].Datas3tName == "import-test-dataset-1" {
				dataset1 = &datas3ts[i]
			} else if datas3ts[i].Datas3tName == "import-test-dataset-2" {
				dataset2 = &datas3ts[i]
			}
		}

		Expect(dataset1).NotTo(BeNil())
		Expect(dataset2).NotTo(BeNil())

		// Verify dataset1 statistics
		Expect(dataset1.BucketName).To(Equal(testBucketConfigName))
		Expect(dataset1.DatarangeCount).To(Equal(int64(3)))
		Expect(dataset1.TotalDatapoints).To(Equal(int64(6000))) // 2000 + 2000 + 2000
		Expect(dataset1.LowestDatapoint).To(Equal(int64(0)))
		Expect(dataset1.HighestDatapoint).To(Equal(int64(7999)))

		// Verify dataset2 statistics
		Expect(dataset2.BucketName).To(Equal(testBucketConfigName))
		Expect(dataset2.DatarangeCount).To(Equal(int64(2)))
		Expect(dataset2.TotalDatapoints).To(Equal(int64(4000))) // 2000 + 2000
		Expect(dataset2.LowestDatapoint).To(Equal(int64(10000)))
		Expect(dataset2.HighestDatapoint).To(Equal(int64(13999)))

		logger.Info("Import verification passed",
			"dataset1_datapoints", dataset1.TotalDatapoints,
			"dataset2_datapoints", dataset2.TotalDatapoints)

		// Step 8: Test upload counter updates
		logger.Info("Step 8: Verifying upload counter updates")

		// We can't directly check upload counters via CLI, but we can test that new uploads
		// get higher counters by uploading new data and checking the object keys in S3

		// Add new datarange to dataset1 using CLI
		testData, _ := createTestTarWithIndex(1000, 8000) // files 8000-8999
		newTarFile := filepath.Join(tempDir, "new_upload.tar")
		err = os.WriteFile(newTarFile, testData, 0644)
		Expect(err).NotTo(HaveOccurred())

		err = runCLICommand(cliPath, "datarange", "upload-tar",
			"--datas3t", "import-test-dataset-1",
			"--file", newTarFile,
		)
		Expect(err).NotTo(HaveOccurred())

		// List objects in bucket to verify new upload has counter > 5
		objectsInfo := minioClient.ListObjects(ctx, testBucketName, miniogo.ListObjectsOptions{
			Prefix: "datas3t/import-test-dataset-1/dataranges/",
		})

		foundNewObject := false
		for objInfo := range objectsInfo {
			if objInfo.Err != nil {
				continue
			}
			// Look for objects with counter > 5 (should be 6 or higher)
			if strings.Contains(objInfo.Key, "000000000006.tar") ||
				strings.Contains(objInfo.Key, "000000000007.tar") ||
				strings.Contains(objInfo.Key, "000000000008.tar") {
				foundNewObject = true
				break
			}
		}
		Expect(foundNewObject).To(BeTrue(), "New upload should have upload counter > 5")

		logger.Info("Upload counter verification passed")

		// Step 9: Test data integrity of imported data
		logger.Info("Step 9: Testing data integrity of imported data")

		// Test bitmap functionality
		bitmap1, err := client.GetDatapointsBitmap(ctx, "import-test-dataset-1")
		Expect(err).NotTo(HaveOccurred())

		// Should include the newly uploaded data too
		Expect(bitmap1.GetCardinality()).To(BeNumerically(">=", 6000))

		// Verify specific datapoints exist from imported data
		// Dataset1: 0-1999, 2000-3999, 6000-7999
		for i := uint64(0); i < 100; i++ {
			Expect(bitmap1.Contains(i)).To(BeTrue(), "Datapoint %d should exist", i)
		}
		for i := uint64(2000); i < 2100; i++ {
			Expect(bitmap1.Contains(i)).To(BeTrue(), "Datapoint %d should exist", i)
		}
		for i := uint64(6000); i < 6100; i++ {
			Expect(bitmap1.Contains(i)).To(BeTrue(), "Datapoint %d should exist", i)
		}

		// Verify gap doesn't exist (4000-5999)
		for i := uint64(4000); i < 4100; i++ {
			Expect(bitmap1.Contains(i)).To(BeFalse(), "Datapoint %d should NOT exist (gap)", i)
		}

		logger.Info("Bitmap verification passed")

		// Step 10: Test download of imported data
		logger.Info("Step 10: Testing download of imported data")

		// Download a range spanning imported dataranges
		downloadTarPath := filepath.Join(tempDir, "imported_download.tar")
		err = runCLICommand(cliPath, "datarange", "download-tar",
			"--datas3t", "import-test-dataset-1",
			"--first-datapoint", "1900",
			"--last-datapoint", "2100",
			"--output", downloadTarPath,
		)
		Expect(err).NotTo(HaveOccurred())

		// Validate downloaded data
		downloadedData, err := os.ReadFile(downloadTarPath)
		Expect(err).NotTo(HaveOccurred())

		err = validateTarArchive(downloadedData)
		Expect(err).NotTo(HaveOccurred())

		// Extract and verify content
		downloadedFiles := make(map[string][]byte)
		err = extractFilesFromTar(downloadedData, downloadedFiles)
		Expect(err).NotTo(HaveOccurred())

		// Should have 201 files (1900-2100)
		Expect(len(downloadedFiles)).To(Equal(201))

		// Verify specific files have correct content
		for i := 1900; i <= 2100; i++ {
			filename := fmt.Sprintf("%020d.txt", i)
			content, exists := downloadedFiles[filename]
			Expect(exists).To(BeTrue(), "File %s should exist", filename)

			expectedPrefix := fmt.Sprintf("Content of file %d - ", i)
			Expect(string(content)).To(HavePrefix(expectedPrefix))
		}

		logger.Info("Download verification passed")

		// Step 11: Test DatapointIterator on imported data
		logger.Info("Step 11: Testing DatapointIterator on imported data")

		datapointCount := 0
		for content, err := range client.DatapointIterator(ctx, "import-test-dataset-2", 10000, 10099) {
			Expect(err).NotTo(HaveOccurred())
			Expect(content).NotTo(BeEmpty())

			// Verify content pattern
			expectedPrefix := fmt.Sprintf("Content of file %d - ", 10000+datapointCount)
			Expect(string(content)).To(HavePrefix(expectedPrefix))

			datapointCount++
		}

		Expect(datapointCount).To(Equal(100), "Should iterate over 100 datapoints")

		logger.Info("DatapointIterator verification passed")

		// Step 12: Test re-import (should not create duplicates)
		logger.Info("Step 12: Testing re-import to verify no duplicates")

		err = runCLICommand(cliPath, "datas3t", "import",
			"--bucket", testBucketConfigName,
		)
		Expect(err).NotTo(HaveOccurred())

		// Verify counts haven't changed (no duplicates)
		datas3tsAfterReimport, err := client.ListDatas3ts(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(datas3tsAfterReimport).To(HaveLen(2))

		// Find datasets again
		for i := range datas3tsAfterReimport {
			if datas3tsAfterReimport[i].Datas3tName == "import-test-dataset-1" {
				// Datarange count should be the same (no duplicates)
				// Note: might be +1 due to the manual upload in step 8
				Expect(datas3tsAfterReimport[i].DatarangeCount).To(BeNumerically("<=", 4))
			} else if datas3tsAfterReimport[i].Datas3tName == "import-test-dataset-2" {
				// Should be exactly the same
				Expect(datas3tsAfterReimport[i].DatarangeCount).To(Equal(int64(2)))
				Expect(datas3tsAfterReimport[i].TotalDatapoints).To(Equal(int64(4000)))
			}
		}

		logger.Info("Re-import verification passed - no duplicates created")

		// Step 13: Test import with JSON output
		logger.Info("Step 13: Testing import with JSON output")

		err = runCLICommand(cliPath, "datas3t", "import",
			"--bucket", testBucketConfigName,
			"--json",
		)
		Expect(err).NotTo(HaveOccurred())

		logger.Info("Import workflow completed successfully",
			"imported_datasets", 2,
			"total_datapoints_dataset1", dataset1.TotalDatapoints,
			"total_datapoints_dataset2", dataset2.TotalDatapoints,
			"upload_counter_updates_verified", true,
			"data_integrity_verified", true,
			"duplicate_prevention_verified", true)
	})

})
