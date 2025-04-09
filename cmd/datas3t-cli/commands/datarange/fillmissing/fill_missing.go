package fillmissing

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/draganm/datas3t/pkg/client"
	tarmmap "github.com/draganm/tar-mmap-go"
	"github.com/urfave/cli/v2"
)

// Matches the same pattern as in the server: 20-digit number followed by extension
var filePattern = regexp.MustCompile(`^(\d{20})\..+$`)

// Range represents a continuous range of sections in the tar file
type SectionRange struct {
	StartOffset uint64
	EndOffset   uint64
	IDs         []uint64
}

func Command(log *slog.Logger) *cli.Command {
	cfg := struct {
		serverURL string
		tarFile   string
		chunkSize int
	}{}

	return &cli.Command{
		Name:      "fill-missing",
		Usage:     "Upload missing data from a tar file to a dataset",
		ArgsUsage: "DATASET_ID",
		Flags: []cli.Flag{
			&cli.PathFlag{
				Name:        "tar-file",
				Required:    true,
				Usage:       "Tar file containing the dataset",
				Destination: &cfg.tarFile,
				EnvVars:     []string{"DATAS3T_TAR_FILE"},
			},
			&cli.StringFlag{
				Name:        "server-url",
				Required:    true,
				Usage:       "URL of the Datas3t server",
				Destination: &cfg.serverURL,
				EnvVars:     []string{"DATAS3T_SERVER_URL"},
			},
			&cli.IntFlag{
				Name:        "chunk-size",
				Value:       1000,
				Usage:       "Number of data points to upload at a time",
				Destination: &cfg.chunkSize,
				EnvVars:     []string{"DATAS3T_CHUNK_SIZE"},
			},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("expected exactly one argument: DATASET_ID")
			}

			datasetID := c.Args().Get(0)

			// Create a client
			cl, err := client.NewClient(cfg.serverURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			// Open the tar file
			tarMmap, err := tarmmap.Open(cfg.tarFile)
			if err != nil {
				return fmt.Errorf("failed to open tar file: %w", err)
			}
			defer tarMmap.Close()

			// Get missing ranges from the server
			missingRanges, err := cl.GetMissingRanges(c.Context, datasetID)
			if err != nil {
				return fmt.Errorf("failed to get missing ranges: %w", err)
			}

			// Map of datapoint ID -> tar section
			idToSection := make(map[uint64]int) // Maps ID to index in Sections slice
			var allIDs []uint64

			// Process each section in the tar file
			for i, section := range tarMmap.Sections {
				// Extract the ID from the filename
				matches := filePattern.FindStringSubmatch(filepath.Base(section.Header.Name))
				if matches == nil {
					log.Warn("Skipping section with invalid filename pattern", "filename", section.Header.Name)
					continue
				}

				// Parse the sequence number
				idStr := matches[1]
				id, err := strconv.ParseUint(idStr, 10, 64)
				if err != nil {
					log.Warn("Failed to parse ID from filename", "filename", section.Header.Name, "error", err)
					continue
				}

				idToSection[id] = i
				allIDs = append(allIDs, id)
			}

			if len(allIDs) == 0 {
				return fmt.Errorf("no valid datapoints found in tar file")
			}

			// Sort all IDs for easier processing
			sort.Slice(allIDs, func(i, j int) bool {
				return allIDs[i] < allIDs[j]
			})

			log.Info("Processed tar file",
				"datasetID", datasetID,
				"totalEntries", len(allIDs),
				"minID", allIDs[0],
				"maxID", allIDs[len(allIDs)-1])

			// Calculate datapoints to upload
			var datapointsToUpload []uint64

			// Handle different cases:

			// Case 1: Empty dataset (no datapoints yet)
			if missingRanges.FirstDatapoint == nil && missingRanges.LastDatapoint == nil && len(missingRanges.MissingRanges) == 0 {
				log.Info("Empty dataset, all datapoints from tar will be uploaded", "datasetID", datasetID)
				datapointsToUpload = allIDs
			} else {
				// Case 2: Dataset has data, check missing ranges and points below FirstDatapoint

				// First add datapoints from missing ranges
				for _, missingRange := range missingRanges.MissingRanges {
					for key := missingRange.Start; key <= missingRange.End; key++ {
						if _, exists := idToSection[key]; exists {
							datapointsToUpload = append(datapointsToUpload, key)
						}
					}
				}

				// Then check for datapoints below FirstDatapoint (if it exists)
				if missingRanges.FirstDatapoint != nil {
					firstServerDatapoint := *missingRanges.FirstDatapoint
					for _, id := range allIDs {
						if id < firstServerDatapoint {
							// This datapoint is below the lowest on the server, add it
							datapointsToUpload = append(datapointsToUpload, id)
						}
					}
				}

				// Remove any duplicates that might have been added
				if len(datapointsToUpload) > 0 {
					sort.Slice(datapointsToUpload, func(i, j int) bool {
						return datapointsToUpload[i] < datapointsToUpload[j]
					})

					// Remove duplicates
					j := 0
					for i := 1; i < len(datapointsToUpload); i++ {
						if datapointsToUpload[j] != datapointsToUpload[i] {
							j++
							datapointsToUpload[j] = datapointsToUpload[i]
						}
					}
					datapointsToUpload = datapointsToUpload[:j+1]
				}
			}

			if len(datapointsToUpload) == 0 {
				log.Info("No datapoints to upload", "datasetID", datasetID)
				return nil
			}

			log.Info("Found datapoints to upload",
				"datasetID", datasetID,
				"foundDatapoints", len(datapointsToUpload))

			// Create progress bar
			bar := pb.New64(int64(len(datapointsToUpload)))
			bar.Set(pb.Bytes, false)
			bar.SetTemplate(pb.Full)
			bar.Start()

			startTime := time.Now()
			var processedDatapoints uint64

			// Group the datapointsToUpload into chunks
			for i := 0; i < len(datapointsToUpload); i += cfg.chunkSize {
				end := i + cfg.chunkSize
				if end > len(datapointsToUpload) {
					end = len(datapointsToUpload)
				}

				// Get the IDs for this chunk
				chunkIDs := datapointsToUpload[i:end]

				// Create ranges for efficient extraction
				ranges := createOptimalRanges(tarMmap, chunkIDs, idToSection)

				// Create a temporary file for this chunk
				tmpFile, err := os.CreateTemp("", "datas3t-upload-*.tar")
				if err != nil {
					return fmt.Errorf("failed to create temporary file: %w", err)
				}
				tmpFilePath := tmpFile.Name()
				defer os.Remove(tmpFilePath)

				// Write the tar data for all ranges
				for _, r := range ranges {
					// Get the raw memory-mapped data for this range
					rawData := tarMmap.Mmap[r.StartOffset:r.EndOffset]

					// Write the raw data to the temporary file
					_, err := tmpFile.Write(rawData)
					if err != nil {
						tmpFile.Close()
						return fmt.Errorf("failed to write tar data: %w", err)
					}
				}

				// Add trailing blocks of zeros to properly terminate the tar file
				// (standard tar files end with at least two zero blocks)
				trailer := make([]byte, 1024) // 2 blocks of 512 zeros
				_, err = tmpFile.Write(trailer)
				if err != nil {
					tmpFile.Close()
					return fmt.Errorf("failed to write tar trailer: %w", err)
				}

				// Sync and rewind the file
				tmpFile.Sync()
				tmpFile.Seek(0, 0)

				// Upload this chunk
				err = cl.UploadDatarange(c.Context, datasetID, tmpFile)
				if err != nil {
					tmpFile.Close()
					return fmt.Errorf("failed to upload data chunk %d-%d: %w", i, end-1, err)
				}
				tmpFile.Close()

				// Update progress tracking
				chunkSize := end - i
				processedDatapoints += uint64(chunkSize)
				bar.Add64(int64(chunkSize))

				elapsed := time.Since(startTime)
				if processedDatapoints > 0 && elapsed.Seconds() > 0 {
					dataPointsPerSecond := float64(processedDatapoints) / elapsed.Seconds()
					remaining := float64(len(datapointsToUpload)-int(processedDatapoints)) / dataPointsPerSecond
					remainingDuration := time.Second * time.Duration(remaining)
					bar.Set("timeLeft", remainingDuration.Round(time.Second).String())
				}
			}

			bar.Finish()
			log.Info("Upload complete",
				"datasetID", datasetID,
				"totalDatapoints", len(datapointsToUpload),
				"duration", time.Since(startTime).Round(time.Second))

			return nil
		},
	}
}

// createOptimalRanges creates ranges of continuous tar sections for efficient extraction
func createOptimalRanges(tarMmap *tarmmap.TarMmap, ids []uint64, idToSection map[uint64]int) []SectionRange {
	var ranges []SectionRange
	if len(ids) == 0 {
		return ranges
	}

	// Sort the IDs to optimize for continuous ranges
	sort.Slice(ids, func(i, j int) bool {
		sectionI := tarMmap.Sections[idToSection[ids[i]]]
		sectionJ := tarMmap.Sections[idToSection[ids[j]]]
		return sectionI.HeaderOffset < sectionJ.HeaderOffset
	})

	// Start with the first range
	currentRange := SectionRange{
		StartOffset: tarMmap.Sections[idToSection[ids[0]]].HeaderOffset,
		EndOffset:   tarMmap.Sections[idToSection[ids[0]]].EndOfDataOffset,
		IDs:         []uint64{ids[0]},
	}

	// Process the rest of the IDs
	for i := 1; i < len(ids); i++ {
		id := ids[i]
		section := tarMmap.Sections[idToSection[id]]

		// If this section starts at or before the end of the current range,
		// extend the current range
		if section.HeaderOffset <= currentRange.EndOffset {
			if section.EndOfDataOffset > currentRange.EndOffset {
				currentRange.EndOffset = section.EndOfDataOffset
			}
			currentRange.IDs = append(currentRange.IDs, id)
		} else {
			// This section doesn't connect to the current range,
			// finalize the current range and start a new one
			ranges = append(ranges, currentRange)
			currentRange = SectionRange{
				StartOffset: section.HeaderOffset,
				EndOffset:   section.EndOfDataOffset,
				IDs:         []uint64{id},
			}
		}
	}

	// Add the last range
	ranges = append(ranges, currentRange)
	return ranges
}
