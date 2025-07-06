package dataranges_test

import (
	"bytes"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t/postgresstore"
	"github.com/draganm/datas3t/server/dataranges"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Aggregate Operations", func() {
	var env *TestEnvironment

	BeforeEach(func(ctx SpecContext) {
		env = SetupTestEnvironment(ctx)
	})

	AfterEach(func(ctx SpecContext) {
		env.TeardownTestEnvironment(ctx)
	})

	Context("StartAggregate", func() {
		BeforeEach(func(ctx SpecContext) {
			// Create multiple dataranges for aggregation
			// Datarange 1: 0-9 (10 datapoints)
			env.CreateCompletedDatarangeForAggregation(ctx, 0, 10)
			// Datarange 2: 10-19 (10 datapoints)
			env.CreateCompletedDatarangeForAggregation(ctx, 10, 10)
			// Datarange 3: 20-29 (10 datapoints)
			env.CreateCompletedDatarangeForAggregation(ctx, 20, 10)
		})

		Context("when starting a valid aggregate", func() {
			It("should successfully start aggregation for small total size (direct PUT)", func(ctx SpecContext) {
				req := &dataranges.StartAggregateRequest{
					Datas3tName:         env.TestDatas3tName,
					FirstDatapointIndex: 0,
					LastDatapointIndex:  19, // Covers first two dataranges
				}

				resp, err := env.UploadSrv.StartAggregate(ctx, env.Logger, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp).NotTo(BeNil())
				Expect(resp.AggregateUploadID).To(BeNumerically(">", 0))
				Expect(resp.UseDirectPut).To(BeTrue()) // Total size should be small
				Expect(resp.PresignedDataPutURL).NotTo(BeEmpty())
				Expect(resp.PresignedIndexPutURL).NotTo(BeEmpty())
				Expect(resp.PresignedMultipartUploadPutURLs).To(BeEmpty())
				Expect(len(resp.SourceDatarangeDownloadURLs)).To(Equal(2))

				// Verify source datarange URLs
				for _, sourceURL := range resp.SourceDatarangeDownloadURLs {
					Expect(sourceURL.PresignedDataURL).NotTo(BeEmpty())
					Expect(sourceURL.PresignedIndexURL).NotTo(BeEmpty())
					Expect(sourceURL.DatarangeID).To(BeNumerically(">", 0))
					Expect(sourceURL.SizeBytes).To(BeNumerically(">", 0))
				}

				// Verify aggregate upload record was created
				uploadDetails, err := env.Queries.GetAggregateUploadWithDetails(ctx, resp.AggregateUploadID)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadDetails.FirstDatapointIndex).To(Equal(int64(0)))
				Expect(uploadDetails.LastDatapointIndex).To(Equal(int64(19)))
				Expect(len(uploadDetails.SourceDatarangeIds)).To(Equal(2))
			})

			It("should successfully start aggregation for large total size (multipart)", func(ctx SpecContext) {
				// Create larger dataranges to exceed multipart threshold (>5MB)
				// Each datarange is ~1MB, so we need at least 6 to exceed 5MB threshold
				env.CreateCompletedDatarangeForAggregation(ctx, 30, 1000)   // 30-1029 (~1MB)
				env.CreateCompletedDatarangeForAggregation(ctx, 1030, 1000) // 1030-2029 (~1MB)
				env.CreateCompletedDatarangeForAggregation(ctx, 2030, 1000) // 2030-3029 (~1MB)
				env.CreateCompletedDatarangeForAggregation(ctx, 3030, 1000) // 3030-4029 (~1MB)
				env.CreateCompletedDatarangeForAggregation(ctx, 4030, 1000) // 4030-5029 (~1MB)
				env.CreateCompletedDatarangeForAggregation(ctx, 5030, 1000) // 5030-6029 (~1MB)

				req := &dataranges.StartAggregateRequest{
					Datas3tName:         env.TestDatas3tName,
					FirstDatapointIndex: 30,
					LastDatapointIndex:  6029, // Covers all six large dataranges (~6MB total)
				}

				resp, err := env.UploadSrv.StartAggregate(ctx, env.Logger, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp).NotTo(BeNil())
				Expect(resp.UseDirectPut).To(BeFalse()) // Total size should exceed threshold
				Expect(resp.PresignedDataPutURL).To(BeEmpty())
				Expect(resp.PresignedIndexPutURL).NotTo(BeEmpty())
				Expect(resp.PresignedMultipartUploadPutURLs).NotTo(BeEmpty())
				Expect(len(resp.SourceDatarangeDownloadURLs)).To(Equal(6))
			})

			It("should handle aggregation across all dataranges", func(ctx SpecContext) {
				req := &dataranges.StartAggregateRequest{
					Datas3tName:         env.TestDatas3tName,
					FirstDatapointIndex: 0,
					LastDatapointIndex:  29, // Covers all three dataranges (0-29)
				}

				resp, err := env.UploadSrv.StartAggregate(ctx, env.Logger, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp).NotTo(BeNil())
				Expect(len(resp.SourceDatarangeDownloadURLs)).To(Equal(3))

				// Verify all source dataranges are included
				var minKeys, maxKeys []int64
				for _, sourceURL := range resp.SourceDatarangeDownloadURLs {
					minKeys = append(minKeys, sourceURL.MinDatapointKey)
					maxKeys = append(maxKeys, sourceURL.MaxDatapointKey)
				}
				Expect(minKeys).To(ContainElements(int64(0), int64(10), int64(20)))
				Expect(maxKeys).To(ContainElements(int64(9), int64(19), int64(29)))
			})
		})

		Context("when validation fails", func() {
			It("should reject empty datas3t name", func(ctx SpecContext) {
				req := &dataranges.StartAggregateRequest{
					Datas3tName:         "",
					FirstDatapointIndex: 0,
					LastDatapointIndex:  19,
				}

				_, err := env.UploadSrv.StartAggregate(ctx, env.Logger, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("datas3t_name is required"))
			})

			It("should reject invalid datapoint range", func(ctx SpecContext) {
				req := &dataranges.StartAggregateRequest{
					Datas3tName:         env.TestDatas3tName,
					FirstDatapointIndex: 20,
					LastDatapointIndex:  10, // Last < First
				}

				_, err := env.UploadSrv.StartAggregate(ctx, env.Logger, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("first_datapoint_index must be <= last_datapoint_index"))
			})

			It("should reject non-existent datas3t", func(ctx SpecContext) {
				req := &dataranges.StartAggregateRequest{
					Datas3tName:         "non-existent-datas3t",
					FirstDatapointIndex: 0,
					LastDatapointIndex:  19,
				}

				_, err := env.UploadSrv.StartAggregate(ctx, env.Logger, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to find datas3t 'non-existent-datas3t'"))
			})

			It("should reject range with insufficient dataranges", func(ctx SpecContext) {
				req := &dataranges.StartAggregateRequest{
					Datas3tName:         env.TestDatas3tName,
					FirstDatapointIndex: 0,
					LastDatapointIndex:  5, // Only covers part of first datarange
				}

				_, err := env.UploadSrv.StartAggregate(ctx, env.Logger, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("not fully covered"))
			})

			It("should reject range with gaps", func(ctx SpecContext) {
				// Delete the middle datarange to create a gap
				deleteReq := &dataranges.DeleteDatarangeRequest{
					Datas3tName:       env.TestDatas3tName,
					FirstDatapointKey: 10,
					LastDatapointKey:  19,
				}
				err := env.UploadSrv.DeleteDatarange(ctx, env.Logger, deleteReq)
				Expect(err).NotTo(HaveOccurred())

				req := &dataranges.StartAggregateRequest{
					Datas3tName:         env.TestDatas3tName,
					FirstDatapointIndex: 0,
					LastDatapointIndex:  29, // Now has gap from 10-19
				}

				_, err = env.UploadSrv.StartAggregate(ctx, env.Logger, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("range is not fully covered"))
			})
		})
	})

	Context("CompleteAggregate", func() {
		var aggregateResp *dataranges.StartAggregateResponse
		var aggregatedTarData []byte
		var aggregatedIndexData []byte

		BeforeEach(func(ctx SpecContext) {
			// Create source dataranges
			env.CreateCompletedDatarangeForAggregation(ctx, 0, 10)  // 0-9
			env.CreateCompletedDatarangeForAggregation(ctx, 10, 10) // 10-19

			// Start aggregation
			req := &dataranges.StartAggregateRequest{
				Datas3tName:         env.TestDatas3tName,
				FirstDatapointIndex: 0,
				LastDatapointIndex:  19,
			}

			var err error
			aggregateResp, err = env.UploadSrv.StartAggregate(ctx, env.Logger, req)
			Expect(err).NotTo(HaveOccurred())

			// Create aggregated tar and index data (simulating user concatenation)
			aggregatedTarData, aggregatedIndexData = CreateProperTarWithIndex(20, 0) // 20 files from 0-19
		})

		Context("when completing a valid aggregate upload", func() {
			It("should successfully complete direct PUT aggregate", func(ctx SpecContext) {
				// Upload aggregated data
				dataResp, err := HttpPut(aggregateResp.PresignedDataPutURL, bytes.NewReader(aggregatedTarData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Upload aggregated index
				indexResp, err := HttpPut(aggregateResp.PresignedIndexPutURL, bytes.NewReader(aggregatedIndexData))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Verify initial state - should have 2 original dataranges + 1 aggregate upload
				initialDatarangeCount, err := env.Queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(initialDatarangeCount).To(Equal(int64(2)))

				// Complete the aggregate
				completeReq := &dataranges.CompleteAggregateRequest{
					AggregateUploadID: aggregateResp.AggregateUploadID,
				}

				err = env.UploadSrv.CompleteAggregate(ctx, env.Logger, completeReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify final state - should have 1 aggregate datarange (original 2 deleted)
				finalDatarangeCount, err := env.Queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(finalDatarangeCount).To(Equal(int64(1)))

				// Verify the aggregate datarange covers the full range
				remainingDatarange, err := env.Queries.GetDatarangeByExactRange(ctx, postgresstore.GetDatarangeByExactRangeParams{
					Name:            env.TestDatas3tName,
					MinDatapointKey: 0,
					MaxDatapointKey: 19,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(remainingDatarange.MinDatapointKey).To(Equal(int64(0)))
				Expect(remainingDatarange.MaxDatapointKey).To(Equal(int64(19)))

				// Verify aggregate upload record was deleted
				_, err = env.Queries.GetAggregateUploadWithDetails(ctx, aggregateResp.AggregateUploadID)
				Expect(err).To(HaveOccurred())

				// Verify the aggregate data object exists in S3
				_, err = env.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(env.TestBucketName),
					Key:    aws.String(aggregateResp.ObjectKey),
				})
				Expect(err).NotTo(HaveOccurred())

				// Verify original datarange objects were scheduled for deletion
				cleanupTasks, err := env.Queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(BeNumerically(">=", 4)) // At least 4 objects (2 data + 2 index from original ranges)
			})
		})

		Context("when validation fails", func() {
			It("should reject aggregate with wrong size", func(ctx SpecContext) {
				// Upload wrong size data
				wrongSizeData := aggregatedTarData[:len(aggregatedTarData)-100]
				dataResp, err := HttpPut(aggregateResp.PresignedDataPutURL, bytes.NewReader(wrongSizeData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Upload correct index
				indexResp, err := HttpPut(aggregateResp.PresignedIndexPutURL, bytes.NewReader(aggregatedIndexData))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Try to complete - should fail
				completeReq := &dataranges.CompleteAggregateRequest{
					AggregateUploadID: aggregateResp.AggregateUploadID,
				}

				err = env.UploadSrv.CompleteAggregate(ctx, env.Logger, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("tar size mismatch"))

				// Verify original dataranges are still there (nothing changed)
				datarangeCount, err := env.Queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(2)))

				// Verify aggregate upload record was cleaned up on failure
				_, err = env.Queries.GetAggregateUploadWithDetails(ctx, aggregateResp.AggregateUploadID)
				Expect(err).To(HaveOccurred())
			})

			It("should reject aggregate with missing index", func(ctx SpecContext) {
				// Upload data but not index
				dataResp, err := HttpPut(aggregateResp.PresignedDataPutURL, bytes.NewReader(aggregatedTarData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Try to complete without uploading index - should fail
				completeReq := &dataranges.CompleteAggregateRequest{
					AggregateUploadID: aggregateResp.AggregateUploadID,
				}

				err = env.UploadSrv.CompleteAggregate(ctx, env.Logger, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("index file not found"))

				// Verify original dataranges are still there
				datarangeCount, err := env.Queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(2)))
			})

			It("should reject non-existent aggregate upload ID", func(ctx SpecContext) {
				completeReq := &dataranges.CompleteAggregateRequest{
					AggregateUploadID: 999999, // Non-existent ID
				}

				err := env.UploadSrv.CompleteAggregate(ctx, env.Logger, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to get aggregate upload details"))
			})
		})
	})

	Context("CancelAggregate", func() {
		var aggregateResp *dataranges.StartAggregateResponse

		BeforeEach(func(ctx SpecContext) {
			// Create source dataranges
			env.CreateCompletedDatarangeForAggregation(ctx, 0, 10)  // 0-9
			env.CreateCompletedDatarangeForAggregation(ctx, 10, 10) // 10-19

			// Start aggregation
			req := &dataranges.StartAggregateRequest{
				Datas3tName:         env.TestDatas3tName,
				FirstDatapointIndex: 0,
				LastDatapointIndex:  19,
			}

			var err error
			aggregateResp, err = env.UploadSrv.StartAggregate(ctx, env.Logger, req)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when cancelling a direct PUT aggregate", func() {
			It("should clean up without affecting original dataranges", func(ctx SpecContext) {
				// Verify initial state
				initialDatarangeCount, err := env.Queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(initialDatarangeCount).To(Equal(int64(2)))

				// Cancel the aggregate
				cancelReq := &dataranges.CancelAggregateRequest{
					AggregateUploadID: aggregateResp.AggregateUploadID,
				}

				err = env.UploadSrv.CancelAggregate(ctx, env.Logger, cancelReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify original dataranges are untouched
				finalDatarangeCount, err := env.Queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(finalDatarangeCount).To(Equal(int64(2)))

				// Verify aggregate upload record was deleted
				_, err = env.Queries.GetAggregateUploadWithDetails(ctx, aggregateResp.AggregateUploadID)
				Expect(err).To(HaveOccurred())
			})

			It("should clean up uploaded objects if they exist", func(ctx SpecContext) {
				// Upload some data first
				testData := make([]byte, 1024)
				for i := range testData {
					testData[i] = byte(i % 256)
				}

				dataResp, err := HttpPut(aggregateResp.PresignedDataPutURL, bytes.NewReader(testData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Verify object was uploaded
				_, err = env.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(env.TestBucketName),
					Key:    aws.String(aggregateResp.ObjectKey),
				})
				Expect(err).NotTo(HaveOccurred())

				// Cancel the aggregate
				cancelReq := &dataranges.CancelAggregateRequest{
					AggregateUploadID: aggregateResp.AggregateUploadID,
				}

				err = env.UploadSrv.CancelAggregate(ctx, env.Logger, cancelReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify uploaded object was deleted
				_, err = env.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(env.TestBucketName),
					Key:    aws.String(aggregateResp.ObjectKey),
				})
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when validation fails", func() {
			It("should reject non-existent aggregate upload ID", func(ctx SpecContext) {
				cancelReq := &dataranges.CancelAggregateRequest{
					AggregateUploadID: 999999, // Non-existent ID
				}

				err := env.UploadSrv.CancelAggregate(ctx, env.Logger, cancelReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to get aggregate upload details"))
			})
		})

		Context("when aggregate has already been cancelled", func() {
			BeforeEach(func(ctx SpecContext) {
				// Cancel it once
				cancelReq := &dataranges.CancelAggregateRequest{
					AggregateUploadID: aggregateResp.AggregateUploadID,
				}
				err := env.UploadSrv.CancelAggregate(ctx, env.Logger, cancelReq)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should return error when trying to cancel again", func(ctx SpecContext) {
				// Try to cancel again
				cancelReq := &dataranges.CancelAggregateRequest{
					AggregateUploadID: aggregateResp.AggregateUploadID,
				}

				err := env.UploadSrv.CancelAggregate(ctx, env.Logger, cancelReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to get aggregate upload details"))
			})
		})
	})

	Context("End-to-End Aggregate Workflow", func() {
		It("should successfully aggregate multiple dataranges into one", func(ctx SpecContext) {
			// Create 4 small dataranges
			env.CreateCompletedDatarangeForAggregation(ctx, 0, 5)  // 0-4
			env.CreateCompletedDatarangeForAggregation(ctx, 5, 5)  // 5-9
			env.CreateCompletedDatarangeForAggregation(ctx, 10, 5) // 10-14
			env.CreateCompletedDatarangeForAggregation(ctx, 15, 5) // 15-19

			// Verify initial state
			initialCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(initialCount).To(Equal(int64(4)))

			// Start aggregation of first 3 dataranges (0-14)
			startReq := &dataranges.StartAggregateRequest{
				Datas3tName:         env.TestDatas3tName,
				FirstDatapointIndex: 0,
				LastDatapointIndex:  14,
			}

			startResp, err := env.UploadSrv.StartAggregate(ctx, env.Logger, startReq)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(startResp.SourceDatarangeDownloadURLs)).To(Equal(3))

			// Create and upload aggregated data
			aggregatedTarData, aggregatedIndexData := CreateProperTarWithIndex(15, 0) // 15 files from 0-14

			dataResp, err := HttpPut(startResp.PresignedDataPutURL, bytes.NewReader(aggregatedTarData))
			Expect(err).NotTo(HaveOccurred())
			Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
			dataResp.Body.Close()

			indexResp, err := HttpPut(startResp.PresignedIndexPutURL, bytes.NewReader(aggregatedIndexData))
			Expect(err).NotTo(HaveOccurred())
			Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
			indexResp.Body.Close()

			// Complete aggregation
			completeReq := &dataranges.CompleteAggregateRequest{
				AggregateUploadID: startResp.AggregateUploadID,
			}

			err = env.UploadSrv.CompleteAggregate(ctx, env.Logger, completeReq)
			Expect(err).NotTo(HaveOccurred())

			// Verify final state: should have 2 dataranges (1 aggregate + 1 original)
			finalCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalCount).To(Equal(int64(2)))

			// Verify the ranges are correct
			allDataranges, err := env.Queries.GetAllDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(allDataranges)).To(Equal(2))

			var foundAggregateRange, foundOriginalRange bool
			for _, dr := range allDataranges {
				if dr.MinDatapointKey == 0 && dr.MaxDatapointKey == 14 {
					foundAggregateRange = true
				} else if dr.MinDatapointKey == 15 && dr.MaxDatapointKey == 19 {
					foundOriginalRange = true
				}
			}
			Expect(foundAggregateRange).To(BeTrue())
			Expect(foundOriginalRange).To(BeTrue())
		})
	})
})
