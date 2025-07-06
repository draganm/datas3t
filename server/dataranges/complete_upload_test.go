package dataranges_test

import (
	"bytes"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t/server/dataranges"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("CompleteUpload", func() {
	var env *TestEnvironment

	BeforeEach(func(ctx SpecContext) {
		env = SetupTestEnvironment(ctx)
	})

	AfterEach(func(ctx SpecContext) {
		env.TeardownTestEnvironment(ctx)
	})

	Context("Direct PUT upload completion", func() {
		var uploadResp *dataranges.UploadDatarangeResponse
		var testData []byte
		var testIndex []byte

		BeforeEach(func(ctx SpecContext) {
			// Start an upload
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         env.TestDatas3tName,
				DataSize:            1024,
				NumberOfDatapoints:  10,
				FirstDatapointIndex: 0,
			}

			var err error
			uploadResp, err = env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).NotTo(HaveOccurred())

			// Prepare test data
			testData = make([]byte, 1024)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			testIndex = []byte("test index data")
		})

		Context("when completing a successful direct PUT upload", func() {
			It("should complete successfully with both files uploaded", func(ctx SpecContext) {
				// Upload data file
				dataResp, err := HttpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(testData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Upload index file
				indexResp, err := HttpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload
				completeReq := &dataranges.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = env.UploadSrv.CompleteDatarangeUpload(ctx, env.Logger, completeReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify upload record was deleted
				uploadCount, err := env.Queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify datarange record still exists
				datarangeCount, err := env.Queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(1)))
			})
		})

		Context("when index file is missing", func() {
			It("should fail and schedule cleanup", func(ctx SpecContext) {
				// Upload only data file (no index)
				dataResp, err := HttpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(testData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Complete the upload (should fail)
				completeReq := &dataranges.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = env.UploadSrv.CompleteDatarangeUpload(ctx, env.Logger, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("index file not found"))

				// Verify cleanup happened - both upload and datarange records should be deleted
				uploadCount, err := env.Queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				datarangeCount, err := env.Queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0)))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := env.Queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(2))) // Both data and index objects scheduled for deletion
			})
		})

		Context("when data file is missing", func() {
			It("should fail and schedule cleanup", func(ctx SpecContext) {
				// Upload only index file (no data)
				indexResp, err := HttpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload (should fail)
				completeReq := &dataranges.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = env.UploadSrv.CompleteDatarangeUpload(ctx, env.Logger, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to get uploaded object info"))

				// Verify cleanup happened
				uploadCount, err := env.Queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := env.Queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(2)))
			})
		})

		Context("when data size is wrong", func() {
			It("should fail and schedule cleanup", func(ctx SpecContext) {
				// Upload wrong size data
				wrongSizeData := make([]byte, 512) // Expected 1024, uploading 512
				for i := range wrongSizeData {
					wrongSizeData[i] = byte(i % 256)
				}

				dataResp, err := HttpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(wrongSizeData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Upload index file
				indexResp, err := HttpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload (should fail)
				completeReq := &dataranges.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = env.UploadSrv.CompleteDatarangeUpload(ctx, env.Logger, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("uploaded size mismatch"))
				Expect(err.Error()).To(ContainSubstring("expected 1024, got 512"))

				// Verify cleanup happened
				uploadCount, err := env.Queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := env.Queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(2)))
			})
		})
	})

	Context("Multipart Upload Complete", func() {
		var uploadResp *dataranges.UploadDatarangeResponse
		var testData []byte
		var testIndex []byte

		BeforeEach(func(ctx SpecContext) {
			// Start a large upload that requires multipart
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         env.TestDatas3tName,
				DataSize:            10 * 1024 * 1024, // 10MB
				NumberOfDatapoints:  1000,
				FirstDatapointIndex: 0,
			}

			var err error
			uploadResp, err = env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploadResp.UseDirectPut).To(BeFalse())

			// Prepare test data
			testData = make([]byte, 10*1024*1024)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			testIndex = []byte("test index data for multipart")
		})

		Context("when completing a successful multipart upload", func() {
			It("should complete successfully with all parts uploaded", func(ctx SpecContext) {
				// Upload all parts
				partSize := 5 * 1024 * 1024 // 5MB per part
				var etags []string

				for i, url := range uploadResp.PresignedMultipartUploadPutURLs {
					startOffset := i * partSize
					endOffset := startOffset + partSize
					if endOffset > len(testData) {
						endOffset = len(testData)
					}

					partData := testData[startOffset:endOffset]
					resp, err := HttpPut(url, bytes.NewReader(partData))
					Expect(err).NotTo(HaveOccurred())
					Expect(resp.StatusCode).To(Equal(http.StatusOK))

					// Get ETag from response
					etag := resp.Header.Get("ETag")
					Expect(etag).NotTo(BeEmpty())
					etags = append(etags, etag)
					resp.Body.Close()
				}

				// Upload index file
				indexResp, err := HttpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload
				completeReq := &dataranges.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
					UploadIDs:         etags,
				}

				err = env.UploadSrv.CompleteDatarangeUpload(ctx, env.Logger, completeReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify upload record was deleted
				uploadCount, err := env.Queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify datarange record still exists
				datarangeCount, err := env.Queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(1)))

				// Verify the file was actually uploaded and accessible
				getResp, err := env.S3Client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(env.TestBucketName),
					Key:    aws.String(uploadResp.ObjectKey),
				})
				Expect(err).NotTo(HaveOccurred())
				defer getResp.Body.Close()

				downloadedData, err := io.ReadAll(getResp.Body)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(downloadedData)).To(Equal(len(testData)))
			})
		})

		Context("when multipart upload fails due to missing parts", func() {
			It("should fail to complete with incomplete parts", func(ctx SpecContext) {
				// Upload only the first part (missing second part)
				partSize := 5 * 1024 * 1024 // 5MB per part
				partData := testData[:partSize]

				resp, err := HttpPut(uploadResp.PresignedMultipartUploadPutURLs[0], bytes.NewReader(partData))
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(http.StatusOK))

				etag := resp.Header.Get("ETag")
				Expect(etag).NotTo(BeEmpty())
				resp.Body.Close()

				// Upload index file
				indexResp, err := HttpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Try to complete with only one ETag (should fail)
				completeReq := &dataranges.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
					UploadIDs:         []string{etag}, // Missing second part
				}

				err = env.UploadSrv.CompleteDatarangeUpload(ctx, env.Logger, completeReq)
				Expect(err).To(HaveOccurred())
				// When only partial data is uploaded, it fails with size mismatch before multipart completion
				Expect(err.Error()).To(ContainSubstring("uploaded size mismatch"))

				// Verify records were cleaned up (both upload and datarange should be deleted on failure)
				uploadCount, err := env.Queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := env.Queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(2)))
			})
		})
	})
})
