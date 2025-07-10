package dataranges_test

import (
	"bytes"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t/server/dataranges"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("CancelDatarangeUpload", func() {
	var env *TestEnvironment

	BeforeEach(func(ctx SpecContext) {
		env = SetupTestEnvironment(ctx)
	})

	AfterEach(func(ctx SpecContext) {
		env.TeardownTestEnvironment(ctx)
	})

	Context("when cancelling a direct PUT upload", func() {
		var uploadResp *dataranges.UploadDatarangeResponse

		BeforeEach(func(ctx SpecContext) {
			// Start a small upload that uses direct PUT
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         env.TestDatas3tName,
				DataSize:            1024, // Small size < 5MB
				NumberOfDatapoints:  10,
				FirstDatapointIndex: 0,
			}

			var err error
			uploadResp, err = env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploadResp.UseDirectPut).To(BeTrue())
		})

		It("should successfully cancel upload and clean up upload records", func(ctx SpecContext) {
			// Verify initial state
			uploadCount, err := env.Queries.CountDatarangeUploads(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploadCount).To(Equal(int64(1)))

			datarangeCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount).To(Equal(int64(0))) // No datarange created yet

			// Cancel the upload
			cancelReq := &dataranges.CancelUploadRequest{
				DatarangeUploadID: uploadResp.DatarangeID,
			}

			err = env.UploadSrv.CancelDatarangeUpload(ctx, env.Logger, cancelReq)
			Expect(err).NotTo(HaveOccurred())

			// Verify upload record was deleted
			uploadCount2, err := env.Queries.CountDatarangeUploads(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploadCount2).To(Equal(int64(0)))

			// Verify no datarange record exists (never created)
			datarangeCount2, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount2).To(Equal(int64(0)))

			// Verify no cleanup tasks needed (objects didn't exist, so immediate deletion succeeded)
			cleanupTasks, err := env.Queries.CountKeysToDelete(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(cleanupTasks).To(Equal(int64(0)))
		})

		It("should delete uploaded data object and index when cancelling", func(ctx SpecContext) {
			// Upload both data and index to S3
			testData := make([]byte, 1024)
			for i := range testData {
				testData[i] = byte(i % 256)
			}
			testIndex := []byte("test index data for cancel direct PUT")

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

			// Get the actual index object key from the database
			uploadDetails, err := env.Queries.GetDatarangeUploadWithDetails(ctx, uploadResp.DatarangeID)
			Expect(err).NotTo(HaveOccurred())

			// Verify both objects were uploaded
			_, err = env.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(env.TestBucketName),
				Key:    aws.String(uploadResp.ObjectKey),
			})
			Expect(err).NotTo(HaveOccurred()) // Data object should exist

			_, err = env.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(env.TestBucketName),
				Key:    aws.String(uploadDetails.IndexObjectKey),
			})
			Expect(err).NotTo(HaveOccurred()) // Index should exist

			// Cancel the upload
			cancelReq := &dataranges.CancelUploadRequest{
				DatarangeUploadID: uploadResp.DatarangeID,
			}

			err = env.UploadSrv.CancelDatarangeUpload(ctx, env.Logger, cancelReq)
			Expect(err).NotTo(HaveOccurred())

			// Verify database cleanup
			uploadCount, err := env.Queries.CountDatarangeUploads(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploadCount).To(Equal(int64(0)))

			// Verify data object was deleted
			_, err = env.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(env.TestBucketName),
				Key:    aws.String(uploadResp.ObjectKey),
			})
			Expect(err).To(HaveOccurred()) // Data object should be deleted

			// Verify index was deleted
			_, err = env.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(env.TestBucketName),
				Key:    aws.String(uploadDetails.IndexObjectKey),
			})
			Expect(err).To(HaveOccurred()) // Index should be deleted

			// Verify no cleanup tasks needed (immediate deletion succeeded)
			cleanupTasks, err := env.Queries.CountKeysToDelete(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(cleanupTasks).To(Equal(int64(0)))
		})
	})

	Context("when cancelling a multipart upload", func() {
		var uploadResp *dataranges.UploadDatarangeResponse

		BeforeEach(func(ctx SpecContext) {
			// Start a large upload that requires multipart
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         env.TestDatas3tName,
				DataSize:            25 * 1024 * 1024, // 25MB
				NumberOfDatapoints:  1000,
				FirstDatapointIndex: 100,
			}

			var err error
			uploadResp, err = env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploadResp.UseDirectPut).To(BeFalse())
		})

		It("should successfully cancel multipart upload and clean up upload records", func(ctx SpecContext) {
			// Verify initial state
			uploadCount, err := env.Queries.CountDatarangeUploads(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploadCount).To(Equal(int64(1)))

			datarangeCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount).To(Equal(int64(0))) // No datarange created yet

			// Cancel the upload
			cancelReq := &dataranges.CancelUploadRequest{
				DatarangeUploadID: uploadResp.DatarangeID,
			}

			err = env.UploadSrv.CancelDatarangeUpload(ctx, env.Logger, cancelReq)
			Expect(err).NotTo(HaveOccurred())

			// Verify upload record was deleted
			uploadCount2, err := env.Queries.CountDatarangeUploads(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploadCount2).To(Equal(int64(0)))

			// Verify no datarange record exists (never created)
			datarangeCount2, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount2).To(Equal(int64(0)))

			// Verify no cleanup tasks needed (objects didn't exist, so immediate deletion succeeded)
			cleanupTasks, err := env.Queries.CountKeysToDelete(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(cleanupTasks).To(Equal(int64(0)))
		})

		It("should handle partial uploads by cancelling and cleaning up properly", func(ctx SpecContext) {
			// Upload one part to simulate partial upload
			testData := make([]byte, 5*1024*1024) // 5MB for first part
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			resp, err := HttpPut(uploadResp.PresignedMultipartUploadPutURLs[0], bytes.NewReader(testData))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			resp.Body.Close()

			// Cancel the upload (should abort multipart and clean up)
			cancelReq := &dataranges.CancelUploadRequest{
				DatarangeUploadID: uploadResp.DatarangeID,
			}

			err = env.UploadSrv.CancelDatarangeUpload(ctx, env.Logger, cancelReq)
			Expect(err).NotTo(HaveOccurred())

			// Verify database cleanup
			uploadCount, err := env.Queries.CountDatarangeUploads(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploadCount).To(Equal(int64(0)))

			datarangeCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount).To(Equal(int64(0)))

			// Verify no cleanup tasks needed (multipart abort cleaned up parts, objects didn't exist)
			cleanupTasks, err := env.Queries.CountKeysToDelete(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(cleanupTasks).To(Equal(int64(0)))
		})

		It("should delete uploaded multipart parts and index when cancelling", func(ctx SpecContext) {
			// Upload both parts and index to S3
			testData := make([]byte, 10*1024*1024) // 10MB total
			for i := range testData {
				testData[i] = byte(i % 256)
			}
			testIndex := []byte("test index data for cancel multipart")

			// Upload both multipart parts
			partSize := 5 * 1024 * 1024 // 5MB per part
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
				resp.Body.Close()
			}

			// Upload index file
			indexResp, err := HttpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
			Expect(err).NotTo(HaveOccurred())
			Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
			indexResp.Body.Close()

			// Get the actual index object key from the database
			uploadDetails, err := env.Queries.GetDatarangeUploadWithDetails(ctx, uploadResp.DatarangeID)
			Expect(err).NotTo(HaveOccurred())

			// Verify index was uploaded
			_, err = env.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(env.TestBucketName),
				Key:    aws.String(uploadDetails.IndexObjectKey),
			})
			Expect(err).NotTo(HaveOccurred()) // Index should exist

			// Cancel the upload
			cancelReq := &dataranges.CancelUploadRequest{
				DatarangeUploadID: uploadResp.DatarangeID,
			}

			err = env.UploadSrv.CancelDatarangeUpload(ctx, env.Logger, cancelReq)
			Expect(err).NotTo(HaveOccurred())

			// Verify database cleanup
			uploadCount, err := env.Queries.CountDatarangeUploads(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploadCount).To(Equal(int64(0)))

			// Verify multipart upload was aborted (data object should not exist)
			_, err = env.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(env.TestBucketName),
				Key:    aws.String(uploadResp.ObjectKey),
			})
			Expect(err).To(HaveOccurred()) // Data object should not exist after abort

			// Verify index was deleted
			_, err = env.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(env.TestBucketName),
				Key:    aws.String(uploadDetails.IndexObjectKey),
			})
			Expect(err).To(HaveOccurred()) // Index should be deleted

			// Verify no cleanup tasks needed (immediate deletion succeeded)
			cleanupTasks, err := env.Queries.CountKeysToDelete(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(cleanupTasks).To(Equal(int64(0)))
		})
	})

	Context("when validation fails", func() {
		It("should reject non-existent upload ID", func(ctx SpecContext) {
			cancelReq := &dataranges.CancelUploadRequest{
				DatarangeUploadID: 999999, // Non-existent ID
			}

			err := env.UploadSrv.CancelDatarangeUpload(ctx, env.Logger, cancelReq)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get datarange upload details"))
		})
	})

	Context("when upload has already been cancelled", func() {
		var uploadResp *dataranges.UploadDatarangeResponse

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

			// Cancel it once
			cancelReq := &dataranges.CancelUploadRequest{
				DatarangeUploadID: uploadResp.DatarangeID,
			}
			err = env.UploadSrv.CancelDatarangeUpload(ctx, env.Logger, cancelReq)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should return error when trying to cancel again", func(ctx SpecContext) {
			// Try to cancel again
			cancelReq := &dataranges.CancelUploadRequest{
				DatarangeUploadID: uploadResp.DatarangeID,
			}

			err := env.UploadSrv.CancelDatarangeUpload(ctx, env.Logger, cancelReq)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get datarange upload details"))
		})
	})
})
