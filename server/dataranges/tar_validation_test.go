package dataranges_test

import (
	"bytes"
	"net/http"

	"github.com/draganm/datas3t/server/dataranges"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tar Index Validation", func() {
	var env *TestEnvironment

	BeforeEach(func(ctx SpecContext) {
		env = SetupTestEnvironment(ctx)
	})

	AfterEach(func(ctx SpecContext) {
		env.TeardownTestEnvironment(ctx)
	})

	Context("when tar validation succeeds", func() {
		var uploadResp *dataranges.UploadDatarangeResponse
		var properTarData []byte
		var properTarIndex []byte

		BeforeEach(func(ctx SpecContext) {
			// Create a proper TAR archive with correctly named files
			properTarData, properTarIndex = CreateProperTarWithIndex(5, 0) // 5 files starting from index 0

			// Start an upload with the correct size
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         env.TestDatas3tName,
				DataSize:            uint64(len(properTarData)),
				NumberOfDatapoints:  5,
				FirstDatapointIndex: 0,
			}

			var err error
			uploadResp, err = env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should validate proper tar files with correct index", func(ctx SpecContext) {
			// Upload proper tar data
			dataResp, err := HttpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(properTarData))
			Expect(err).NotTo(HaveOccurred())
			Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
			dataResp.Body.Close()

			// Upload proper tar index
			indexResp, err := HttpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(properTarIndex))
			Expect(err).NotTo(HaveOccurred())
			Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
			indexResp.Body.Close()

			// Complete the upload - should succeed with validation
			completeReq := &dataranges.CompleteUploadRequest{
				DatarangeUploadID: uploadResp.DatarangeID,
			}

			err = env.UploadSrv.CompleteDatarangeUpload(ctx, env.Logger, completeReq)
			Expect(err).NotTo(HaveOccurred())

			// Verify upload completed successfully
			uploadCount, err := env.Queries.CountDatarangeUploads(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploadCount).To(Equal(int64(0)))
		})
	})

	Context("when tar validation fails due to size mismatch", func() {
		var uploadResp *dataranges.UploadDatarangeResponse
		var properTarData []byte
		var properTarIndex []byte

		BeforeEach(func(ctx SpecContext) {
			// Create a proper TAR archive with correctly named files
			properTarData, properTarIndex = CreateProperTarWithIndex(5, 0) // 5 files starting from index 0

			// Start an upload with the correct size
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         env.TestDatas3tName,
				DataSize:            uint64(len(properTarData)),
				NumberOfDatapoints:  5,
				FirstDatapointIndex: 0,
			}

			var err error
			uploadResp, err = env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reject tar with incorrect size", func(ctx SpecContext) {
			// Create tar data with wrong size (truncate it)
			wrongSizeTarData := properTarData[:len(properTarData)-100]

			// Upload wrong size tar data
			dataResp, err := HttpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(wrongSizeTarData))
			Expect(err).NotTo(HaveOccurred())
			Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
			dataResp.Body.Close()

			// Upload proper tar index (which will now be inconsistent with data)
			indexResp, err := HttpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(properTarIndex))
			Expect(err).NotTo(HaveOccurred())
			Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
			indexResp.Body.Close()

			// Complete the upload - should fail during validation
			completeReq := &dataranges.CompleteUploadRequest{
				DatarangeUploadID: uploadResp.DatarangeID,
			}

			err = env.UploadSrv.CompleteDatarangeUpload(ctx, env.Logger, completeReq)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("uploaded size mismatch"))
		})
	})

	Context("when tar validation fails due to invalid file names", func() {
		It("should reject tar with incorrectly named files", func(ctx SpecContext) {
			// Create tar with wrong file names
			invalidTarData, invalidTarIndex := CreateTarWithInvalidNames()

			// Start a new upload with correct size
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         env.TestDatas3tName,
				DataSize:            uint64(len(invalidTarData)),
				NumberOfDatapoints:  3,
				FirstDatapointIndex: 0,
			}

			uploadResp, err := env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).NotTo(HaveOccurred())

			// Upload invalid tar data
			dataResp, err := HttpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(invalidTarData))
			Expect(err).NotTo(HaveOccurred())
			Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
			dataResp.Body.Close()

			// Upload invalid tar index
			indexResp, err := HttpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(invalidTarIndex))
			Expect(err).NotTo(HaveOccurred())
			Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
			indexResp.Body.Close()

			// Complete the upload - should fail during validation
			completeReq := &dataranges.CompleteUploadRequest{
				DatarangeUploadID: uploadResp.DatarangeID,
			}

			err = env.UploadSrv.CompleteDatarangeUpload(ctx, env.Logger, completeReq)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("tar index validation failed"))
			Expect(err.Error()).To(ContainSubstring("invalid file name format"))
		})
	})

	Context("tar validation with multipart uploads", func() {
		It("should validate large tar files uploaded via multipart", func(ctx SpecContext) {
			// Create a large tar archive
			largeTarData, largeTarIndex := CreateProperTarWithIndex(1000, 0) // Large number of files

			// Start a large upload that requires multipart
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         env.TestDatas3tName,
				DataSize:            uint64(len(largeTarData)),
				NumberOfDatapoints:  1000,
				FirstDatapointIndex: 0,
			}

			uploadResp, err := env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).NotTo(HaveOccurred())

			// Upload all parts if it's multipart
			var etags []string
			if !uploadResp.UseDirectPut {
				partSize := 5 * 1024 * 1024 // 5MB per part
				for i, url := range uploadResp.PresignedMultipartUploadPutURLs {
					startOffset := i * partSize
					endOffset := startOffset + partSize
					if endOffset > len(largeTarData) {
						endOffset = len(largeTarData)
					}

					partData := largeTarData[startOffset:endOffset]
					resp, err := HttpPut(url, bytes.NewReader(partData))
					Expect(err).NotTo(HaveOccurred())
					Expect(resp.StatusCode).To(Equal(http.StatusOK))

					etag := resp.Header.Get("ETag")
					Expect(etag).NotTo(BeEmpty())
					etags = append(etags, etag)
					resp.Body.Close()
				}
			} else {
				// Direct PUT for smaller files
				dataResp, err := HttpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(largeTarData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()
			}

			// Upload index
			indexResp, err := HttpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(largeTarIndex))
			Expect(err).NotTo(HaveOccurred())
			Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
			indexResp.Body.Close()

			// Complete the upload - should succeed with validation
			completeReq := &dataranges.CompleteUploadRequest{
				DatarangeUploadID: uploadResp.DatarangeID,
				UploadIDs:         etags,
			}

			err = env.UploadSrv.CompleteDatarangeUpload(ctx, env.Logger, completeReq)
			Expect(err).NotTo(HaveOccurred())

			// Verify upload completed successfully
			uploadCount, err := env.Queries.CountDatarangeUploads(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploadCount).To(Equal(int64(0)))

			// Verify datarange was created
			datarangeCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount).To(Equal(int64(1)))
		})
	})
})
