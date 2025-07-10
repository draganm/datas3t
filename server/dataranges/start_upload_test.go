package dataranges_test

import (
	"github.com/draganm/datas3t/server/dataranges"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("StartDatarangeUpload", func() {
	var env *TestEnvironment

	BeforeEach(func(ctx SpecContext) {
		env = SetupTestEnvironment(ctx)
	})

	AfterEach(func(ctx SpecContext) {
		env.TeardownTestEnvironment(ctx)
	})

	Context("when starting a valid small upload (direct PUT)", func() {
		It("should successfully create upload with direct PUT URLs", func(ctx SpecContext) {
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         env.TestDatas3tName,
				DataSize:            1024, // Small size < 5MB
				NumberOfDatapoints:  10,
				FirstDatapointIndex: 0,
			}

			resp, err := env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp).NotTo(BeNil())
			Expect(resp.UseDirectPut).To(BeTrue())
			Expect(resp.PresignedDataPutURL).NotTo(BeEmpty())
			Expect(resp.PresignedIndexPutURL).NotTo(BeEmpty())
			Expect(resp.PresignedMultipartUploadPutURLs).To(BeEmpty())
			Expect(resp.DatarangeID).To(BeNumerically(">", 0))
			Expect(resp.FirstDatapointIndex).To(Equal(uint64(0)))

			// Verify database state - no datarange record yet (created on completion)
			datarangeCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount).To(Equal(int64(0)))

			// Verify upload record
			uploads, err := env.Queries.GetAllDatarangeUploads(ctx)
			Expect(err).NotTo(HaveOccurred())

			var uploadCount int
			for _, upload := range uploads {
				uploadCount++
				Expect(upload.UploadID).To(Equal("DIRECT_PUT"))
				Expect(upload.FirstDatapointIndex).To(Equal(int64(0)))
				Expect(upload.NumberOfDatapoints).To(Equal(int64(10)))
				Expect(upload.DataSize).To(Equal(int64(1024)))
			}
			Expect(uploadCount).To(Equal(1))
		})
	})

	Context("when starting a valid large upload (multipart)", func() {
		It("should successfully create upload with multipart URLs", func(ctx SpecContext) {
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         env.TestDatas3tName,
				DataSize:            25 * 1024 * 1024, // 25MB > 20MB threshold
				NumberOfDatapoints:  1000,
				FirstDatapointIndex: 100,
			}

			resp, err := env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp).NotTo(BeNil())
			Expect(resp.UseDirectPut).To(BeFalse())
			Expect(resp.PresignedDataPutURL).To(BeEmpty())
			Expect(resp.PresignedIndexPutURL).NotTo(BeEmpty())
			Expect(resp.PresignedMultipartUploadPutURLs).NotTo(BeEmpty())
			Expect(len(resp.PresignedMultipartUploadPutURLs)).To(Equal(2)) // 25MB / 20MB = 2 parts (rounded up)
			Expect(resp.DatarangeID).To(BeNumerically(">", 0))
			Expect(resp.FirstDatapointIndex).To(Equal(uint64(100)))

			// Verify database state - no datarange record yet (created on completion)
			datarangeCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount).To(Equal(int64(0)))

			// Verify upload record
			uploadIDs, err := env.Queries.GetDatarangeUploadIDs(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(len(uploadIDs)).To(Equal(1))
			uploadID := uploadIDs[0]
			Expect(uploadID).NotTo(Equal("DIRECT_PUT"))
			Expect(uploadID).NotTo(BeEmpty())
		})
	})

	Context("when validation fails", func() {
		It("should reject empty datas3t name", func(ctx SpecContext) {
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         "",
				DataSize:            1024,
				NumberOfDatapoints:  10,
				FirstDatapointIndex: 0,
			}

			_, err := env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("datas3t_name is required"))

			// Verify no database changes
			datarangeCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount).To(Equal(int64(0)))
		})

		It("should reject zero data size", func(ctx SpecContext) {
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         env.TestDatas3tName,
				DataSize:            0,
				NumberOfDatapoints:  10,
				FirstDatapointIndex: 0,
			}

			_, err := env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("data_size must be greater than 0"))

			// Verify no database changes
			datarangeCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount).To(Equal(int64(0)))
		})

		It("should reject zero number of datapoints", func(ctx SpecContext) {
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         env.TestDatas3tName,
				DataSize:            1024,
				NumberOfDatapoints:  0,
				FirstDatapointIndex: 0,
			}

			_, err := env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("number_of_datapoints must be greater than 0"))

			// Verify no database changes
			datarangeCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount).To(Equal(int64(0)))
		})

		It("should reject non-existent datas3t", func(ctx SpecContext) {
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         "non-existent-datas3t",
				DataSize:            1024,
				NumberOfDatapoints:  10,
				FirstDatapointIndex: 0,
			}

			_, err := env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to find datas3t 'non-existent-datas3t'"))

			// Verify no database changes
			datarangeCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount).To(Equal(int64(0)))
		})
	})

	Context("when handling overlapping dataranges", func() {
		BeforeEach(func(ctx SpecContext) {
			// Create an existing datarange from 0-99
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         env.TestDatas3tName,
				DataSize:            1024,
				NumberOfDatapoints:  100,
				FirstDatapointIndex: 0,
			}

			_, err := env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should allow overlapping upload ranges", func(ctx SpecContext) {
			// Try to create overlapping range 50-149 (should now be allowed)
			// Note: Ranges 0-99 (from BeforeEach) and 50-149 overlap from 50-99
			// This is allowed during upload start - disambiguation happens at completion
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         env.TestDatas3tName,
				DataSize:            1024,
				NumberOfDatapoints:  100,
				FirstDatapointIndex: 50,
			}

			_, err := env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).NotTo(HaveOccurred()) // Should succeed now

			// Verify both upload records exist (overlapping uploads are allowed)
			// Only the first one to complete will succeed; others will fail at completion
			uploadCount, err := env.Queries.CountDatarangeUploads(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploadCount).To(Equal(int64(2)))
		})

		It("should allow adjacent ranges", func(ctx SpecContext) {
			// Create adjacent range 100-199 (no overlap)
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         env.TestDatas3tName,
				DataSize:            1024,
				NumberOfDatapoints:  100,
				FirstDatapointIndex: 100,
			}

			_, err := env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
			Expect(err).NotTo(HaveOccurred())

			// Verify two upload records exist
			uploadCount, err := env.Queries.CountDatarangeUploads(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploadCount).To(Equal(int64(2)))
		})
	})
})
