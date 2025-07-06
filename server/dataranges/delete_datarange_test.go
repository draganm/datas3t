package dataranges_test

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t/server/dataranges"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("DeleteDatarange", func() {
	var env *TestEnvironment

	BeforeEach(func(ctx SpecContext) {
		env = SetupTestEnvironment(ctx)
	})

	AfterEach(func(ctx SpecContext) {
		env.TeardownTestEnvironment(ctx)
	})

	Context("when deleting an existing datarange", func() {
		var testDataObjectKey string
		var testIndexObjectKey string

		BeforeEach(func(ctx SpecContext) {
			// Create a completed datarange for testing deletion
			testDataObjectKey, testIndexObjectKey = env.CreateCompletedDatarange(ctx, 0, 10) // Create datarange from 0-9
		})

		It("should successfully delete the datarange and S3 objects", func(ctx SpecContext) {
			// Verify initial state
			datarangeCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount).To(Equal(int64(1)))

			// Verify S3 objects exist
			_, err = env.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(env.TestBucketName),
				Key:    aws.String(testDataObjectKey),
			})
			Expect(err).NotTo(HaveOccurred()) // Data object should exist

			_, err = env.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(env.TestBucketName),
				Key:    aws.String(testIndexObjectKey),
			})
			Expect(err).NotTo(HaveOccurred()) // Index object should exist

			// Delete the datarange
			deleteReq := &dataranges.DeleteDatarangeRequest{
				Datas3tName:       env.TestDatas3tName,
				FirstDatapointKey: 0,
				LastDatapointKey:  9,
			}

			err = env.UploadSrv.DeleteDatarange(ctx, env.Logger, deleteReq)
			Expect(err).NotTo(HaveOccurred())

			// Verify datarange was deleted from database
			datarangeCount2, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount2).To(Equal(int64(0)))

			// Verify S3 objects were deleted
			_, err = env.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(env.TestBucketName),
				Key:    aws.String(testDataObjectKey),
			})
			Expect(err).To(HaveOccurred()) // Data object should be deleted

			_, err = env.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(env.TestBucketName),
				Key:    aws.String(testIndexObjectKey),
			})
			Expect(err).To(HaveOccurred()) // Index object should be deleted

			// Verify no cleanup tasks were scheduled (immediate deletion succeeded)
			cleanupTasks, err := env.Queries.CountKeysToDelete(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(cleanupTasks).To(Equal(int64(0)))
		})
	})

	Context("when validation fails", func() {
		It("should reject empty datas3t name", func(ctx SpecContext) {
			deleteReq := &dataranges.DeleteDatarangeRequest{
				Datas3tName:       "",
				FirstDatapointKey: 0,
				LastDatapointKey:  9,
			}

			err := env.UploadSrv.DeleteDatarange(ctx, env.Logger, deleteReq)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("datas3t_name is required"))
		})

		It("should reject invalid datapoint key range", func(ctx SpecContext) {
			deleteReq := &dataranges.DeleteDatarangeRequest{
				Datas3tName:       env.TestDatas3tName,
				FirstDatapointKey: 10,
				LastDatapointKey:  5, // Last < First
			}

			err := env.UploadSrv.DeleteDatarange(ctx, env.Logger, deleteReq)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("last_datapoint_key must be greater than or equal to first_datapoint_key"))
		})

		It("should reject non-existent datas3t", func(ctx SpecContext) {
			deleteReq := &dataranges.DeleteDatarangeRequest{
				Datas3tName:       "non-existent-datas3t",
				FirstDatapointKey: 0,
				LastDatapointKey:  9,
			}

			err := env.UploadSrv.DeleteDatarange(ctx, env.Logger, deleteReq)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to find datarange"))
		})

		It("should reject non-existent datarange", func(ctx SpecContext) {
			deleteReq := &dataranges.DeleteDatarangeRequest{
				Datas3tName:       env.TestDatas3tName,
				FirstDatapointKey: 100, // Non-existent range
				LastDatapointKey:  199,
			}

			err := env.UploadSrv.DeleteDatarange(ctx, env.Logger, deleteReq)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to find datarange"))
		})
	})

	Context("when datarange exists but S3 objects don't exist", func() {
		var testDataObjectKey string
		var testIndexObjectKey string

		BeforeEach(func(ctx SpecContext) {
			// Create a completed datarange for testing deletion
			testDataObjectKey, testIndexObjectKey = env.CreateCompletedDatarange(ctx, 0, 10) // Create datarange from 0-9

			// Manually delete the S3 objects to simulate a scenario where they don't exist
			_, err := env.S3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(env.TestBucketName),
				Key:    aws.String(testDataObjectKey),
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = env.S3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(env.TestBucketName),
				Key:    aws.String(testIndexObjectKey),
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should successfully delete the datarange even when S3 objects don't exist", func(ctx SpecContext) {
			// Verify initial state
			datarangeCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount).To(Equal(int64(1)))

			// Delete the datarange
			deleteReq := &dataranges.DeleteDatarangeRequest{
				Datas3tName:       env.TestDatas3tName,
				FirstDatapointKey: 0,
				LastDatapointKey:  9,
			}

			err = env.UploadSrv.DeleteDatarange(ctx, env.Logger, deleteReq)
			Expect(err).NotTo(HaveOccurred())

			// Verify datarange was deleted from database
			datarangeCount2, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount2).To(Equal(int64(0)))

			// Verify no cleanup tasks were scheduled (objects didn't exist)
			cleanupTasks, err := env.Queries.CountKeysToDelete(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(cleanupTasks).To(Equal(int64(0)))
		})
	})

	Context("when multiple dataranges exist", func() {
		BeforeEach(func(ctx SpecContext) {
			// Create multiple dataranges
			env.CreateCompletedDatarange(ctx, 0, 10)  // 0-9
			env.CreateCompletedDatarange(ctx, 10, 10) // 10-19
			env.CreateCompletedDatarange(ctx, 100, 5) // 100-104
		})

		It("should delete only the specified datarange", func(ctx SpecContext) {
			// Verify initial state
			datarangeCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount).To(Equal(int64(3)))

			// Delete the middle datarange (10-19)
			deleteReq := &dataranges.DeleteDatarangeRequest{
				Datas3tName:       env.TestDatas3tName,
				FirstDatapointKey: 10,
				LastDatapointKey:  19,
			}

			err = env.UploadSrv.DeleteDatarange(ctx, env.Logger, deleteReq)
			Expect(err).NotTo(HaveOccurred())

			// Verify only one datarange was deleted
			datarangeCount2, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount2).To(Equal(int64(2)))

			// Verify the correct dataranges remain
			remainingDataranges, err := env.Queries.GetAllDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(remainingDataranges)).To(Equal(2))

			// Check that we have the expected ranges (0-9 and 100-104)
			minKeys := []int64{remainingDataranges[0].MinDatapointKey, remainingDataranges[1].MinDatapointKey}
			maxKeys := []int64{remainingDataranges[0].MaxDatapointKey, remainingDataranges[1].MaxDatapointKey}

			Expect(minKeys).To(ContainElements(int64(0), int64(100)))
			Expect(maxKeys).To(ContainElements(int64(9), int64(104)))
		})

		It("should handle deletion of first datarange", func(ctx SpecContext) {
			// Delete the first datarange (0-9)
			deleteReq := &dataranges.DeleteDatarangeRequest{
				Datas3tName:       env.TestDatas3tName,
				FirstDatapointKey: 0,
				LastDatapointKey:  9,
			}

			err := env.UploadSrv.DeleteDatarange(ctx, env.Logger, deleteReq)
			Expect(err).NotTo(HaveOccurred())

			// Verify only one datarange was deleted
			datarangeCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount).To(Equal(int64(2)))

			// Verify the correct dataranges remain (10-19 and 100-104)
			remainingDataranges, err := env.Queries.GetAllDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(remainingDataranges)).To(Equal(2))

			minKeys := []int64{remainingDataranges[0].MinDatapointKey, remainingDataranges[1].MinDatapointKey}
			maxKeys := []int64{remainingDataranges[0].MaxDatapointKey, remainingDataranges[1].MaxDatapointKey}

			Expect(minKeys).To(ContainElements(int64(10), int64(100)))
			Expect(maxKeys).To(ContainElements(int64(19), int64(104)))
		})

		It("should handle deletion of last datarange", func(ctx SpecContext) {
			// Delete the last datarange (100-104)
			deleteReq := &dataranges.DeleteDatarangeRequest{
				Datas3tName:       env.TestDatas3tName,
				FirstDatapointKey: 100,
				LastDatapointKey:  104,
			}

			err := env.UploadSrv.DeleteDatarange(ctx, env.Logger, deleteReq)
			Expect(err).NotTo(HaveOccurred())

			// Verify only one datarange was deleted
			datarangeCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount).To(Equal(int64(2)))

			// Verify the correct dataranges remain (0-9 and 10-19)
			remainingDataranges, err := env.Queries.GetAllDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(remainingDataranges)).To(Equal(2))

			minKeys := []int64{remainingDataranges[0].MinDatapointKey, remainingDataranges[1].MinDatapointKey}
			maxKeys := []int64{remainingDataranges[0].MaxDatapointKey, remainingDataranges[1].MaxDatapointKey}

			Expect(minKeys).To(ContainElements(int64(0), int64(10)))
			Expect(maxKeys).To(ContainElements(int64(9), int64(19)))
		})
	})

	Context("when deleting all dataranges", func() {
		BeforeEach(func(ctx SpecContext) {
			// Create a single datarange
			env.CreateCompletedDatarange(ctx, 0, 10) // 0-9
		})

		It("should successfully delete the only datarange", func(ctx SpecContext) {
			// Verify initial state
			datarangeCount, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount).To(Equal(int64(1)))

			// Delete the datarange
			deleteReq := &dataranges.DeleteDatarangeRequest{
				Datas3tName:       env.TestDatas3tName,
				FirstDatapointKey: 0,
				LastDatapointKey:  9,
			}

			err = env.UploadSrv.DeleteDatarange(ctx, env.Logger, deleteReq)
			Expect(err).NotTo(HaveOccurred())

			// Verify no dataranges remain
			datarangeCount2, err := env.Queries.CountDataranges(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(datarangeCount2).To(Equal(int64(0)))

			// Verify datas3t still exists (only dataranges are deleted)
			datas3ts, err := env.Queries.AllDatas3ts(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(datas3ts)).To(Equal(1))
			Expect(datas3ts[0]).To(Equal(env.TestDatas3tName))
		})
	})
})
