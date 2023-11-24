package server_test

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/draganm/datas3t/client"
	"github.com/draganm/datas3t/server"
	"github.com/gofrs/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	. "github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/types"
	. "github.com/onsi/gomega"
)

func TestBooks(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "AdminSuite", types.ReporterConfig{NoColor: true, Succinct: false})
}

// docker run --rm -p 9000:9000 -p 9001:9001 quay.io/minio/minio server /data --console-address ":9001"

const minioEndpoint = "localhost:9000"

var minioClient *minio.Client
var bucketName string

var _ = BeforeSuite(func(ctx SpecContext) {
	var err error
	minioClient, err = minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
	Expect(err).NotTo(HaveOccurred())

	bucketName = mustCreateRandomName()

	err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	Expect(err).NotTo(HaveOccurred())

	DeferCleanup(func(ctx SpecContext) {
		err := minioClient.RemoveBucketWithOptions(ctx, bucketName, minio.RemoveBucketOptions{
			ForceDelete: true,
		})
		Expect(err).NotTo(HaveOccurred())
	})
})

func mustCreateRandomName() string {
	bucketNameUUID, err := uuid.NewV4()
	Expect(err).NotTo(HaveOccurred())
	return bucketNameUUID.String()
}

var _ = Describe("server admin api", func() {

	var srv *server.Server
	var prefix string
	var hs *httptest.Server
	var cl *client.DataS3tClient

	BeforeEach(func(ctx SpecContext) {
		prefix = mustCreateRandomName()
		var err error

		srv, err = server.OpenServer(
			ctx,
			GinkgoLogr,
			server.S3Config{
				S3Endpoint:        fmt.Sprintf("http://%s", minioEndpoint),
				AccessKeyID:       "minioadmin",
				SecretAccessKey:   "minioadmin",
				BucketName:        bucketName,
				Prefix:            prefix,
				HostnameImmutable: true,
			},
		)
		Expect(err).NotTo(HaveOccurred())

		hs = httptest.NewServer(srv.API)

		time.Sleep(1 * time.Second)

		cl, err = client.NewClient(hs.URL, client.Options{})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		hs.Close()
	})

	Describe("get last ID", func() {
		BeforeEach(func(ctx SpecContext) {
			err := cl.CreateDB(ctx, "test")
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when the DB is empty", func() {
			It("should return max uint64", func(ctx SpecContext) {
				id, err := cl.GetLastID(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(id).To(Equal(uint64(math.MaxUint64)))
			})
		})

		Context("when DB has one object", func() {
			BeforeEach(func(ctx SpecContext) {
				ur, err := cl.GetUploadURL(ctx, "test", 0)
				Expect(err).ToNot(HaveOccurred())

				data := []byte("foobar")
				req, err := http.NewRequestWithContext(ctx, "PUT", ur, bytes.NewReader(data))
				Expect(err).ToNot(HaveOccurred())

				res, err := http.DefaultClient.Do(req)
				Expect(err).ToNot(HaveOccurred())

				defer res.Body.Close()

				Expect(res.StatusCode).To(Equal(http.StatusOK))

			})

			It("should return 0", func(ctx SpecContext) {
				id, err := cl.GetLastID(ctx, "test")
				Expect(err).ToNot(HaveOccurred())
				Expect(id).To(Equal(uint64(0)))
			})
		})
	})

	Describe("bulk upload", func() {
		BeforeEach(func(ctx SpecContext) {
			err := cl.CreateDB(ctx, "test")
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when I request an upload URL", func() {
			var urls []string
			BeforeEach(func(ctx SpecContext) {
				var err error
				urls, err = cl.GetBulkUploaddURLs(ctx, "test", 0, 1)
				Expect(err).ToNot(HaveOccurred())
			})

			It("should return a valid URL", func() {
				for _, u := range urls {
					_, err := url.Parse(u)
					Expect(err).ToNot(HaveOccurred())
				}
			})

			Context("when I upload files to the URLs", func() {
				BeforeEach(func(ctx SpecContext) {
					{
						data := []byte("foobar")
						req, err := http.NewRequestWithContext(ctx, "PUT", urls[0], bytes.NewReader(data))
						Expect(err).ToNot(HaveOccurred())

						res, err := http.DefaultClient.Do(req)
						Expect(err).ToNot(HaveOccurred())

						defer res.Body.Close()
						Expect(res.StatusCode).To(Equal(http.StatusOK))

						_, err = io.ReadAll(res.Body)
						Expect(err).ToNot(HaveOccurred())
					}

					{
						data := []byte("barfoo")
						req, err := http.NewRequestWithContext(ctx, "PUT", urls[1], bytes.NewReader(data))
						Expect(err).ToNot(HaveOccurred())

						res, err := http.DefaultClient.Do(req)
						Expect(err).ToNot(HaveOccurred())

						defer res.Body.Close()
						Expect(res.StatusCode).To(Equal(http.StatusOK))

						_, err = io.ReadAll(res.Body)
						Expect(err).ToNot(HaveOccurred())
					}

				})

				Context("when I bulk download the data", func() {
					var data [][]byte
					BeforeEach(func(ctx SpecContext) {
						data = make([][]byte, 2)
						urls, err := cl.GetBulkDownloadURLs(ctx, "test", 0, 1)
						Expect(err).ToNot(HaveOccurred())
						Expect(len(urls)).To(Equal(2))

						res, err := http.Get(urls[0])
						Expect(err).ToNot(HaveOccurred())
						Expect(res.StatusCode).To(Equal(http.StatusOK))

						defer res.Body.Close()

						data[0], err = io.ReadAll(res.Body)
						Expect(err).ToNot(HaveOccurred())

						res, err = http.Get(urls[1])
						Expect(err).ToNot(HaveOccurred())
						Expect(res.StatusCode).To(Equal(http.StatusOK))

						defer res.Body.Close()

						data[1], err = io.ReadAll(res.Body)
						Expect(err).ToNot(HaveOccurred())

					})

					It("should  return the uploaded data", func() {
						Expect(data).To(Equal([][]byte{[]byte("foobar"), []byte("barfoo")}))
					})
				})

			})

		})

	})

	Describe("upload", func() {
		BeforeEach(func(ctx SpecContext) {
			err := cl.CreateDB(ctx, "test")
			Expect(err).ToNot(HaveOccurred())
		})
		Context("when I request an upload URL", func() {
			var ur string
			BeforeEach(func(ctx SpecContext) {
				var err error
				ur, err = cl.GetUploadURL(ctx, "test", 0)
				Expect(err).ToNot(HaveOccurred())
			})

			It("should return a valid URL", func() {
				_, err := url.Parse(ur)
				Expect(err).ToNot(HaveOccurred())
			})

			Context("when I upload the file to the URL", func() {
				var statusCode int
				BeforeEach(func(ctx SpecContext) {

					data := []byte("foobar")
					req, err := http.NewRequestWithContext(ctx, "PUT", ur, bytes.NewReader(data))
					Expect(err).ToNot(HaveOccurred())

					res, err := http.DefaultClient.Do(req)
					Expect(err).ToNot(HaveOccurred())

					defer res.Body.Close()
					statusCode = res.StatusCode

					_, err = io.ReadAll(res.Body)
					Expect(err).ToNot(HaveOccurred())
				})

				It("should return 200 status code", func() {
					Expect(statusCode).To(Equal(200))
				})

				Context("when I download the data", func() {
					var err error
					var data []byte
					BeforeEach(func(ctx SpecContext) {
						err = cl.Download(ctx, "test", 0, func(r io.Reader) error {
							var err error
							data, err = io.ReadAll(r)
							return err
						})

					})
					It("should not return an error", func() {
						Expect(err).ToNot(HaveOccurred())
					})

					It("should  return the uploaded data", func() {
						Expect(string(data)).To(Equal("foobar"))
					})
				})

			})

		})
	})

	Describe("create db", func() {
		var err error
		BeforeEach(func(ctx SpecContext) {
			err = cl.CreateDB(ctx, "test")
		})
		It("should not return error", func() {
			Expect(err).NotTo(HaveOccurred())
		})

		When("I create the same db again", func() {
			BeforeEach(func(ctx SpecContext) {
				err = cl.CreateDB(ctx, "test")
			})
			It("should return error", func() {
				Expect(err).To(MatchError(client.ErrAlreadyExists))
			})
		})

		When("I list the DBs", func() {
			var dbs []string

			BeforeEach(func(ctx SpecContext) {
				var err error
				dbs, err = cl.ListDBs(ctx)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should contain the new database", func() {
				Expect(dbs).To(Equal([]string{"test"}))
			})
		})
	})

})
