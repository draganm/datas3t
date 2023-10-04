package server_test

import (
	"bytes"
	"fmt"
	"io"
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
	var cl *client.AdminClient

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

		hs = httptest.NewServer(srv.Admin)

		time.Sleep(1 * time.Second)

		cl, err = client.NewAdminClient(hs.URL)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		hs.Close()
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
					GinkgoLogr.Info("url", ur)
				})

				It("should return 200 status code", func() {
					Expect(statusCode).To(Equal(200))
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
