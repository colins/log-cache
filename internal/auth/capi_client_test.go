package auth_test

import (
	"fmt"
	"sync"

	"code.cloudfoundry.org/log-cache/internal/auth"

	"errors"
	"net/http"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CAPIClient", func() {
	var (
		capiClient *spyHTTPClient
		client     *auth.CAPIClient
	)

	BeforeEach(func() {
		capiClient = newSpyHTTPClient()
		client = auth.NewCAPIClient("https://capi.com", "http://external.capi.com", capiClient)
	})

	Describe("IsAuthorized", func() {
		It("hits CAPI correctly", func() {
			capiClient.resps = []response{
				{status: http.StatusNotFound},
			}

			client.IsAuthorized("some-id", "some-token")
			r := capiClient.requests[0]

			Expect(r.Method).To(Equal(http.MethodGet))
			Expect(r.URL.String()).To(Equal("https://capi.com/internal/v4/log_access/some-id"))
			Expect(r.Header.Get("Authorization")).To(Equal("some-token"))

			r = capiClient.requests[1]

			Expect(r.Method).To(Equal(http.MethodGet))
			Expect(r.URL.String()).To(Equal("http://external.capi.com/v2/service_instances/some-id"))
			Expect(r.Header.Get("Authorization")).To(Equal("some-token"))
		})

		It("returns true for authorized token for an app", func() {
			capiClient.resps = []response{{status: http.StatusOK}}
			Expect(client.IsAuthorized("some-id", "some-token")).To(BeTrue())
		})

		It("returns true for authorized token for a service instance", func() {
			capiClient.resps = []response{
				{status: http.StatusNotFound},
				{status: http.StatusOK},
			}
			Expect(client.IsAuthorized("some-id", "some-token")).To(BeTrue())
		})

		It("returns false when CAPI returns non 200 for app and service instance", func() {
			capiClient.resps = []response{
				{status: http.StatusNotFound},
				{status: http.StatusNotFound},
			}
			Expect(client.IsAuthorized("some-id", "some-token")).To(BeFalse())
		})

		It("returns false when CAPI request fails", func() {
			capiClient.resps = []response{{err: errors.New("intentional error")}}
			Expect(client.IsAuthorized("some-id", "some-token")).To(BeFalse())
		})

		It("is goroutine safe", func() {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				for i := 0; i < 1000; i++ {
					client.IsAuthorized(fmt.Sprintf("some-id-%d", i), "some-token")
				}

				wg.Done()
			}()

			for i := 0; i < 1000; i++ {
				client.IsAuthorized(fmt.Sprintf("some-id-%d", i), "some-token")
			}
			wg.Wait()
		})
	})

	Describe("AvailableSourceIDs", func() {
		It("hits CAPI correctly", func() {
			client.AvailableSourceIDs("some-token")
			r := capiClient.requests[0]

			Expect(r.Method).To(Equal(http.MethodGet))
			Expect(r.URL.String()).To(Equal("http://external.capi.com/v3/apps"))
			Expect(r.Header.Get("Authorization")).To(Equal("some-token"))
		})

		It("returns the available source IDs", func() {
			capiClient.resps = []response{{status: http.StatusOK, body: []byte(`{"resources": [{"guid": "source-0"}, {"guid": "source-1"}]}`)}}
			sourceIDs := client.AvailableSourceIDs("some-token")

			Expect(sourceIDs).To(ConsistOf("source-0", "source-1"))
		})

		It("returns empty slice when CAPI returns non 200", func() {
			capiClient.resps = []response{{status: http.StatusNotFound}}
			Expect(client.AvailableSourceIDs("some-token")).To(BeEmpty())
		})

		It("returns empty slice when CAPI request fails", func() {
			capiClient.resps = []response{{err: errors.New("intentional error")}}
			Expect(client.AvailableSourceIDs("some-token")).To(BeEmpty())
		})

		It("is goroutine safe", func() {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				for i := 0; i < 1000; i++ {
					client.AvailableSourceIDs("some-token")
				}

				wg.Done()
			}()

			for i := 0; i < 1000; i++ {
				client.AvailableSourceIDs("some-token")
			}
			wg.Wait()
		})
	})
})
