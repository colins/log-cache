package auth

import (
	"net/http"

	"log"

	"context"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"github.com/golang/protobuf/jsonpb"
	"github.com/gorilla/mux"
)

type CFAuthMiddlewareProvider struct {
	oauth2Reader  Oauth2ClientReader
	logAuthorizer LogAuthorizer
	metaFetcher   MetaFetcher
	marshaller    jsonpb.Marshaler
}

type Oauth2Client struct {
	IsAdmin bool
}

type Oauth2ClientReader interface {
	Read(token string) Oauth2Client
}

type LogAuthorizer interface {
	IsAuthorized(sourceID, token string) bool
	AvailableSourceIDs(token string) []string
}

type MetaFetcher interface {
	Meta(context.Context) (map[string]*rpc.MetaInfo, error)
}

func NewCFAuthMiddlewareProvider(
	oauth2Reader Oauth2ClientReader,
	logAuthorizer LogAuthorizer,
	metaFetcher MetaFetcher,
) CFAuthMiddlewareProvider {
	return CFAuthMiddlewareProvider{
		oauth2Reader:  oauth2Reader,
		logAuthorizer: logAuthorizer,
		metaFetcher:   metaFetcher,
	}
}

func (m CFAuthMiddlewareProvider) Middleware(h http.Handler) http.Handler {
	router := mux.NewRouter()

	router.HandleFunc("/v1/read/{sourceID}", func(w http.ResponseWriter, r *http.Request) {
		sourceID, ok := mux.Vars(r)["sourceID"]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		authToken := r.Header.Get("Authorization")
		if authToken == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if !m.oauth2Reader.Read(authToken).IsAdmin {
			if !m.logAuthorizer.IsAuthorized(sourceID, authToken) {
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}

		h.ServeHTTP(w, r)
	})

	router.HandleFunc("/v1/meta", func(w http.ResponseWriter, r *http.Request) {
		meta, err := m.metaFetcher.Meta(r.Context())
		if err != nil {
			log.Printf("failed to fetch meta information: %s", err)
			w.WriteHeader(http.StatusBadGateway)
			return
		}

		authToken := r.Header.Get("Authorization")
		if authToken == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		// We don't care if writing to the client fails. They can come back and ask again.
		_ = m.marshaller.Marshal(w, &rpc.MetaResponse{
			Meta: m.onlyAuthorized(authToken, meta),
		})
	})

	return router
}

func (m CFAuthMiddlewareProvider) onlyAuthorized(authToken string, meta map[string]*rpc.MetaInfo) map[string]*rpc.MetaInfo {
	if m.oauth2Reader.Read(authToken).IsAdmin {
		return meta
	}

	authorized := m.logAuthorizer.AvailableSourceIDs(authToken)
	intersection := make(map[string]*rpc.MetaInfo)
	for _, id := range authorized {
		if v, ok := meta[id]; ok {
			intersection[id] = v
		}
	}

	return intersection
}
