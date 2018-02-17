package logcache_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache"
	"code.cloudfoundry.org/log-cache/internal/egress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LogCache", func() {
	var (
		tlsConfig *tls.Config
	)

	BeforeEach(func() {
		var err error
		tlsConfig, err = newTLSConfig(
			Cert("log-cache-ca.crt"),
			Cert("log-cache.crt"),
			Cert("log-cache.key"),
			"log-cache",
		)
		Expect(err).ToNot(HaveOccurred())
	})

	It("returns tail of data filtered by source ID in 1 node cluster", func() {
		spyMetrics := newSpyMetrics()
		cache := logcache.New(
			logcache.WithAddr("127.0.0.1:0"),
			logcache.WithClustered(0, []string{"my-addr"}),
			logcache.WithMetrics(spyMetrics),
			logcache.WithServerOpts(
				grpc.Creds(credentials.NewTLS(tlsConfig)),
			),
		)
		cache.Start()

		conn, err := grpc.Dial(cache.Addr(),
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		)
		Expect(err).ToNot(HaveOccurred())
		defer conn.Close()
		client := rpc.NewEgressClient(conn)
		orchClient := rpc.NewOrchestrationClient(conn)

		_, err = orchClient.SetRanges(context.Background(), &rpc.SetRangesRequest{
			Ranges: map[string]*rpc.Ranges{
				"my-addr": &rpc.Ranges{
					Ranges: []*rpc.Range{
						{Start: 0, End: 18446744073709551615},
					},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		writeEnvelopes(cache.Addr(), []*loggregator_v2.Envelope{
			{Timestamp: 1, SourceId: "app-a"},
			{Timestamp: 2, SourceId: "app-b"},
			{Timestamp: 3, SourceId: "app-a"},
			{Timestamp: 4, SourceId: "app-a"},
		})

		var es []*loggregator_v2.Envelope
		f := func() error {
			resp, err := client.Read(context.Background(), &rpc.ReadRequest{
				SourceId:   "app-a",
				Descending: true,
				Limit:      2,
			})
			if err != nil {
				return err
			}

			if len(resp.Envelopes.Batch) != 2 {
				return errors.New("expected 2 envelopes")
			}

			es = resp.Envelopes.Batch
			return nil
		}
		Eventually(f).Should(BeNil())

		Expect(es[0].Timestamp).To(Equal(int64(4)))
		Expect(es[0].SourceId).To(Equal("app-a"))
		Expect(es[1].Timestamp).To(Equal(int64(3)))
		Expect(es[1].SourceId).To(Equal("app-a"))

		Eventually(spyMetrics.getter("Ingress")).Should(Equal(uint64(4)))
		Eventually(spyMetrics.getter("Egress")).Should(Equal(uint64(2)))
	})

	It("copies envelopes from a peer if they from an older route", func() {
		peer := newSpyLogCache(tlsConfig)
		peerAddr := peer.start()

		cache := logcache.New(
			logcache.WithAddr("127.0.0.1:0"),
			logcache.WithClustered(0, []string{"my-addr", peerAddr},
				grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			),
			logcache.WithServerOpts(
				grpc.Creds(credentials.NewTLS(tlsConfig)),
			),
		)
		cache.Start()

		conn, err := grpc.Dial(cache.Addr(),
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		)
		Expect(err).ToNot(HaveOccurred())
		defer conn.Close()
		client := rpc.NewEgressClient(conn)
		orchClient := rpc.NewOrchestrationClient(conn)

		_, err = orchClient.SetRanges(context.Background(), &rpc.SetRangesRequest{
			Ranges: map[string]*rpc.Ranges{
				"my-addr": &rpc.Ranges{
					Ranges: []*rpc.Range{
						{Start: 0, End: 9223372036854775807, Term: 2},
					},
				},
				peerAddr: &rpc.Ranges{
					Ranges: []*rpc.Range{
						{Start: 0, End: 9223372036854775807, Term: 1},
						{Start: 9223372036854775808, End: 18446744073709551615, Term: 2},
					},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		peer.readEnvelopes["source-0"] = func() []*loggregator_v2.Envelope {
			return []*loggregator_v2.Envelope{
				{Timestamp: 2, SourceId: "source-0"},
			}
		}

		writeEnvelopes(cache.Addr(), []*loggregator_v2.Envelope{
			// source-0 hashes to 7700738999732113484 (route to node 0)
			{Timestamp: 1, SourceId: "source-0"},
		})

		Eventually(func() error {
			resp, err := client.Read(context.Background(), &rpc.ReadRequest{
				SourceId: "source-0",
				Limit:    2,
			})
			if err != nil {
				return err
			}

			if len(resp.Envelopes.Batch) != 2 {
				return fmt.Errorf("expected 2 envelopes: %d", len(resp.Envelopes.Batch))
			}

			return nil
		}).Should(BeNil())
	})

	It("routes envelopes to peers", func() {
		peer := newSpyLogCache(tlsConfig)
		peerAddr := peer.start()

		cache := logcache.New(
			logcache.WithAddr("127.0.0.1:0"),
			logcache.WithClustered(0, []string{"my-addr", peerAddr},
				grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			),
			logcache.WithServerOpts(
				grpc.Creds(credentials.NewTLS(tlsConfig)),
			),
		)
		cache.Start()

		conn, err := grpc.Dial(cache.Addr(),
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		)
		Expect(err).ToNot(HaveOccurred())
		defer conn.Close()
		orchClient := rpc.NewOrchestrationClient(conn)

		_, err = orchClient.SetRanges(context.Background(), &rpc.SetRangesRequest{
			Ranges: map[string]*rpc.Ranges{
				"my-addr": &rpc.Ranges{
					Ranges: []*rpc.Range{
						{Start: 0, End: 9223372036854775807, Term: 2},

						// Has older term and should be ignored
						{Start: 9223372036854775808, End: 18446744073709551615, Term: 1},
					},
				},
				peerAddr: &rpc.Ranges{
					Ranges: []*rpc.Range{
						{Start: 9223372036854775808, End: 18446744073709551615, Term: 2},
					},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		writeEnvelopes(cache.Addr(), []*loggregator_v2.Envelope{
			// source-0 hashes to 7700738999732113484 (route to node 0)
			{Timestamp: 1, SourceId: "source-0"},
			// source-1 hashes to 15704273932878139171 (route to node 1)
			{Timestamp: 2, SourceId: "source-1"},
			{Timestamp: 3, SourceId: "source-1"},
		})

		Eventually(peer.getEnvelopes).Should(HaveLen(2))
		Expect(peer.getEnvelopes()[0].Timestamp).To(Equal(int64(2)))
		Expect(peer.getEnvelopes()[1].Timestamp).To(Equal(int64(3)))
	})

	It("accepts envelopes from peers", func() {
		cache := logcache.New(
			logcache.WithAddr("127.0.0.1:0"),
			logcache.WithClustered(0, []string{"my-addr", "other-addr"},
				grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			),
			logcache.WithServerOpts(
				grpc.Creds(credentials.NewTLS(tlsConfig)),
			),
		)
		cache.Start()

		conn, err := grpc.Dial(cache.Addr(),
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		)
		Expect(err).ToNot(HaveOccurred())
		defer conn.Close()
		client := rpc.NewEgressClient(conn)
		orchClient := rpc.NewOrchestrationClient(conn)

		_, err = orchClient.SetRanges(context.Background(), &rpc.SetRangesRequest{
			Ranges: map[string]*rpc.Ranges{
				"my-addr": &rpc.Ranges{
					Ranges: []*rpc.Range{
						{Start: 0, End: 18446744073709551615},
					},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		peerWriter := egress.NewPeerWriter(
			cache.Addr(),
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		)

		// source-0 hashes to 7700738999732113484 (route to node 0)
		peerWriter.Write(&loggregator_v2.Envelope{SourceId: "source-0", Timestamp: 1})

		var es []*loggregator_v2.Envelope
		f := func() error {
			resp, err := client.Read(context.Background(), &rpc.ReadRequest{
				SourceId: "source-0",
			})
			if err != nil {
				return err
			}

			if len(resp.Envelopes.Batch) != 1 {
				return errors.New("expected 1 envelopes")
			}

			es = resp.Envelopes.Batch
			return nil
		}
		Eventually(f).Should(BeNil())

		Expect(es[0].Timestamp).To(Equal(int64(1)))
		Expect(es[0].SourceId).To(Equal("source-0"))
	})

	It("routes query requests to peers", func() {
		peer := newSpyLogCache(tlsConfig)
		peerAddr := peer.start()
		peer.readEnvelopes["source-1"] = func() []*loggregator_v2.Envelope {
			return []*loggregator_v2.Envelope{
				{Timestamp: 99},
				{Timestamp: 101},
			}
		}

		cache := logcache.New(
			logcache.WithAddr("127.0.0.1:0"),
			logcache.WithClustered(0, []string{"my-addr", peerAddr},
				grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			),
			logcache.WithServerOpts(
				grpc.Creds(credentials.NewTLS(tlsConfig)),
			),
		)

		cache.Start()

		conn, err := grpc.Dial(cache.Addr(),
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		)
		Expect(err).ToNot(HaveOccurred())
		defer conn.Close()
		client := rpc.NewEgressClient(conn)
		orchClient := rpc.NewOrchestrationClient(conn)

		_, err = orchClient.SetRanges(context.Background(), &rpc.SetRangesRequest{
			Ranges: map[string]*rpc.Ranges{
				"my-addr": &rpc.Ranges{
					Ranges: []*rpc.Range{
						{Start: 0, End: 9223372036854775807, Term: 2},

						// Has older term and should be ignored
						{Start: 9223372036854775808, End: 18446744073709551615, Term: 1},
					},
				},
				peerAddr: &rpc.Ranges{
					Ranges: []*rpc.Range{
						{Start: 9223372036854775808, End: 18446744073709551615, Term: 2},
					},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		// source-1 hashes to 15704273932878139171 (route to node 1)
		resp, err := client.Read(context.Background(), &rpc.ReadRequest{
			SourceId:     "source-1",
			StartTime:    99,
			EndTime:      101,
			EnvelopeType: rpc.EnvelopeTypes_LOG,
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Envelopes.Batch).To(HaveLen(2))

		Eventually(peer.getReadRequests).Should(HaveLen(1))
		req := peer.getReadRequests()[0]
		Expect(req.SourceId).To(Equal("source-1"))
		Expect(req.StartTime).To(Equal(int64(99)))
		Expect(req.EndTime).To(Equal(int64(101)))
		Expect(req.EnvelopeType).To(Equal(rpc.EnvelopeTypes_LOG))
	})

	It("returns all meta information", func() {
		peer := newSpyLogCache(tlsConfig)
		peer.metaResponses = map[string]*rpc.MetaInfo{
			"source-1": &rpc.MetaInfo{
				Count:           1,
				Expired:         2,
				OldestTimestamp: 3,
				NewestTimestamp: 4,
			},
		}

		peerAddr := peer.start()

		myAddr := "127.0.0.1:0"
		lc := logcache.New(
			logcache.WithAddr(myAddr),
			logcache.WithClustered(0, []string{myAddr, peerAddr},
				grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			),
			logcache.WithServerOpts(
				grpc.Creds(credentials.NewTLS(tlsConfig)),
			),
		)

		lc.Start()

		conn, err := grpc.Dial(lc.Addr(),
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		)
		Expect(err).ToNot(HaveOccurred())
		defer conn.Close()
		ingressClient := rpc.NewIngressClient(conn)
		egressClient := rpc.NewEgressClient(conn)
		orchClient := rpc.NewOrchestrationClient(conn)

		_, err = orchClient.SetRanges(context.Background(), &rpc.SetRangesRequest{
			Ranges: map[string]*rpc.Ranges{
				myAddr: &rpc.Ranges{
					Ranges: []*rpc.Range{
						{Start: 0, End: 9223372036854775807, Term: 2},

						// Has older term and should be ignored
						{Start: 9223372036854775808, End: 18446744073709551615, Term: 1},
					},
				},
				peerAddr: &rpc.Ranges{
					Ranges: []*rpc.Range{
						{Start: 9223372036854775808, End: 18446744073709551615, Term: 2},
					},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		sendRequest := &rpc.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{SourceId: "source-0"},
				},
			},
		}

		ingressClient.Send(context.Background(), sendRequest)
		Eventually(func() map[string]*rpc.MetaInfo {
			resp, err := egressClient.Meta(context.Background(), &rpc.MetaRequest{})
			if err != nil {
				return nil
			}

			return resp.Meta
		}).Should(And(
			HaveKeyWithValue("source-0", &rpc.MetaInfo{
				Count: 1,
			}),
			HaveKeyWithValue("source-1", &rpc.MetaInfo{
				Count:           1,
				Expired:         2,
				OldestTimestamp: 3,
				NewestTimestamp: 4,
			}),
		))
	})

	Describe("orchestration", func() {
		var (
			orchClient rpc.OrchestrationClient
		)

		BeforeEach(func() {
			cache := logcache.New(
				logcache.WithAddr("127.0.0.1:0"),
			)
			cache.Start()

			conn, err := grpc.Dial(cache.Addr(), grpc.WithInsecure())
			Expect(err).ToNot(HaveOccurred())
			orchClient = rpc.NewOrchestrationClient(conn)
		})

		It("returns only the latest ranges", func() {
			resp, err := orchClient.ListRanges(context.Background(), &rpc.ListRangesRequest{})
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.Ranges).To(HaveLen(0))

			orchClient.AddRange(context.Background(), &rpc.AddRangeRequest{
				Range: &rpc.Range{
					Start: 0,
					End:   100,
				},
			})

			orchClient.AddRange(context.Background(), &rpc.AddRangeRequest{
				Range: &rpc.Range{
					Start: 101,
					End:   200,
				},
			})

			resp, err = orchClient.ListRanges(context.Background(), &rpc.ListRangesRequest{})
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.Ranges).To(HaveLen(2))
		})
	})
})

func writeEnvelopes(addr string, es []*loggregator_v2.Envelope) {
	tlsConfig, err := newTLSConfig(
		Cert("log-cache-ca.crt"),
		Cert("log-cache.crt"),
		Cert("log-cache.key"),
		"log-cache",
	)
	if err != nil {
		panic(err)
	}
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
	)
	if err != nil {
		panic(err)
	}

	client := rpc.NewIngressClient(conn)
	var envelopes []*loggregator_v2.Envelope
	for _, e := range es {
		envelopes = append(envelopes, &loggregator_v2.Envelope{
			Timestamp: e.Timestamp,
			SourceId:  e.SourceId,
		})
	}

	_, err = client.Send(context.Background(), &rpc.SendRequest{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: envelopes,
		},
	})
	if err != nil {
		panic(err)
	}
}

type spyLogCache struct {
	mu            sync.Mutex
	envelopes     []*loggregator_v2.Envelope
	readRequests  []*rpc.ReadRequest
	readEnvelopes map[string]func() []*loggregator_v2.Envelope
	metaResponses map[string]*rpc.MetaInfo
	tlsConfig     *tls.Config
}

func newSpyLogCache(tlsConfig *tls.Config) *spyLogCache {
	return &spyLogCache{
		readEnvelopes: make(map[string]func() []*loggregator_v2.Envelope),
		tlsConfig:     tlsConfig,
	}
}

func (s *spyLogCache) start() string {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(s.tlsConfig)),
	)
	rpc.RegisterIngressServer(srv, s)
	rpc.RegisterEgressServer(srv, s)
	go srv.Serve(lis)

	return lis.Addr().String()
}

func (s *spyLogCache) getEnvelopes() []*loggregator_v2.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := make([]*loggregator_v2.Envelope, len(s.envelopes))
	copy(r, s.envelopes)
	return r
}

func (s *spyLogCache) getReadRequests() []*rpc.ReadRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := make([]*rpc.ReadRequest, len(s.readRequests))
	copy(r, s.readRequests)
	return r
}

func (s *spyLogCache) Send(ctx context.Context, r *rpc.SendRequest) (*rpc.SendResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range r.Envelopes.Batch {
		s.envelopes = append(s.envelopes, e)
	}

	return &rpc.SendResponse{}, nil
}

func (s *spyLogCache) Read(ctx context.Context, r *rpc.ReadRequest) (*rpc.ReadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.readRequests = append(s.readRequests, r)

	b := s.readEnvelopes[r.GetSourceId()]

	var batch []*loggregator_v2.Envelope
	if b != nil {
		batch = b()
	}

	return &rpc.ReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: batch,
		},
	}, nil
}

func (s *spyLogCache) Meta(ctx context.Context, r *rpc.MetaRequest) (*rpc.MetaResponse, error) {
	return &rpc.MetaResponse{
		Meta: s.metaResponses,
	}, nil
}

func newTLSConfig(caPath, certPath, keyPath, cn string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		ServerName:         cn,
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: false,
	}

	caCertBytes, err := ioutil.ReadFile(caPath)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCertBytes); !ok {
		return nil, errors.New("cannot parse ca cert")
	}

	tlsConfig.RootCAs = caCertPool

	return tlsConfig, nil
}
