module github.com/ONSdigital/dp-dataset-api

go 1.23.4

// to avoid the following vulnerabilities:
//     - CVE-2022-29153 # pkg:golang/github.com/hashicorp/consul/api@v1.1.0
//     - sonatype-2021-1401 # pkg:golang/github.com/miekg/dns@v1.0.14
replace github.com/spf13/cobra => github.com/spf13/cobra v1.7.0

// [CVE-2024-24786] CWE-835: Loop with Unreachable Exit Condition ('Infinite Loop')
replace google.golang.org/protobuf => google.golang.org/protobuf v1.33.0

require (
	github.com/ONSdigital/dp-api-clients-go/v2 v2.261.0
	github.com/ONSdigital/dp-assistdog v0.0.1
	github.com/ONSdigital/dp-authorisation v0.2.1
	github.com/ONSdigital/dp-component-test v0.10.0
	github.com/ONSdigital/dp-graph/v2 v2.17.0
	github.com/ONSdigital/dp-healthcheck v1.6.3
	github.com/ONSdigital/dp-kafka/v4 v4.1.0
	github.com/ONSdigital/dp-mongodb/v3 v3.7.0
	github.com/ONSdigital/dp-net/v2 v2.13.0
	github.com/ONSdigital/dp-otel-go v0.0.7
	github.com/ONSdigital/log.go/v2 v2.4.3
	github.com/cucumber/godog v0.14.0
	github.com/google/go-cmp v0.6.0
	github.com/gorilla/mux v1.8.1
	github.com/jinzhu/copier v0.4.0
	github.com/justinas/alice v1.2.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/pkg/errors v0.9.1
	github.com/satori/go.uuid v1.2.1-0.20181028125025-b2ce2384e17b
	github.com/smartystreets/goconvey v1.8.1
	github.com/stretchr/testify v1.10.0
	go.mongodb.org/mongo-driver v1.13.1
	go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux v0.48.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.54.0
)

require (
	github.com/ONSdigital/dp-mongodb-in-memory v1.7.0 // indirect
	github.com/ONSdigital/dp-rchttp v1.0.0 // indirect
	github.com/ONSdigital/go-ns v0.0.0-20210916104633-ac1c1c52327e // indirect
	github.com/ONSdigital/golang-neo4j-bolt-driver v0.0.0-20210408132126-c2323ff08bf1 // indirect
	github.com/ONSdigital/graphson v0.3.0 // indirect
	github.com/ONSdigital/gremgo-neptune v1.1.0 // indirect
	github.com/Shopify/sarama v1.38.1 // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/chromedp/cdproto v0.0.0-20240202021202-6d0b6a386732 // indirect
	github.com/chromedp/chromedp v0.9.5 // indirect
	github.com/chromedp/sysutil v1.0.0 // indirect
	github.com/cucumber/gherkin/go/v26 v26.2.0 // indirect
	github.com/cucumber/messages/go/v21 v21.0.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/eapache/go-resiliency v1.5.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230731223053-c322873962e3 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/fatih/color v1.18.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-avro/avro v0.0.0-20171219232920-444163702c11 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/gobwas/ws v1.3.2 // indirect
	github.com/gofrs/uuid v4.4.0+incompatible // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gopherjs/gopherjs v1.17.2 // indirect
	github.com/gorilla/schema v1.4.1 // indirect
	github.com/gorilla/websocket v1.5.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.19.1 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-memdb v1.3.4 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/hokaccha/go-prettyjson v0.0.0-20211117102719-0474bc63780f // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/klauspost/compress v1.17.6 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/maxcnunes/httpfake v1.2.4 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/smarty/assertions v1.16.0 // indirect
	github.com/spf13/afero v1.11.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/square/mongo-lock v0.0.0-20230808145049-cfcf499f6bf0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20201027041543-1326539a0a0a // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama v0.43.0 // indirect
	go.opentelemetry.io/contrib/propagators/autoprop v0.48.0 // indirect
	go.opentelemetry.io/contrib/propagators/aws v1.23.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.23.0 // indirect
	go.opentelemetry.io/contrib/propagators/jaeger v1.23.0 // indirect
	go.opentelemetry.io/contrib/propagators/ot v1.23.0 // indirect
	go.opentelemetry.io/otel v1.33.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.23.1 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.23.1 // indirect
	go.opentelemetry.io/otel/metric v1.33.0 // indirect
	go.opentelemetry.io/otel/sdk v1.33.0 // indirect
	go.opentelemetry.io/otel/trace v1.33.0 // indirect
	go.opentelemetry.io/proto/otlp v1.1.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/net v0.33.0 // indirect
	golang.org/x/sync v0.10.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240725223205-93522f1f2a9f // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1 // indirect
	google.golang.org/grpc v1.66.2 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
