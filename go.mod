module github.com/ONSdigital/dp-dataset-api

go 1.20

// to avoid the following vulnerabilities:
//     - CVE-2022-29153 # pkg:golang/github.com/hashicorp/consul/api@v1.1.0
//     - sonatype-2021-1401 # pkg:golang/github.com/miekg/dns@v1.0.14
replace github.com/spf13/cobra => github.com/spf13/cobra v1.7.0

// to avoid the following vulnerabilities:
//     - CVE-2023-32731 # pkg:google.golang.org/grpc
replace google.golang.org/grpc => google.golang.org/grpc v1.55.0

// to avoid 'sonatype-2021-4899' non-CVE Vulnerability
exclude github.com/gorilla/sessions v1.2.1

require (
	github.com/ONSdigital/dp-api-clients-go/v2 v2.254.0
	github.com/ONSdigital/dp-assistdog v0.0.1
	github.com/ONSdigital/dp-authorisation v0.2.1
	github.com/ONSdigital/dp-component-test v0.9.2
	github.com/ONSdigital/dp-graph/v2 v2.17.0
	github.com/ONSdigital/dp-healthcheck v1.6.1
	github.com/ONSdigital/dp-kafka/v3 v3.10.0
	github.com/ONSdigital/dp-mongodb/v3 v3.6.0
	github.com/ONSdigital/dp-net/v2 v2.11.1
	github.com/ONSdigital/log.go/v2 v2.4.1
	github.com/cucumber/godog v0.12.6
	github.com/google/go-cmp v0.5.9
	github.com/gorilla/mux v1.8.0
	github.com/jinzhu/copier v0.3.5
	github.com/justinas/alice v1.2.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/pkg/errors v0.9.1
	github.com/satori/go.uuid v1.2.1-0.20181028125025-b2ce2384e17b
	github.com/smartystreets/goconvey v1.8.0
	github.com/stretchr/testify v1.8.4
	go.mongodb.org/mongo-driver v1.12.1
)

require (
	github.com/ONSdigital/dp-mongodb-in-memory v1.7.0 // indirect
	github.com/ONSdigital/dp-rchttp v1.0.0 // indirect
	github.com/ONSdigital/go-ns v0.0.0-20210916104633-ac1c1c52327e // indirect
	github.com/ONSdigital/golang-neo4j-bolt-driver v0.0.0-20210408132126-c2323ff08bf1 // indirect
	github.com/ONSdigital/graphson v0.3.0 // indirect
	github.com/ONSdigital/gremgo-neptune v1.1.0 // indirect
	github.com/Shopify/sarama v1.38.1 // indirect
	github.com/chromedp/cdproto v0.0.0-20230914224007-a15a36ccbc2e // indirect
	github.com/chromedp/chromedp v0.9.2 // indirect
	github.com/chromedp/sysutil v1.0.0 // indirect
	github.com/cucumber/gherkin-go/v19 v19.0.3 // indirect
	github.com/cucumber/messages-go/v16 v16.0.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/eapache/go-resiliency v1.4.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230731223053-c322873962e3 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/fatih/color v1.15.0 // indirect
	github.com/go-avro/avro v0.0.0-20171219232920-444163702c11 // indirect
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/gobwas/ws v1.3.0 // indirect
	github.com/gofrs/uuid v4.4.0+incompatible // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.3.1 // indirect
	github.com/gopherjs/gopherjs v1.17.2 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
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
	github.com/klauspost/compress v1.17.0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/maxcnunes/httpfake v1.2.4 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	github.com/smartystreets/assertions v1.13.1 // indirect
	github.com/spf13/afero v1.9.5 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/square/mongo-lock v0.0.0-20230808145049-cfcf499f6bf0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20201027041543-1326539a0a0a // indirect
	golang.org/x/crypto v0.14.0 // indirect
	golang.org/x/net v0.17.0 // indirect
	golang.org/x/sync v0.3.0 // indirect
	golang.org/x/sys v0.13.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
