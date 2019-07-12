# gremgo-neptune

[![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg)](http://godoc.org/github.com/ONSdigital/gremgo-neptune) [![Build Status](https://travis-ci.org/ONSdigital/gremgo-neptune.svg?branch=master)](https://travis-ci.org/ONSdigital/gremgo-neptune) [![Go Report Card](https://goreportcard.com/badge/github.com/ONSdigital/gremgo-neptune)](https://goreportcard.com/report/github.com/ONSdigital/gremgo-neptune)

gremgo-neptune is a fork of [qasaur/gremgo](https://github.com/qasaur/gremgo) with alterations to make it compatible with [AWS Neptune](https://aws.amazon.com/neptune/) which is a "Fast, reliable graph database built for the cloud".

gremgo is a fast, efficient, and easy-to-use client for the TinkerPop graph database stack. It is a Gremlin language driver which uses WebSockets to interface with Gremlin Server and has a strong emphasis on concurrency and scalability. Please keep in mind that gremgo is still under heavy development and although effort is being made to fully cover gremgo with reliable tests, bugs may be present in several areas.

**Modifications were made to `gremgo` in order to "support" AWS Neptune's lack of Gremlin-specific features, like no support for query bindings, among others. See differences in Gremlin support here: [AWS Neptune Gremlin Implementation Differences](https://docs.aws.amazon.com/neptune/latest/userguide/access-graph-gremlin-differences.html)**

Installation
==========
```
go get github.com/ONSdigital/gremgo-neptune
dep ensure
```

Development
====

If you amend the `dialer` interface, please run:
```
go generate
```

Documentation
==========

* [GoDoc](https://godoc.org/github.com/ONSdigital/gremgo-neptune)

Examples

- [simple example](examples/simple/main.go)
- [cursor example](examples/cursor/main.go)
- [authentication example](examples/authentication/main.go)
  - The plugin accepts authentication creating a secure dialer where credentials are set.
    If the server needs authentication and you do not provide the credentials the complement will panic.

License
==========
See [LICENSE](LICENSE.md)
