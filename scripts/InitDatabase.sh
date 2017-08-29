#!/usr/bin/env bash

mongo mongodb://localhost:27017/datasets <<EOF
db.dropDatabase();
db.datasets.insert({
    "contact": {
        "email": "jsinclair@test.co.uk",
        "name": "john sinclair",
        "telephone": "01633 123456"
    },
    "dataset_id": "123",
    "description": "census covers the ethnicity of people living in the uk",
    "_id": "95c4669b-3ae9-4ba7-b690-87e890a1c67c",
    "links": {
        "editions": "/datasets/123/editions",
        "latest_version": "/dataset/123/editions/2016/versions/1",
        "self": "/datasets/123"
    },
    "next_release": "2018-08-23",
    "periodicity": "yearly",
    "publisher": {
        "name": "The office of national statistics",
        "type": "goverment department",
        "url": "https://www.ons.gov.uk/"
    },
    "state": "published",
    "theme": "population",
    "title": "CPI",
    "updated_at": "2017-08-25T15:09:11.829+01:00"
});
db.datasets.insert({
    "contact": {
        "email": "ldavis@test.co.uk",
        "name": "lawrence davis",
        "telephone": "01633 123457"
    },
    "dataset_id": "456",
    "description": "census covers the ethnicity of people living in the uk",
    "_id": "a9fa845c-0c05-4954-aed7-752b8208da34",
    "links": {
        "latest_version": "/dataset/456/editions/2011/versions/1",
        "editions": "/datasets/456/editions",
        "self": "/datasets/456"
    },
    "next_release": "2021-04-30",
    "periodicity": "yearly",
    "publisher": {
        "name": "The office of national statistics",
        "type": "goverment department",
        "url": "https://www.ons.gov.uk/"
    },
    "theme": "population",
    "title": "CensusEthnicity",
    "state": "unpublished",
    "updated_at": "2017-08-25T15:09:11.829+01:00"
});
db.editions.insert({
    "edition": "2016",
    "_id": "a051a058-58a9-4ba4-8374-fbb7315d3b78",
    "links": {
        "dataset": "/datasets/123",
        "self": "/datasets/123/editions/2016",
        "versions": "/datasets/123/editions/2016/versions"
    },
    "state": "published",
    "updated_at": "2017-08-25T15:09:11.829+01:00"
});
db.editions.insert({
    "edition": "2017",
    "_id": "8af20615-c4c5-4bb9-af35-a2530e5a2433",
    "links": {
        "dataset": "/datasets/123",
        "self": "/datasets/123/editions/2017",
        "versions": "/datasets/123/editions/2017/versions"
    },
    "state": "unpublished",
    "updated_at": "2017-08-25T15:09:11.829+01:00"
});
db.editions.insert({
    "_id": "2dc3a321-2c31-4a8a-9a8d-7962d7590ed3",
    "edition": "2011",
    "links": {
        "dataset": "/datasets/123",
        "self": "/datasets/456/editions/2011",
        "versions": "/datasets/456/editions/2011/versions"
    },
    "state": "unpublished",
    "updated_at": "2017-08-25T15:09:11.829+01:00"
});
db.versions.insert({
    "edition": "2016",
    "_id": "63294ed7-dccf-4f30-ad57-62365f038fb7",
    "License": "ONS",
    "links": {
        "dataset": "/datasets/123",
        "edition": "/datasets/123/editions/2016",
        "self": "/datasets/123/editions/2016/versions/1",
        "dimensions": ""
    },
    "release_date": "2016-08-23",
    "state": "published",
    "updated_at": "2017-08-25T15:09:11.829+01:00",
    "version": "1"
});
db.versions.insert({
    "edition": "2016",
    "_id": "4ce2ee5c-d50b-469f-b005-ddfed3f5072b",
    "License": "ONS",
    "links": {
        "dataset": "/datasets/123",
        "edition": "/datasets/123/editions/2016",
        "self": "/datasets/123/editions/2016/versions/2",
        "dimensions": ""
    },
    "release_date": "2016-08-24",
    "state": "unpublished",
    "updated_at": "2017-08-25T15:09:11.829+01:00",
    "version": "2"
});
db.versions.insert({
    "edition": "2017",
    "_id": "679ebe5f-d9cd-4d6e-8afc-6a2a4f991ccf",
    "License": "ONS",
    "links": {
        "dataset": "/datasets/123",
        "edition": "/datasets/123/editions/2017",
        "self": "/datasets/123/editions/2017/versions/1",
        "dimensions": ""
    },
    "release_date": "2017-08-23",
    "state": "unpublished",
    "updated_at": "2017-08-25T15:09:11.829+01:00",
    "version": "1"
});
db.versions.insert({
    "edition": "2011",
    "_id": "3b45921b-0efa-4844-a16e-3f9f30df4f88",
    "License": "ONS",
    "links": {
        "dataset": "/datasets/123",
        "edition": "/datasets/123/editions/2011",
        "self": "/datasets/456/editions/2011/versions/1",
        "dimensions": ""
    },
    "release_date": "2011-04-30",
    "state": "unpublished",
    "updated_at": "2017-08-25T15:09:11.829+01:00",
    "version": "1"
});
db.contacts.insert({
    "_id": "1",
    "name": "john sinclair",
    "email": "jsinclair@test.co.uk",
    "telephone": "01633 123456",
    "updated_at": "2017-08-25T15:09:11.829+01:00"
});
db.contacts.insert({
    "_id": "2",
    "name": "lawrence davis",
    "email": "ldavis@test.co.uk",
    "telephone": "01633 123457",
    "updated_at": "2017-08-25T15:09:11.829+01:00"
});
db.createCollection("dimensions");
db.datasets.ensureIndex({"dataset_id":1},{"background":true});
db.editions.ensureIndex({"links.dataset":1},{"background":true});
db.versions.ensureIndex({"links.dataset":1},{"background":true});
db.versions.ensureIndex({"links.edition":1},{"background":true})
EOF
