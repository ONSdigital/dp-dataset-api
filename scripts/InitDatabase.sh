#!/usr/bin/env bash

mongo mongodb://localhost:27017/datasets <<EOF
db.dropDatabase();
db.datasets.insert({
    "_id": "123",
    "name": "CPI",
    "next_release": "2018-08-23",
    "links": {
        "self": "/datasets/123",
        "editions": "/datasets/123/editions"
    },
    "contact": {
        "email": "jsinclair@test.co.uk",
        "name": "john sinclair",
        "telephone": "01633 123456"
    },
    "updated_at": "2017-08-25T15:09:11.829+01:00"
});
db.datasets.insert({
    "_id": "456",
    "name": "CensusEthnicity",
    "next_release": "2021-04-30",
    "links": {
        "self": "/datasets/456",
        "editions": "/datasets/456/editions"
    },
    "contact": {
        "email": "ldavis@test.co.uk",
        "name": "lawrence davis",
        "telephone": "01633 123457"
    },
    "updated_at": "2017-08-25T15:09:11.829+01:00"
});
db.editions.insert({
    "_id": "123_2016",
    "name": "CPI",
    "edition": "2016",
    "links": {
        "self": "/datasets/123/editions/2016",
        "versions": "/datasets/123/editions/2016/versions"
    },
    "updated_at": "2017-08-25T15:09:11.829+01:00"
});
db.editions.insert({
    "_id": "123_2017",
    "name": "CPI",
    "edition": "2017",
    "links": {
        "self": "/datasets/123/editions/2017",
        "versions": "/datasets/123/editions/2017/versions"
    },
    "updated_at": "2017-08-25T15:09:11.829+01:00"
});
db.editions.insert({
    "_id": "456_2011",
    "name": "CensusEthnicity",
    "edition": "2011",
    "links": {
        "self": "/datasets/456/editions/2011",
        "versions": "/datasets/456/editions/2011/versions"
    },
    "updated_at": "2017-08-25T15:09:11.829+01:00"
});
db.versions.insert({
    "_id": "123_2016_1",
    "name": "CPI",
    "edition": "2016",
    "version": "1",
    "release_date": "2016-08-23",
    "links": {
        "self": "/datasets/123/editions/2016/versions/1",
        "dimensions": ""
    },
    "updated_at": "2017-08-25T15:09:11.829+01:00"
});
db.versions.insert({
    "_id": "123_2016_2",
    "name": "CPI",
    "edition": "2016",
    "version": "2",
    "release_date": "2016-08-24",
    "links": {
        "self": "/datasets/123/editions/2016/versions/2",
        "dimensions": ""
    },
    "updated_at": "2017-08-25T15:09:11.829+01:00"
});
db.versions.insert({
    "_id": "123_2017_1",
    "name": "CPI",
    "edition": "2017",
    "version": "1",
    "release_date": "2017-08-23",
    "links": {
        "self": "/datasets/123/editions/2017/versions/1",
        "dimensions": ""
    },
    "updated_at": "2017-08-25T15:09:11.829+01:00"
});
db.versions.insert({
    "_id": "456_2011_1",
    "name": "CPI",
    "edition": "2011",
    "version": "1",
    "release_date": "2011-04-30",
    "links": {
        "self": "/datasets/456/editions/2011/versions/1",
        "dimensions": ""
    },
    "updated_at": "2017-08-25T15:09:11.829+01:00"
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
db.createCollection("dimensions")
EOF
