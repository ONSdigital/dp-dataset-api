#!/usr/bin/env bash

mongo mongodb://localhost:27017/datasets <<EOF
db.dropDatabase();
db.datasets.insert({
    "_id": "123",
    "name": "CPI",
    "next_release": "2018-08-23",
    "links": {
        "self": "/datasets/123",
        "editions": [
            {
                "id": "2016",
                "url": "/datasets/123/editions/2016"
            },
            {
                "id": "2017",
                "url": "/datasets/123/editions/2017"
            }
        ]
    },
    "contact": {
        "email": "jsinclair@test.co.uk",
        "name": "john sinclair",
        "telephone": "01633 123456"
    }
});
db.editions.insert({
    "_id": "123_2016",
    "name": "CPI",
    "edition": "2016",
    "links": {
        "self": "/datasets/123/editions/2016",
        "versions": [
            {
                "id": "1",
                "url": "/datasets/123/editions/2016/versions/1"
            },
            {
                "id": "2",
                "url": "/datasets/123/editions/2016/versions/2"
            }
        ]
    }
});
db.editions.insert({
    "_id": "123_2017",
    "name": "CPI",
    "edition": "2017",
    "links": {
        "self": "/datasets/123/editions/2017",
        "versions": [
            {
                "id": "1",
                "url": "/datasets/123/editions/2017/versions/1"
            }
        ]
    }
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
    }
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
    }
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
    }
});
db.contacts.insert({
    "_id": "1",
    "name": "john sinclair",
    "email": "jsinclair@test.co.uk",
    "telephone": "01633 123456"
});
db.createCollection("dimensions")
EOF
