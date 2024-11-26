# Transitdata-stop-estimates [![Test and create Docker image](https://github.com/HSLdevcom/transitdata-stop-estimates/actions/workflows/test-and-build.yml/badge.svg)](https://github.com/HSLdevcom/transitdata-stop-estimates/actions/workflows/test-and-build.yml)

## Description

Application for creating abstract StopEstimates from raw estimates sourced from PubTrans or Metro ATS.
This provides a nice abstraction to later combine busses, trains, metros and other transportation methods.
Messages are read from one Pulsar topic and the output is written to another Pulsar topic.

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

### Locally

- `mvn compile`
- `mvn package`

### Docker image

- Run [this script](build-image.sh) to build the Docker image

## Running

### Dependencies

* Pulsar

### Environment variables

* `SOURCE`: specifies which configuration file to use from resources folder, currently available: `metro-estimate.conf` and `ptroi.conf`
