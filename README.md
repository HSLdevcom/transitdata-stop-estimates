[![Build Status](https://travis-ci.org/HSLdevcom/transitdata-stop-estimates.svg?branch=master)](https://travis-ci.org/HSLdevcom/transitdata-stop-estimates)

# Transitdata-stop-estimates

## Description

Application for parsing Stop Estimates from Pubtrans raw data.
This provides a nice abstraction to later combine busses, trains, metros and other transportation methods.
Messages are read from one Pulsar topic and the output is written to another Pulsar topic.

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

### Locally

- ```mvn compile```  
- ```mvn package```  

### Docker image

- Run [this script](build-image.sh) to build the Docker image

## Running

Requirements:
- Local Pulsar Cluster
  - By default uses localhost, override host in PULSAR_HOST if needed.
    - Tip: f.ex if running inside Docker in OSX set `PULSAR_HOST=host.docker.internal` to connect to the parent machine
  - You can use [this script](https://github.com/HSLdevcom/transitdata/blob/master/bin/pulsar/pulsar-up.sh) to launch it as Docker container
