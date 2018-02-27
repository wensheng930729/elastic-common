# elastic-common
Elasticsearch Common Module

#### Setting up the Project

* Install Gradle Version 4.5.1 from https://gradle.org/install/
* Git clone this project `git clone https://github.com/polyglotted/elastic-common` and cd into it
* Run ESDocker docker container `docker run -d --name esdocker -p 9200:9200 -e ELASTIC_PASSWORD=SteelEye steeleye/esdocker:6.2.2`
* Build and Test `./gradlew clean check`
* Generate IDEA files `gradle idea`

#### Continuous Integration
[![CircleCI](https://circleci.com/gh/polyglotted/elastic-common.svg?style=shield)](https://circleci.com/gh/polyglotted/elastic-common)