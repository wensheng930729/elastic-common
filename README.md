# elastic-common
Elasticsearch Common Module

#### Setting up the Project

* Install Gradle Version 4.5.1 from https://gradle.org/install/
* Git clone this project `git clone https://github.com/polyglotted/elastic-common` and cd into it
* Run ESDocker docker container `docker run -d --name esdocker -p 9200:9200 -e ELASTIC_PASSWORD=SteelEye steeleye/esdocker:6.2.2`
* Build and Test `./gradlew clean check`
* Generate IDEA files `gradle idea`

#### Continuous Integration

[![CircleCI](https://circleci.com/gh/polyglotted/elastic-common.svg?style=shield)](https://circleci.com/gh/polyglotted/elastic-common)  [![codecov](https://codecov.io/gh/polyglotted/elastic-common/branch/master/graph/badge.svg?style=shield)](https://codecov.io/gh/polyglotted/elastic-common)  [![Known Vulnerabilities](https://snyk.io/test/github/polyglotted/elastic-common/badge.svg?targetFile=build.gradle&style=shield)](https://snyk.io/test/github/polyglotted/elastic-common?targetFile=build.gradle)  [![Maintainability](https://api.codeclimate.com/v1/badges/86bdbe393dd41417bcf8/maintainability)](https://codeclimate.com/github/polyglotted/elastic-common/maintainability)
                                                                                                                                 
#### Distribution

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=shield)](https://opensource.org/licenses/Apache-2.0) 