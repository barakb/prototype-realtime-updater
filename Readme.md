[![Build Status](https://www.travis-ci.com/barakb/prototype-realtime-updater.svg?branch=master)](https://www.travis-ci.com/barakb/prototype-realtime-updater)

# Getting Started


### Configuration
The configuration file is `application.yaml` in the resource directory.
All configuration is done using spring-boot convention so environment variables, system properties can be used to override the configuration.
Logs are configured as well from the same configuration file.

### Running/Debugging
`mvn spring-boot:run`
To debug you will only to define a maven runner with the same maven command in the idea.

### Browsing the API using Open API
http://localhost:8080/swagger-ui.html
The API is used to trigger actions like adding/removing service, list active pipelines and publish messages to a service topic.

### Management
http://localhost:8080/management

