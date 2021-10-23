# Kaspar Core

The core libraries backing Kaspar

## Building/Testing

Kaspar is a maven project so supports the usual goals. However, due to the way in which they are
run, integration tests must be excluded from this build and run separately:

```
    mvn -DskipTests clean package
    mvn -DtagsToExclude=DOCKER test
```

To run integration tests specify the test suite separately e.g.:

```
    mvn -Dsuites=integration.CsvSelectIntegrationTest test
```

## Documentation

Please see the documentation here:

[documentation](https://thomaskwscott.github.io/kaspar/)

