Run tests:

Unit:

1.  mvn -DtagsToExclude=DOCKER test

Integration:

1. mvn -DskipTests clean package     <-- needed for testcontainers deployment
2. e.g. mvn -Dsuites=integration.CsvSelectIntegrationTest test <-- individual tests