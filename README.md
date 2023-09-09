<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/the_cube_dev)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js Mongo Atlas JDBC Database Driver

This driver is based on the JDBC driver to connect to Mongo Atlas SQL (https://www.mongodb.com/atlas/sql). More details on the supported sql interface can be found here: https://www.mongodb.com/docs/atlas/data-federation/query/sql/language-reference

To use this driver you need to configure it as follows:

- CUBEJS_JDBC_URL=[JDBC url from Mongo Atlas]
- CUBEJS_DB_PASS=[password]
- CUBEJS_DB_PORT=27017
- CUBEJS_DB_NAME=[username]
- CUBEJS_DB_SSL=true

This drivers improves the default JDBC driver by extracting metadata from the JDBC metadata instead of the values from the query.

This software is alpha software and should not be used in production.

To run this driver use the following Dockerfile:

    FROM cubejs/cube:jdk
    RUN echo '{}' > package.json
    
    RUN npm install mongo-atlas-jdbc-cubejs-driver


Github url: https://github.com/xip-online-applications/cube-js-mongo-atlas-driver




