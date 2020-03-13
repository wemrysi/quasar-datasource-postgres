# quasar-datasource-postgres [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

## Usage

```sbt
libraryDependencies += "com.precog" %% "quasar-datasource-postgres" % <version>
```

## Configuration

```json
{
  "connectionUri": String
  [, "connectionPoolSize": Int]
}
```

* `connectionUri`: the [PostgreSQL connection URL](https://jdbc.postgresql.org/documentation/head/connect.html) _without the leading `jdbc:`_.
* `connectionPoolSize` (optional): The size of the connection pool to use per-datasource, the default is 10 and should be sufficient for most deployments.
