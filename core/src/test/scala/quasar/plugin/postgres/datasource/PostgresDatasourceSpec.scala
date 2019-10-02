/*
 * Copyright 2014–2019 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.plugin.postgres.datasource

import slamdata.Predef._

import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.util.ExecutionContexts

import fs2.Stream

import org.specs2.specification.BeforeAfterAll

import quasar.api.resource.{ResourcePathType => RPT, _}
import quasar.connector._
import quasar.contrib.scalaz.MonadError_

import scala.concurrent.ExecutionContext.Implicits.global

object PostgresDatasourceSpec
    extends DatasourceSpec[IO, Stream[IO, ?], RPT.Physical]
    with BeforeAfterAll {

  sequential

  // Uncomment to log all queries to STDOUT
  //implicit val doobieLogHandler: LogHandler = LogHandler.jdkLogHandler

  implicit val ioContextShift: ContextShift[IO] =
    IO.contextShift(global)

  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val xa = Transactor.fromDriverManager[IO](
    PostgresDriverFqcn,
    "jdbc:postgresql://localhost:54322/postgres?user=postgres&password=postgres",
    ExecutionContexts.synchronous)

  val datasource = new PostgresDatasource(xa)

  val nonExistentPath = ResourcePath.root() / ResourceName("schemadne") / ResourceName("tabledne")

  def gatherMultiple[A](s: Stream[IO, A]) = s.compile.toList

  def beforeAll(): Unit = {
    val setup = for {
      _ <- sql"""CREATE SCHEMA "pgsrcSchemaA"""".update.run
      _ <- sql"""CREATE SCHEMA "pgsrcSchemaB"""".update.run

      _ <- sql"""CREATE TABLE "pgsrcSchemaA"."tableA1" (x SMALLINT, y SMALLINT)""".update.run
      _ <- sql"""CREATE TABLE "pgsrcSchemaA"."tableA2" (a TEXT, b INT)""".update.run

      _ <- sql"""CREATE TABLE "pgsrcSchemaB"."tableB1" (foo VARCHAR, bar BOOLEAN)""".update.run
      _ <- sql"""CREATE TABLE "pgsrcSchemaB"."tableB2" (ts TIMESTAMP)""".update.run

      _ <- sql"""CREATE TABLE "pgsrcPublic1" (name VARCHAR, age INT)""".update.run
    } yield ()

    setup.transact(xa).unsafeRunSync()
  }

  def afterAll(): Unit = {
    val teardown = for {
      _ <- sql"""DROP SCHEMA "pgsrcSchemaA" CASCADE""".update.run
      _ <- sql"""DROP SCHEMA "pgsrcSchemaB" CASCADE""".update.run
      _ <- sql"""DROP TABLE "pgsrcPublic1"""".update.run
    } yield ()

    teardown.transact(xa).unsafeRunSync()
  }

  "schema handling" >> {
    "public tables are resources under /public, by default" >>* {
      datasource
        .pathIsResource(ResourcePath.root() / ResourceName("public") / ResourceName("pgsrcPublic1"))
        .map(_ must beTrue)
    }

    "tables in created schemas are resources" >>* {
      datasource
        .pathIsResource(ResourcePath.root() / ResourceName("pgsrcSchemaB") / ResourceName("tableB1"))
        .map(_ must beTrue)
    }
  }

  "naming" >> {
    def mustBeAResource(schema: String, table: String) = {
      val setup = for {
        _ <- (fr"CREATE SCHEMA" ++ Fragment.const0(s""""$schema"""")).update.run
        _ <- (fr"CREATE TABLE" ++ Fragment.const(s""""$schema"."$table"""") ++ fr0"(value varchar)").update.run
      } yield ()

      val teardown =
        (fr"DROP SCHEMA" ++ Fragment.const(s""""$schema"""") ++ fr0"CASCADE").update.run

      Resource.make(setup)(_ => teardown.void)
        .mapK(xa.trans)
        .use(_ => datasource.pathIsResource(ResourcePath.root() / ResourceName(schema) / ResourceName(table)))
        .map(_ must beTrue)
    }

    "support schema names containing '_'" >>* mustBeAResource("some_schema_x", "tablex")

    "support schema names containing '%'" >>* mustBeAResource("some%schema%y", "tabley")

    "support table names containing '_'" >>* mustBeAResource("someschemau", "table_q")

    "support table names containing '%'" >>* mustBeAResource("someschemav", "table%r")
  }

  "evaluation" >> {
    "path with length 0 fails with not a resource" >> todo

    "path with length 1 fails with not a resource" >> todo

    "path with length 3 fails with not a resource" >> todo

    "path with length 2 that isn't a table fails with resource not found" >> todo

    "path to extant empty table returns empty results" >> todo

    "path to extant non-empty table returns rows as line-delimited json" >> todo
  }
}
