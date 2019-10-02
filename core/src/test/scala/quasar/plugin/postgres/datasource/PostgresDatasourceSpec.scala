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

import argonaut._, Argonaut._
import argonaut.JawnParser.facade

import cats.~>
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.util.ExecutionContexts

import fs2.Stream

import jawnfs2._

import org.specs2.specification.BeforeAfterAll

import quasar.ScalarStages
import quasar.api.resource.{ResourcePathType => RPT, _}
import quasar.connector.{ResourceError => RE, _}
import quasar.contrib.scalaz.MonadError_
import quasar.qscript.InterpretedRead

import scala.concurrent.ExecutionContext.Implicits.global

import shims._

object PostgresDatasourceSpec
    extends DatasourceSpec[IO, Stream[IO, ?], RPT.Physical]
    with BeforeAfterAll {

  sequential

  // Uncomment to log all queries to STDOUT
  //implicit val doobieLogHandler: LogHandler = LogHandler.jdkLogHandler

  implicit val ioContextShift: ContextShift[IO] =
    IO.contextShift(global)

  implicit val ioMonadResourceErr: MonadError_[IO, RE] =
    MonadError_.facet[IO](RE.throwableP)

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

      xa.connect(xa.kernel)
        .flatMap(c =>
          Resource.make(setup)(_ => teardown.void)
            .mapK(λ[ConnectionIO ~> IO](_.foldMap(xa.interpret).run(c))))
        .use(_ => datasource.pathIsResource(ResourcePath.root() / ResourceName(schema) / ResourceName(table)))
        .map(_ must beTrue)
    }

    "support schema names containing '_'" >>* mustBeAResource("some_schema_x", "tablex")

    "support schema names containing '%'" >>* mustBeAResource("some%schema%y", "tabley")

    "support table names containing '_'" >>* mustBeAResource("someschemau", "table_q")

    "support table names containing '%'" >>* mustBeAResource("someschemav", "table%r")
  }

  "evaluation" >> {
    "path with length 0 fails with not a resource" >>* {
      val ir = InterpretedRead(ResourcePath.root(), ScalarStages.Id)

      MonadResourceErr.attempt(datasource.evaluate(ir))
        .map(_.toEither must beLeft(RE.notAResource(ir.path)))
    }

    "path with length 1 fails with not a resource" >>* {
      val ir = InterpretedRead(
        ResourcePath.root() / ResourceName("a"),
        ScalarStages.Id)

      MonadResourceErr.attempt(datasource.evaluate(ir))
        .map(_.toEither must beLeft(RE.notAResource(ir.path)))
    }

    "path with length 3 fails with not a resource" >>* {
      val ir = InterpretedRead(
        ResourcePath.root() / ResourceName("x") / ResourceName("y") / ResourceName("z"),
        ScalarStages.Id)

      MonadResourceErr.attempt(datasource.evaluate(ir))
        .map(_.toEither must beLeft(RE.notAResource(ir.path)))
    }

    "path with length 2 that isn't a table fails with path not found" >>* {
      val ir = InterpretedRead(nonExistentPath, ScalarStages.Id)

      MonadResourceErr.attempt(datasource.evaluate(ir))
        .map(_.toEither must beLeft(RE.pathNotFound(ir.path)))
    }

    "path to extant empty table returns empty results" >>* {
      val ir = InterpretedRead(
        ResourcePath.root() / ResourceName("pgsrcSchemaA") / ResourceName("tableA1"),
        ScalarStages.Id)

      datasource.evaluate(ir) flatMap {
        case QueryResult.Typed(fmt, bs, stages) =>
          fmt must_=== DataFormat.ldjson
          stages must_=== ScalarStages.Id
          bs.compile.toList map (_ must beEmpty)

        case _ => IO.pure(ko("Expected QueryResult.Typed"))
      }
    }

    "path to extant non-empty table returns rows as line-delimited json" >>* {
      val path =
        ResourcePath.root() / ResourceName("pgsrcSchemaA") / ResourceName("widgets")

      val widgets = List(
        Widget("X349", 34.23, 12.5),
        Widget("XY34", 64.25, 23.1),
        Widget("CR12", 0.023, 40.33))

      val setup = for {
        _ <- sql"""DROP TABLE IF EXISTS "pgsrcSchemaA"."widgets"""".update.run
        _ <- sql"""CREATE TABLE "pgsrcSchemaA"."widgets" (serial VARCHAR, width DOUBLE PRECISION, height DOUBLE PRECISION)""".update.run
        load = """INSERT INTO "pgsrcSchemaA"."widgets" (serial, width, height) VALUES (?, ?, ?)"""
        _ <- Update[Widget](load).updateMany(widgets)
      } yield ()

      for {
        _ <- setup.transact(xa)

        qr <- datasource.evaluate(InterpretedRead(path, ScalarStages.Id))

        ws <- qr match {
          case QueryResult.Typed(_, s, _) =>
            s.chunks.parseJsonStream[Json]
              .map(_.as[Widget].toOption)
              .unNone
              .compile.toList

          case _ =>
            IO.pure(List.empty[Widget])
        }
      } yield ws must containTheSameElementsAs(widgets)
    }
  }

  private final case class Widget(serial: String, width: Double, height: Double)

  private object Widget {
    implicit val widgetCodecJson: CodecJson[Widget] =
      casecodec3(Widget.apply, Widget.unapply)("serial", "width", "height")
  }
}
