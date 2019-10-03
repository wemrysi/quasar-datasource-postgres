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

import cats.data._
import cats.effect._
import cats.implicits._

import doobie._
import doobie.hikari.HikariTransactor
import doobie.implicits._

import eu.timepit.refined.auto._

import java.util.concurrent.Executors

import org.slf4s.Logging

import quasar.api.datasource.{DatasourceError => DE, DatasourceType}
import quasar.concurrent.{BlockingContext, NamedDaemonThreadFactory}
import quasar.connector.{LightweightDatasourceModule, MonadResourceErr}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random
import scala.util.control.NonFatal

import scalaz.syntax.tag._

object PostgresDatasourceModule extends LightweightDatasourceModule with Logging {

  type InitErr = DE.InitializationError[Json]

  // The duration to await validation of the initial connection.
  val ValidationTimeout: FiniteDuration = 10.seconds

  // Maximum number of database connections per-datasource.
  val ConnectionPoolSize: Int = 10

  val kind: DatasourceType = DatasourceType("postgres", 1L)

  def sanitizeConfig(config: Json): Json =
    config.as[Config]
      .map(_.sanitized.asJson)
      .getOr(jEmptyObject)

  def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json)(
      implicit ec: ExecutionContext)
      : Resource[F, Either[InitErr, LightweightDatasourceModule.DS[F]]] = {

    val cfg0: Either[InitErr, Config] =
      config.as[Config].fold(
        (err, c) =>
          Left(DE.malformedConfiguration[Json, InitErr](
            kind,
            jString(Redacted),
            err)),

        Right(_))

    val validateConnection: ConnectionIO[Either[InitErr, Unit]] =
      FC.isValid(ValidationTimeout.toSeconds.toInt) map { v =>
        if (!v) Left(connectionInvalid(sanitizeConfig(config))) else Right(())
      }

    val init = for {
      cfg <- EitherT(cfg0.pure[Resource[F, ?]])

      suffix <- EitherT.right(Resource.liftF(Sync[F].delay(Random.alphanumeric.take(6).mkString)))

      awaitPool <- EitherT.right(awaitConnPool[F](s"pgsrc-await-$suffix", ConnectionPoolSize))

      xaPool <- EitherT.right(transactPool[F](s"pgsrc-transact-$suffix"))

      xa <- EitherT.right(hikariTransactor[F](cfg, awaitPool, xaPool))

      _ <- EitherT(Resource.liftF(validateConnection.transact(xa) recover {
        case NonFatal(ex: Exception) =>
          Left(DE.connectionFailed[Json, InitErr](kind, sanitizeConfig(config), ex))
      }))

      _ <- EitherT.right[InitErr](Resource.liftF(Sync[F].delay(
        log.info(s"Initialized postgres datasource: tag = $suffix, config = ${cfg.sanitized.asJson}"))))

    } yield new PostgresDatasource(xa): LightweightDatasourceModule.DS[F]

    init.value
  }

  ////

  private def awaitConnPool[F[_]](name: String, size: Int)(implicit F: Sync[F])
      : Resource[F, ExecutionContext] = {

    val alloc =
      F.delay(Executors.newFixedThreadPool(size, NamedDaemonThreadFactory(name)))

    Resource.make(alloc)(es => F.delay(es.shutdown()))
      .map(ExecutionContext.fromExecutor)
  }

  private def connectionInvalid(c: Json): InitErr =
    DE.connectionFailed[Json, InitErr](
      kind, c, new RuntimeException("Connection is invalid."))

  private def hikariTransactor[F[_]: Async: ContextShift](
      cfg: Config,
      connectPool: ExecutionContext,
      xaPool: BlockingContext)
      : Resource[F, HikariTransactor[F]] = {

    HikariTransactor.initial[F](connectPool, xaPool.unwrap) evalMap { xa =>
      xa.configure { ds =>
        Sync[F] delay {
          ds.setJdbcUrl(s"jdbc:${cfg.connectionUri}")
          ds.setDriverClassName(PostgresDriverFqcn)
          ds.setMaximumPoolSize(ConnectionPoolSize)
          xa
        }
      }
    }
  }

  private def transactPool[F[_]](name: String)(implicit F: Sync[F])
      : Resource[F, BlockingContext] = {

    val alloc =
      F.delay(Executors.newCachedThreadPool(NamedDaemonThreadFactory(name)))

    Resource.make(alloc)(es => F.delay(es.shutdown()))
      .map(es => BlockingContext(ExecutionContext.fromExecutor(es)))
  }
}
