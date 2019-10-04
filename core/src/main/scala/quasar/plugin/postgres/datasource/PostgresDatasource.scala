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

import cats.~>
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.{text, Pull, Stream}

import quasar.api.datasource.DatasourceType
import quasar.api.resource.{ResourcePathType => RPT, _}
import quasar.connector.{ResourceError => RE, _}
import quasar.connector.datasource.LightweightDatasource
import quasar.qscript.InterpretedRead

import shims._

final class PostgresDatasource[F[_]: Bracket[?[_], Throwable]: MonadResourceErr: Sync](
    xa: Transactor[F])
    extends LightweightDatasource[F, Stream[F, ?], QueryResult[F]] {

  import PostgresDatasource._

  type Ident = String
  type Schema = Ident
  type Table = Ident

  val kind: DatasourceType = PostgresDatasourceModule.kind

  def evaluate(ir: InterpretedRead[ResourcePath]): F[QueryResult[F]] =
    pathToLoc(ir.path) match {
      case Some(Right((schema, table))) =>
        val back = tableExists(schema, table) map { exists =>
          if (exists)
            Right(tableAsJsonBytes(schema, table))
          else
            Left(RE.pathNotFound[RE](ir.path))
        }

        xa.connect(xa.kernel)
          .evalMap(c => runCIO(c)(back.map(_.map(_.translate(runCIO(c))))))
          .allocated
          .flatMap {
            case (Right(s), release) =>
              QueryResult.typed(DataFormat.ldjson, s.onFinalize(release), ir.stages).pure[F]

            case (Left(re), release) =>
              release >> MonadResourceErr[F].raiseError(re)
          }

      case _ =>
        MonadResourceErr[F].raiseError(RE.notAResource(ir.path))
    }

  def pathIsResource(path: ResourcePath): F[Boolean] =
    pathToLoc(path) match {
      case Some(Right((schema, table))) =>
        tableExists(schema, table).transact(xa)

      case _ => false.pure[F]
    }

  def prefixedChildPaths(prefixPath: ResourcePath): F[Option[Stream[F, (ResourceName, RPT.Physical)]]] =
    if (prefixPath === ResourcePath.Root)
      allSchemas
        .map(n => (ResourceName(n), RPT.prefix))
        .transact(xa)
        .some
        .pure[F]
    else
      pathToLoc(prefixPath) match {
        case Some(Right((schema, table))) =>
          tableExists(schema, table)
            .map(p => if (p) Some(Stream.empty.covaryAll[F, (ResourceName, RPT.Physical)]) else None)
            .transact(xa)

        case Some(Left(schema)) =>
          val l = Some(schema).filterNot(containsWildcard).map(Left(_))

          val paths =
            tables(l)
              .filter(_.schemaName === schema)
              .map(m => (ResourceName(m.tableName), RPT.leafResource))
              .pull.peek1
              .flatMap(t => Pull.output1(t.map(_._2)))
              .stream

          val back = for {
            c <- xa.connect(xa.kernel)
            opt <- paths.translate(runCIO(c)).compile.resource.lastOrError
          } yield opt.map(_.translate(runCIO(c)))

          back.allocated flatMap {
            case (o @ None, release) => release.as(o)
            case (Some(s), release) => s.onFinalize(release).some.pure[F]
          }

        case None => (None: Option[Stream[F, (ResourceName, RPT.Physical)]]).pure[F]
      }

  ////

  private type Loc = Option[Either[Schema, (Schema, Table)]]

  // Characters considered as pattern placeholders in
  // `DatabaseMetaData#getTables`
  private val Wildcards: Set[Char] = Set('_', '%')

  // We use `tables` here to constrain discovered schemas
  // to be only those containing visible tables.
  private def allSchemas: Stream[ConnectionIO, String] =
    tables(None)
      .scan((Set.empty[String], None: Option[String])) {
        case ((seen, _), TableMeta(schema, _)) =>
          if (seen(schema))
            (seen, None)
          else
            (seen + schema, Some(schema))
      }
      .map(_._2)
      .unNone

  /** Returns a `Loc` representing the most specific tables selector.
    *
    * To be a tables selector, an `Ident` must not contain any of the wildcard characters.
    */
  private def locSelector(schema: Schema, table: Table): Loc =
    (containsWildcard(schema), containsWildcard(table)) match {
      case (true, _) => None
      case (false, true) => Some(Left(schema))
      case (false, false) => Some(Right((schema, table)))
    }

  private def containsWildcard(s: String): Boolean =
    s.exists(Wildcards)

  /** Returns a quoted and escaped version of `ident`. */
  private def hygienicIdent(ident: Ident): Ident =
    s""""${ident.replace("\"", "\"\"")}""""

  private def pathToLoc(rp: ResourcePath): Loc =
    Some(rp) collect {
      case schema /: ResourcePath.Root => Left(schema)
      case schema /: table /: ResourcePath.Root => Right((schema, table))
    }

  private def runCIO(c: java.sql.Connection): ConnectionIO ~> F =
    λ[ConnectionIO ~> F](_.foldMap(xa.interpret).run(c))

  private def tableAsJsonBytes(schema: Schema, table: Table): Stream[ConnectionIO, Byte] = {
    val schemaFr = Fragment.const0(hygienicIdent(schema))
    val tableFr = Fragment.const(hygienicIdent(table))
    val sql = fr"SELECT to_json(t) FROM" ++ schemaFr ++ fr0"." ++ tableFr ++ fr0"AS t"

    sql.query[String].stream.intersperse("\n").through(text.utf8Encode)
  }

  private def tableExists(schema: Schema, table: Table): ConnectionIO[Boolean] =
    tables(locSelector(schema, table))
      .exists(m => m.schemaName === schema && m.tableName === table)
      .compile
      .lastOrError

  private def tables(selector: Loc): Stream[ConnectionIO, TableMeta] = {
    val (selectSchema, selectTable) = selector match {
      case Some(Left(s)) => (s, "%")
      case Some(Right((s, t))) => (s, t)
      case None => (null, "%")
    }

    Stream.force(for {
      catalog <- HC.getCatalog
      rs <- HC.getMetaData(FDMD.getTables(catalog, selectSchema, selectTable, VisibleTableTypes))
      ts = HRS.stream[TableMeta](MetaChunkSize)
    } yield ts.translate(λ[ResultSetIO ~> ConnectionIO](FC.embed(rs, _))))
  }
}

object PostgresDatasource {

  /** The chunk size used for metadata streams. */
  val MetaChunkSize: Int = 10//1024

  /** The types of tables that should be visible during discovery.
    *
    * Available types from DatabaseMetaData#getTableTypes
    *
    * FOREIGN TABLE, INDEX, MATERIALIZED VIEW, SEQUENCE, SYSTEM INDEX,
    * SYSTEM TABLE, SYSTEM TOAST INDEX, SYSTEM TOAST TABLE, SYSTEM VIEW,
    * TABLE, TEMPORARY INDEX, TEMPORARY SEQUENCE, TEMPORARY TABLE,
    * TEMPORARY VIEW, TYPE, VIEW
    */
  val VisibleTableTypes: Array[String] =
    Array(
      "FOREIGN TABLE",
      "MATERIALIZED VIEW",
      "SYSTEM TABLE",
      "SYSTEM TOAST TABLE",
      "SYSTEM VIEW",
      "TABLE",
      "TEMPORARY TABLE",
      "TEMPORARY VIEW",
      "VIEW")
}
