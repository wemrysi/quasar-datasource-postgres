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

import doobie.util.Read

private[datasource] final case class SchemaMeta(schemaName: String)

private[datasource] object SchemaMeta {
  /** Only usable with the ResultSet returned from `DatabaseMetaData#getSchemas`
    *
    * 1. TABLE_SCHEM String => schema name
    * 2. TABLE_CATALOG String => catalog name (may be null)
    */
  implicit val schemaMetaRead: Read[SchemaMeta] =
    new Read[SchemaMeta](Nil, (rs, _) => SchemaMeta(rs.getString(1)))
}
