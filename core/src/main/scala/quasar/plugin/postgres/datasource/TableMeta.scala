/*
 * Copyright 2020 Precog Data
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

private[datasource] final case class TableMeta(schemaName: String, tableName: String)

private[datasource] object TableMeta {
  /** Only usable with the ResultSet returned from `DatabaseMetaData#getTables`
    *
    *  1. TABLE_CAT String => table catalog (may be null)
    *  2. TABLE_SCHEM String => table schema (may be null)
    *  3. TABLE_NAME String => table name
    *  4. TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
    *  5. REMARKS String => explanatory comment on the table
    *  6. TYPE_CAT String => the types catalog (may be null)
    *  7. TYPE_SCHEM String => the types schema (may be null)
    *  8. TYPE_NAME String => type name (may be null)
    *  9. SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
    * 10. REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null)
    */
  implicit val tableMetaRead: Read[TableMeta] =
    new Read[TableMeta](Nil, (rs, _) => TableMeta(rs.getString(2), rs.getString(3)))
}
