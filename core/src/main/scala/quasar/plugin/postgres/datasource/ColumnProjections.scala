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

import slamdata.Predef.{Eq => _, _}

import cats._
import cats.data.NonEmptyList
import cats.implicits._

sealed trait ColumnProjections extends Product with Serializable

object ColumnProjections {
  final case class Explicit(names: NonEmptyList[Ident]) extends ColumnProjections
  case object All extends ColumnProjections

  implicit val columnProjectionsEq: Eq[ColumnProjections] =
    Eq by {
      case Explicit(ns) => Some(ns)
      case All => None
    }

  implicit val columnProjectionsShow: Show[ColumnProjections] =
    Show.fromToString
}
