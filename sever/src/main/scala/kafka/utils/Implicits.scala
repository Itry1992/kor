/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import java.util
import java.util.Properties
import scala.annotation.nowarn
import scala.jdk.CollectionConverters._


/**
 * In order to have these implicits in scope, add the following import:
 *
 * `import kafka.utils.Implicits._`
 */
object Implicits {

  /**
   * The java.util.Properties.putAll override introduced in Java 9 is seen as an overload by the
   * Scala compiler causing ambiguity errors in some cases. The `++=` methods introduced via
   * implicits provide a concise alternative.
   *
   * See https://github.com/scala/bug/issues/10418 for more details.
   */
  implicit class PropertiesOps(properties: Properties) {

    def ++=(props: Properties): Unit =
      (properties: util.Hashtable[AnyRef, AnyRef]).putAll(props)

    def ++=(map: collection.Map[String, AnyRef]): Unit =
      (properties: util.Hashtable[AnyRef, AnyRef]).putAll(map.asJava)

  }

  /**
   * Exposes `forKeyValue` which maps to `foreachEntry` in Scala 2.13 and `foreach` in Scala 2.12
   * (with the help of scala.collection.compat). `foreachEntry` avoids the tuple allocation and
   * is more efficient.
   *
   * This was not named `foreachEntry` to avoid `unused import` warnings in Scala 2.13 (the implicit
   * would not be triggered in Scala 2.13 since `Map.foreachEntry` would have precedence).
   */
  @nowarn("cat=unused-imports")
  implicit class MapExtensionMethods[K, V](private val self: scala.collection.Map[K, V]) extends AnyVal {

    def forKeyValue[U](f: (K, V) => U): Unit = {
      self.foreachEntry { (k, v) => f(k, v) }
    }

  }

}
