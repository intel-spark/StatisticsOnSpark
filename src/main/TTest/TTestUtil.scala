/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.mllib.stat

import org.apache.commons.math3.distribution.TDistribution
import org.apache.spark.rdd.RDD

/**
  * Created by yuhao on 12/31/15.
  */
trait TTestBasic {

  private[stat] def t(m: Double, mu: Double, v: Double, n: Long): Double = {
    val t = math.abs((m - mu) / math.sqrt(v / n))
    val TDistribution = new TDistribution(null, n - 1.0D)
    return 2.0D * TDistribution.cumulativeProbability(-t)
  }

}
