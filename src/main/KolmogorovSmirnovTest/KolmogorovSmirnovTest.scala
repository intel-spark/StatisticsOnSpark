/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.mllib.stat.test

import org.apache.commons.math3.util.FastMath

/**
 * We just found out KolmogorovSmirnovTest was included in Spark 1.6.
 * Instead of providing a new implementation, it better aligns with
 * users' interests if we can provide improvement or new functions based
 * on the Spark version. If you find something potentially useful yet missing
 * from Spark, please go ahead and create an issue in the project.
 *
 */
object KolmogorovSmirnovTest {

  def ksSum (t: Double, tolerance: Double, maxIterations: Int): Double = {
    if (t == 0.0) {
      return 0.0
    }
    val x: Double = -2 * t * t
    var sign: Int = -1
    var i: Long = 1
    var partialSum: Double = 0.5d
    var delta: Double = 1
    while (delta > tolerance && i < maxIterations) {
      delta = FastMath.exp(x * i * i)
      partialSum += sign * delta
      sign *= -1
      i += 1
    }
    partialSum * 2
  }
}

