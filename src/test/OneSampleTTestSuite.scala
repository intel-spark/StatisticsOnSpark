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

import org.apache.commons.math3.stat.inference.TestUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by yuhao on 12/31/15.
  */
object OneSampleTTestSuite {

  def main(args: Array[String]) {
    val observed = Array(100d, 200d, 300d, 400d)
    val mu = 2.5d

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("TallSkinnySVD").setMaster("local")
    val sc = new SparkContext(conf)

    assert(TestUtils.tTest(mu, observed, 0.05) == new OneSampleTTest().tTest(mu, sc.parallelize(observed), 0.05))
    assert(TestUtils.tTest(mu, observed) == new OneSampleTTest().tTest(mu, sc.parallelize(observed)))
  }

}
