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
class OneSampleTTest extends TTestBasic {
  /**
    * Performs a  two-sided t-test evaluating the null hypothesis that the mean of the population from
    * which sample is drawn equals mu
    * Returns true iff the null hypothesis can be
    * rejected with confidence 1 - alpha.  To
    * perform a 1-sided test, use alpha * 2

    * To test the (2-sided) hypothesis sample mean = mu at
    * the 95% level, use tTest(mu, sample, 0.05)
    *
    * To test the (one-sided) hypothesis  sample mean < mu
    * at the 99% level, first verify that the measured sample mean is less
    * than mu and then use
    * tTest(mu, sample, 0.02)
    *
    * @param mu constant value to compare sample mean against
    * @param sample array of sample data values
    * @param alpha significance level of the test
    * @return p-value
    */
  def tTest(mu: Double, sample: RDD[Double], alpha:Double): Boolean = {
    tTest(mu, sample) < alpha
  }

  /**
    * Returns the observed significance level, or
    * p-value, associated with a one-sample, two-tailed t-test
    * comparing the mean of the input array with the constant mu.
    *
    * The number returned is the smallest significance level
    * at which one can reject the null hypothesis that the mean equals
    * mu in favor of the two-sided alternative that the mean
    * is different from mu. For a one-sided test, divide the
    * returned value by 2.
    *
    * @param mu constant value to compare sample mean against
    * @param sample array of sample data values
    * @return p-value
    */
  def tTest(mu: Double, sample: RDD[Double]): Double = {
    val n = sample.count()
    val mean = sample.sum() / n
    val variance = sample.map(d => (d - mean) * (d - mean)).sum() / (n - 1)
    t(mean, mu, variance, n)
  }

}
