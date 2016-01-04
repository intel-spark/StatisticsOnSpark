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
import org.apache.commons.math3.util.FastMath
import org.apache.spark.rdd.RDD

/**
  * Created by yuhao on 12/31/15.
  */
class TwoSampleIndependentTTest {
  /**
    * Performs a two-sided t-test evaluating the null hypothesis that sample1
    * and sample2 are drawn from populations with the same mean,
    * with significance level alpha.  This test does not assume
    * that the subpopulation variances are equal.
    * Returns true iff the null hypothesis that the means are
    * equal can be rejected with confidence 1 - alpha.  To
    * perform a 1-sided test, use alpha * 2
    * To test the (2-sided) hypothesis mean 1 = mean 2 at
    * the 95% level,  use
    * tTest(sample1, sample2, 0.05).
    * To test the (one-sided) hypothesis mean 1 < mean 2,
    * at the 99% level, first verify that the measured  mean of sample 1
    * is less than the mean of sample 2 and then use
    * tTest(sample1, sample2, 0.02)
    * @param sample1 array of sample data values
    * @param sample2 array of sample data values
    * @param alpha significance level of the test
    * @return true if the null hypothesis can be rejected with
    *         confidence 1 - alpha
    */
  def tTest(sample1: RDD[Double], sample2: RDD[Double], alpha: Double): Boolean = {
    tTest(sample1, sample2) < alpha
  }

  /**
    * Returns the observed significance level, or
    * p-value, associated with a two-sample, two-tailed t-test
    * comparing the means of the input arrays.
    *
    * The number returned is the smallest significance level
    * at which one can reject the null hypothesis that the two means are
    * equal in favor of the two-sided alternative that they are different.
    * For a one-sided test, divide the returned value by 2.
    *
    * @param sample1 array of sample data values
    * @param sample2 array of sample data values
    * @return p-value for t-test
    */
  def tTest(sample1: RDD[Double], sample2: RDD[Double]): Double = {
    val n1 = sample1.count()
    val n2 = sample2.count()
    val m1 = sample1.sum() / n1
    val m2 = sample2.sum() / n2
    val v1 = sample1.map(d => (d - m1) * (d - m1)).sum() / (n1 - 1)
    val v2 = sample2.map(d => (d - m2) * (d - m2)).sum() / (n2 - 1)
    val t: Double = math.abs((m1 - m2) / FastMath.sqrt((v1 / n1) + (v2 / n2)))
    val degreesOfFreedom: Double = (((v1 / n1) + (v2 / n2)) * ((v1 / n1) + (v2 / n2))) /
      ((v1 * v1) / (n1 * n1 * (n1 - 1d)) + (v2 * v2) / (n2 * n2 * (n2 - 1d)))

    // pass a null rng to avoid unneeded overhead as we will not sample from this distribution
    val distribution: TDistribution = new TDistribution(null, degreesOfFreedom)
    2.0 * distribution.cumulativeProbability(-t)
  }

}
