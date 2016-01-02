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


import org.apache.spark.rdd.RDD

/**
  * Created by yuhao on 12/31/15.
  */
class PairTwoSampleTTest extends TTestBasic {
  /**
    * Returns the observed significance level, or
    *  p-value, associated with a paired, two-sample, two-tailed t-test
    * based on the data in the input arrays.
    *
    * The number returned is the smallest significance level
    * at which one can reject the null hypothesis that the mean of the paired
    * differences is 0 in favor of the two-sided alternative that the mean paired
    * difference is not equal to 0. For a one-sided test, divide the returned
    * value by 2.
    *
    * This test is equivalent to a one-sample t-test computed using
    * {@link #tTest(double, double[])} with mu = 0 and the sample
    * array consisting of the signed differences between corresponding elements of
    * sample1 and sample2.
    *
    * @param sample1 array of sample data values
    * @param sample2 array of sample data values
    * @return p-value for t-test
    */
  def tTest(sample1: RDD[Double], sample2: RDD[Double]): Double = {
    val n = sample1.count()
    require(n == sample2.count())

    val meanDifference = sample1.zip(sample2).map(p => p._1 - p._2).sum() / n

    val sum1 = sample1.zip(sample2).map(p => (p._1 - p._2 - meanDifference) * (p._1 - p._2 - meanDifference)).sum()
    val sum2 = sample1.zip(sample2).map(p => p._1 - p._2 - meanDifference).sum()
    val varianceDifference = (sum1 - sum2 * sum2 / n) / (n - 1)

    t(meanDifference, 0, varianceDifference, n)
  }

  /**
    * Performs a paired t-test evaluating the null hypothesis that the
    * mean of the paired differences between sample1 and
    * sample2 is 0 in favor of the two-sided alternative that the
    * mean paired difference is not equal to 0, with significance level
    * alpha.
    * Returns true iff the null hypothesis can be rejected with
    * confidence 1 - alpha.  To perform a 1-sided test, use
    * alpha * 2
    *
    * @param sample1 array of sample data values
    * @param sample2 array of sample data values
    * @param alpha significance level of the test
    * @return true if the null hypothesis can be rejected with
    * confidence 1 - alpha
    */
  def tTest(sample1: RDD[Double], sample2: RDD[Double], alpha: Double): Boolean = {
    tTest(sample1, sample2) < alpha
  }

}
