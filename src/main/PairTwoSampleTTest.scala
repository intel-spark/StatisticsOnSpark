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
    * Returns the <i>observed significance level</i>, or
    * <i> p-value</i>, associated with a paired, two-sample, two-tailed t-test
    * based on the data in the input arrays.
    * <p>
    * The number returned is the smallest significance level
    * at which one can reject the null hypothesis that the mean of the paired
    * differences is 0 in favor of the two-sided alternative that the mean paired
    * difference is not equal to 0. For a one-sided test, divide the returned
    * value by 2.</p>
    * <p>
    * This test is equivalent to a one-sample t-test computed using
    * {@link #tTest(double, double[])} with <code>mu = 0</code> and the sample
    * array consisting of the signed differences between corresponding elements of
    * <code>sample1</code> and <code>sample2.</code></p>
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
    val sum2 = sample1.zip(sample2).map(p => (p._1 - p._2 - meanDifference)).sum()
    val varianceDifference = (sum1 - sum2 * sum2 / n) / (n - 1)

    t(meanDifference, 0, varianceDifference, n)
  }

  /**
    * Performs a paired t-test evaluating the null hypothesis that the
    * mean of the paired differences between <code>sample1</code> and
    * <code>sample2</code> is 0 in favor of the two-sided alternative that the
    * mean paired difference is not equal to 0, with significance level
    * <code>alpha</code>.
    * <p>
    * Returns <code>true</code> iff the null hypothesis can be rejected with
    * confidence <code>1 - alpha</code>.  To perform a 1-sided test, use
    * <code>alpha * 2</code></p>
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
