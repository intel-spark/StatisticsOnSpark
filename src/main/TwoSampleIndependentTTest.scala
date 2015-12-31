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
    * <p>
    * Returns <code>true</code> iff the null hypothesis that the means are
    * equal can be rejected with confidence <code>1 - alpha</code>.  To
    * perform a 1-sided test, use <code>alpha * 2</code></p>
    * <li>To test the (2-sided) hypothesis <code>mean 1 = mean 2 </code> at
    * the 95% level,  use
    * <br><code>tTest(sample1, sample2, 0.05). </code>
    * </li>
    * <li>To test the (one-sided) hypothesis <code> mean 1 < mean 2 </code>,
    * at the 99% level, first verify that the measured  mean of <code>sample 1</code>
    * is less than the mean of <code>sample 2</code> and then use
    * <br><code>tTest(sample1, sample2, 0.02) </code>
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
    * Returns the <i>observed significance level</i>, or
    * <i>p-value</i>, associated with a two-sample, two-tailed t-test
    * comparing the means of the input arrays.
    * <p>
    * The number returned is the smallest significance level
    * at which one can reject the null hypothesis that the two means are
    * equal in favor of the two-sided alternative that they are different.
    * For a one-sided test, divide the returned value by 2.</p>
    * <p>
    * The test does not assume that the underlying popuation variances are
    * equal  and it uses approximated degrees of freedom computed from the
    * sample data to compute the p-value.  The t-statistic used is as defined in
    * {@link #t(double[], double[])} and the Welch-Satterthwaite approximation
    * to the degrees of freedom is used,
    * as described
    * <a href="http://www.itl.nist.gov/div898/handbook/prc/section3/prc31.htm">
    * here.</a>
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
    val v2 = sample1.map(d => (d - m2) * (d - m2)).sum() / (n2 - 1)
    val t: Double = math.abs((m1 - m2) / FastMath.sqrt((v1 / n1) + (v2 / n2)))
    val degreesOfFreedom: Double = (((v1 / n1) + (v2 / n2)) * ((v1 / n1) + (v2 / n2))) /
      ((v1 * v1) / (n1 * n1 * (n1 - 1d)) + (v2 * v2) / (n2 * n2 * (n2 - 1d)))

    // pass a null rng to avoid unneeded overhead as we will not sample from this distribution
    val distribution: TDistribution = new TDistribution(null, degreesOfFreedom)
    return 2.0 * distribution.cumulativeProbability(-t)
  }

}
