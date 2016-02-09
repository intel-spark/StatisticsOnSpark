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

import breeze.linalg.max
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.exception.{ConvergenceException, MaxCountExceededException, NoDataException, NullArgumentException}
import org.apache.commons.math3.stat.ranking.{NaNStrategy, NaturalRanking, TiesStrategy}
import org.apache.commons.math3.util.FastMath
import org.apache.spark.rdd.RDD

/**
 * An implementation of the Mann-Whitney U test (also called Wilcoxon rank-sum test).
 *
 */
object MannWhitneyUTest {


  /**
   * Computes the <a
   * href="http://en.wikipedia.org/wiki/Mann%E2%80%93Whitney_U"> Mann-Whitney
   * U statistic</a> comparing mean for two independent samples possibly of
   * different length.
   * <p>
   * This statistic can be used to perform a Mann-Whitney U test evaluating
   * the null hypothesis that the two independent samples has equal mean.
   * </p>
   * <p>
   * Let X<sub>i</sub> denote the i'th individual of the first sample and
   * Y<sub>j</sub> the j'th individual in the second sample. Note that the
   * samples would often have different length.
   * </p>
   * <p>
   * <strong>Preconditions</strong>:
   * <ul>
   * <li>All observations in the two samples are independent.</li>
   * <li>The observations are at least ordinal (continuous are also ordinal).</li>
   * </ul>
   * </p>
   *
   * @param x the first sample
   * @param y the second sample
   * @return Mann-Whitney U statistic (maximum of U<sup>x</sup> and U<sup>y</sup>)
   */
  def mannWhitneyU(x: RDD[Double], y: RDD[Double]): Double = {
    val zz = x.union(y)
    val originalPositions = zz.zipWithIndex().sortByKey().map(_._2) // original positions sorted
    val rank = originalPositions.zipWithIndex() // original position and rank
    val xLength = x.count()
    val yLength = y.count()
    val sumRankX = rank.filter( p => p._1 < xLength).map(_._2).sum() + xLength

    val U1: Double = sumRankX - (xLength * (xLength + 1)) / 2
    val U2: Double = xLength.toLong * yLength - U1
    return math.max(U1, U2)
  }

  /**
   * @param Umin smallest Mann-Whitney U value
   * @param n1 number of subjects in first sample
   * @param n2 number of subjects in second sample
   * @return two-sided asymptotic p-value
   */
  private def calculateAsymptoticPValue(Umin: Double, n1: Long, n2: Long): Double = {
    val n1n2prod: Long = n1.toLong * n2
    val EU: Double = n1n2prod / 2.0
    val VarU: Double = n1n2prod * (n1 + n2 + 1) / 12.0
    val z: Double = (Umin - EU) / FastMath.sqrt(VarU)
    val standardNormal: NormalDistribution = new NormalDistribution(null, 0, 1)
    return 2 * standardNormal.cumulativeProbability(z)
  }

  /**
   * Returns the asymptotic <i>observed significance level</i>, or <a href=
   * "http://www.cas.lancs.ac.uk/glossary_v1.1/hyptest.html#pvalue">
   * p-value</a>, associated with a <a
   * href="http://en.wikipedia.org/wiki/Mann%E2%80%93Whitney_U"> Mann-Whitney
   * U statistic</a> comparing mean for two independent samples.
   * <p>
   * Let X<sub>i</sub> denote the i'th individual of the first sample and
   * Y<sub>j</sub> the j'th individual in the second sample. Note that the
   * samples would often have different length.
   * </p>
   * <p>
   * <strong>Preconditions</strong>:
   * <ul>
   * <li>All observations in the two samples are independent.</li>
   * <li>The observations are at least ordinal (continuous are also ordinal).</li>
   * </ul>
   * </p><p>
   * Ties give rise to biased variance at the moment. See e.g. <a
   * href="http://mlsc.lboro.ac.uk/resources/statistics/Mannwhitney.pdf"
   * >http://mlsc.lboro.ac.uk/resources/statistics/Mannwhitney.pdf</a>.</p>
   *
   * @param x the first sample
   * @param y the second sample
   * @return asymptotic p-value
   */

  def mannWhitneyUTest(x: RDD[Double], y: RDD[Double]): Double = {
    val Umax: Double = mannWhitneyU(x, y)
    val Umin: Double = x.count() * y.count() - Umax
    return calculateAsymptoticPValue(Umin, x.count(), y.count())
  }
}
