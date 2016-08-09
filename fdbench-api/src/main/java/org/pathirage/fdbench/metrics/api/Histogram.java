/**
 * Copyright 2016 Milinda Pathirage
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pathirage.fdbench.metrics.api;

import org.HdrHistogram.AbstractHistogram;
import org.apache.samza.metrics.Metric;
import org.apache.samza.metrics.MetricsVisitor;

public class Histogram extends org.HdrHistogram.Histogram implements Metric {
  public static final double[] LOGARITHMIC_PERCENTILES = {
      0.0f,
      10.0f,
      20.0f,
      30.0f,
      40.0f,
      50.0f,
      55.0f,
      60.0f,
      65.0f,
      70.0f,
      75.0f,
      77.5f,
      80.0f,
      82.5f,
      85.0f,
      87.5f,
      88.75f,
      90.0f,
      91.25f,
      92.5f,
      93.75f,
      94.375f,
      95.0f,
      95.625f,
      96.25f,
      96.875f,
      97.1875f,
      97.5f,
      97.8125f,
      98.125f,
      98.4375f,
      98.5938f,
      98.75f,
      98.9062f,
      99.0625f,
      99.2188f,
      99.2969f,
      99.375f,
      99.4531f,
      99.5313f,
      99.6094f,
      99.6484f,
      99.6875f,
      99.7266f,
      99.7656f,
      99.8047f,
      99.8242f,
      99.8437f,
      99.8633f,
      99.8828f,
      99.9023f,
      99.9121f,
      99.9219f,
      99.9316f,
      99.9414f,
      99.9512f,
      99.9561f,
      99.9609f,
      99.9658f,
      99.9707f,
      99.9756f,
      99.978f,
      99.9805f,
      99.9829f,
      99.9854f,
      99.9878f,
      99.989f,
      99.9902f,
      99.9915f,
      99.9927f,
      99.9939f,
      99.9945f,
      99.9951f,
      99.9957f,
      99.9963f,
      99.9969f,
      99.9973f,
      99.9976f,
      99.9979f,
      99.9982f,
      99.9985f,
      99.9986f,
      99.9988f,
      99.9989f,
      99.9991f,
      99.9992f,
      99.9993f,
      99.9994f,
      99.9995f,
      99.9996f,
      99.9997f,
      99.9998f,
      99.9999f,
      100.0f};

  private final String name;

  public Histogram(String name, int numberOfSignificantValueDigits) {
    super(numberOfSignificantValueDigits);
    this.name = name;
  }

  public Histogram(String name, long highestTrackableValue, int numberOfSignificantValueDigits) {
    super(highestTrackableValue, numberOfSignificantValueDigits);
    this.name = name;
  }

  public Histogram(String name, long lowestDiscernibleValue, long highestTrackableValue, int numberOfSignificantValueDigits) {
    super(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
    this.name = name;
  }

  public Histogram(String name, AbstractHistogram source) {
    super(source);
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public void visit(MetricsVisitor visitor) {
    visitor.visit(this);
  }
}
