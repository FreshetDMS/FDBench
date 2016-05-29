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

package org.pathirage.fdbench.messaging.api.metrics;

import java.util.Map;

public abstract class Metric {
  private final long timestamp;

  private final Map<String, String> attributes;

  public Metric(long timestamp, Map<String, String> attributes) {
    this.timestamp = timestamp;
    this.attributes = attributes;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getAttributeValue(String attribute) {
    return attributes.get(attribute);
  }
}