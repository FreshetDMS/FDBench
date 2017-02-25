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

package org.pathirage.fdbench.aws;

import com.typesafe.config.Config;
import org.pathirage.fdbench.config.AbstractConfig;

import java.util.List;

public class AWSConfiguration extends AbstractConfig {
  private static final String AWS_ACCESS_KEY_ID = "aws.access.key.id";
  private static final String AWS_ACCESS_KEY_SECRET = "aws.access.key.secret";
  private static final String AWS_REGION = "aws.region";
  private static final String S3_BUCKET_PREFIX = "aws.s3.bucket.prefix";
  private static final String EBS_VOLUMES = "aws.ebs.volumes";
  private static final String EC2_INSTANCES = "aws.ec2.instances";

  public AWSConfiguration(Config config) {
    super(config);
  }

  public String getAWSAccessKeyId() {
    return getString(AWS_ACCESS_KEY_ID);
  }

  public String getAWSAccessKeySecret() {
    return getString(AWS_ACCESS_KEY_SECRET);
  }

  public String getAWSRegion() {
    return getString(AWS_REGION, "us-west-2");
  }

  public String getS3BucketPrefix() {
    return getString(S3_BUCKET_PREFIX, "fdbench-aws");
  }

  public List<String> getEBSVolumes() {
    return getStringList(EBS_VOLUMES);
  }

  public List<String> getEC2Instances() {
    return getStringList(EC2_INSTANCES);
  }
}
