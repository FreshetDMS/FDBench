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

package org.pathirage.fdbench.tools;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

public class ThroughputResultPlotter {
  @Parameter(names = {"--table", "-t"}, required = true, description = "DynamoDB Table Name")
  String tableName;

  @Parameter(names = {"--benchmark", "-b"}, required = true, description = "Benchmark Name")
  String benchmarkName;

  @Parameter(names = {"--aws-id", "-i"}, required = true, description = "AWS Access Key ID")
  String awsAccessKeyId;

  @Parameter(names = {"--aws-secret", "-s"}, required = true, description = "AWS Access Key Secret")
  String awsAccessKeySecret;

  @Parameter(names = {"--aws-region", "-r"}, description = "AWS Region")
  String awsRegion = "us-west-2";

  @Parameter(names = {"--out-dir", "-o"}, description = "Output Directory")
  String outputDir = System.getProperty("user.dir") + File.separator + "out";

  public void plot() {
    Map<BigDecimal, Item> measurements = new HashMap<>();

    AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(new BasicAWSCredentials(awsAccessKeyId,
        awsAccessKeySecret));
    dynamoDBClient.withRegion(Regions.fromName(awsRegion));
    DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);

    TableKeysAndAttributes benchMetricsTableKeysAndAttribs = new TableKeysAndAttributes(tableName);
    benchMetricsTableKeysAndAttribs.addHashOnlyPrimaryKey("BenchName", benchmarkName);

    BatchGetItemOutcome outcome = dynamoDB.batchGetItem(benchMetricsTableKeysAndAttribs);

    for(Item item : outcome.getTableItems().get(tableName)) {
      measurements.put(item.getNumber("Id"), item);
    }

    BigDecimal ts = (BigDecimal) measurements.keySet().toArray()[
        new Random(System.currentTimeMillis()).nextInt(measurements.keySet().size())];

    Item latest = measurements.get(ts);
    String snapshot = latest.getString("Snapshot");
    Gson gson = new Gson();
    Map<String, Object> parsedSnapshot = gson.fromJson(snapshot, Map.class);
    Map<String, Object> produceLatency = (Map<String, Object>)((Map<String, Object>)parsedSnapshot.get("kafka-producer-throughput")).get("produce-latency");

    TreeSet<String> percentiles = new TreeSet<>();

    for(String k : produceLatency.keySet()) {
      if(!k.equals("100.0")) {
        percentiles.add(k);
      }
    }

    List<Double> latencies = new ArrayList<>();

    for(String l : percentiles){
      latencies.add((Double) produceLatency.get(l));
    }

    try {
      writeOutput(percentiles, latencies);
    } catch (Exception e) {
      System.out.println("Couldn't write results.");
    }
  }

  private void writeOutput(TreeSet<String> percentiles, List<Double> latencies) throws IOException, TemplateException {
    Configuration config = new Configuration(Configuration.VERSION_2_3_23);
    config.setClassForTemplateLoading(ThroughputResultPlotter.class, "/org/pathirage/fdbench/templates");

    Template template = config.getTemplate("throughput.ftl");
    Map<String, Object> data = new HashMap<>();


    data.put("successLabels", percentiles);
    data.put("successLatencies", latencies);
    data.put("benchmark", benchmarkName);
    data.put("containermemusage", Collections.emptyList());

    FileWriter fileWriter = new FileWriter(outputDir + File.separator + benchmarkName + ".html");

    template.process(data, fileWriter);

    fileWriter.flush();
    fileWriter.close();
  }

  public static void main(String[] args) {
    ThroughputResultPlotter tp = new ThroughputResultPlotter();
    new JCommander(tp, args);

    tp.plot();
  }
}
