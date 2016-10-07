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

package org.pathirage.fdbench.utils;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import org.pathirage.fdbench.FDBenchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DynamoDBUtils {
  private static final Logger log = LoggerFactory.getLogger(DynamoDBUtils.class);

  public static Table createTable(DynamoDB dynamoDB, String tableName, List<AttributeDefinition> attribDefinitions,
                                  List<KeySchemaElement> keySchemas, long readCapacityUnits, long writeCapacityUnits) {
    Table table;

    try {
      log.info("Checking the availability of table " + tableName);
      table = dynamoDB.getTable(tableName);
      table.describe();
    } catch (ResourceNotFoundException e) {
      log.info("No table with name " + tableName + " exists. So creating a new table.");


      CreateTableRequest request = new CreateTableRequest()
          .withTableName(tableName)
          .withKeySchema(keySchemas)
          .withAttributeDefinitions(attribDefinitions)
          .withProvisionedThroughput(new ProvisionedThroughput()
              .withReadCapacityUnits(5L)
              .withWriteCapacityUnits(5L));

      table = dynamoDB.createTable(request);
    }

    try {
      table.waitForActive();
      return table;
    } catch (InterruptedException ie) {
      String errMsg = "Waiting for DynamoDB table to become active interrupted.";
      log.error(errMsg, ie);
      throw new FDBenchException(errMsg, ie);
    }
  }
}
