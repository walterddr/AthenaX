/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.athenax.vm.connectors.kafka;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorDescriptorValidator.KAFKA_CONFIG_PREFIX;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorDescriptorValidator.PARTITIONER_CLASS_NAME_DEFAULT;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorDescriptorValidator.PARTITIONER_CLASS_NAME_KEY;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorDescriptorValidator.TOPIC_NAME_KEY;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorDescriptorValidator.TOPIC_SCHEMA_KEY;

public class KafkaJsonTableSinkFactory implements TableSinkFactory<Row> {
  private static final String TYPE = "kafka+json";

  @Override
  public TableSink<Row> createTableSink(Map<String, String> properties) {
    DescriptorProperties params = new DescriptorProperties(true);
    params.putProperties(properties);
    TableSchema schema = params.getTableSchema(TOPIC_SCHEMA_KEY);
    String topic = params.getString(TOPIC_NAME_KEY);
    Properties conf = new Properties();
    conf.putAll(params.getPropertiesWithPrefix(KAFKA_CONFIG_PREFIX));
    String partitionerClass = params.getOptionalString(PARTITIONER_CLASS_NAME_KEY)
        .orElse(PARTITIONER_CLASS_NAME_DEFAULT);
    FlinkKafkaPartitioner<Row> partitioner;
    try {
      partitioner = KafkaUtils.instantiatePartitioner(partitionerClass);
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new RuntimeException(e);
    }
    return new Kafka010TableSink(
        schema, topic, conf, Optional.of(partitioner),
        new JsonRowSerializationSchema.Builder(new RowTypeInfo(
            schema.getFieldTypes(), schema.getFieldNames()
        )).build());
  }

  @Override
  public Map<String, String> requiredContext() {
    return null;
  }

  @Override
  public List<String> supportedProperties() {
    return null;
  }
}
