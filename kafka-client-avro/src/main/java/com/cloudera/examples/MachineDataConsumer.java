/*
 * Copyright 2020 Cloudera, Inc.
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

package com.cloudera.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;
import static com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry.CURRENT_PROTOCOL;

public class MachineDataConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(MachineDataConsumer.class);
    private static final String TOPIC = "topic";

    final private String topicName;
    final private Properties props;

    public MachineDataConsumer(String propsFile) throws IOException {
        props = new Properties();
        try (FileInputStream fileInputStream = new FileInputStream(propsFile)) {
            props.load(fileInputStream);
        }

        // get topic name and remove it from the properties object, since it's not a Kafka client property
        topicName = props.getProperty(TOPIC);
        props.remove(TOPIC);

        // specify the protocol version being used
        props.put(SERDES_PROTOCOL_VERSION, CURRENT_PROTOCOL);

        // if "schemaregistry.reader.schema.versions" is present, converts its value to a Map
        if (props.containsKey(KafkaAvroDeserializer.READER_VERSIONS)) {
            ObjectMapper mapper = new ObjectMapper();
            String json = props.getProperty(KafkaAvroDeserializer.READER_VERSIONS);
            props.put(KafkaAvroDeserializer.READER_VERSIONS, mapper.readValue(json, Map.class));
        }
    }

    private void consumeData() throws IOException {
        final KafkaConsumer<String, GenericData.Record> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));
        while (true) {
            LOG.debug("Consumer partition assignment: {}", consumer.assignment());
            ConsumerRecords<String, GenericData.Record> records = consumer.poll(Duration.ofMillis(1000));
            LOG.info("Consumed {} records", records.count());
            for (ConsumerRecord<String, GenericData.Record> record : records) {
                try {
                    LOG.info("Received message: ({}, {}) at partition [{}-{}], offset [{}], with headers: {}", record.key(), record.value().toString(), record.topic(), record.partition(), record.offset(), Arrays.toString(record.headers().toArray()));
                } catch (ClassCastException e) {
                    LOG.warn("Could not deserialize message partition [{}-{}], offset [{}], with headers: {}", record.topic(), record.partition(), record.offset(), Arrays.toString(record.headers().toArray()));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Syntax: " + MachineDataConsumer.class.getName() + " <consumer_properties_file>");
            System.exit(1);
        }
        String propsFile = args[0];

        MachineDataConsumer consumer = new MachineDataConsumer(propsFile);
        consumer.consumeData();
    }
}
