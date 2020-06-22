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

import com.cloudera.examples.utils.MachineDataCollector;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;
import static com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry.CURRENT_PROTOCOL;

public class MachineDataProducer {
    private static final Logger LOG = LoggerFactory.getLogger(MachineDataProducer.class);
    private static final String TOPIC = "topic";

    final private String topicName;
    final private Properties props;
    final private Schema schema;

    public MachineDataProducer(String propsFile, String schemaFile) throws IOException, SchemaNotFoundException {
        props = new Properties();
        try (FileInputStream fileInputStream = new FileInputStream(propsFile)) {
            props.load(fileInputStream);
        }
        topicName = props.getProperty(TOPIC);
        props.remove(TOPIC);
        props.put(SERDES_PROTOCOL_VERSION, CURRENT_PROTOCOL);

        String schemaText = null;
        if (schemaFile != null) {
            LOG.info("Using schema from file {}", schemaFile);
            try (Stream<String> lines = Files.lines(Paths.get(schemaFile))) {
                schemaText = lines.collect(Collectors.joining(System.lineSeparator()));
            }
        } else {
            LOG.info("Retrieving schema from Schema Registry");
            schemaText = retrieveSchema();
        }

        this.schema = new Schema.Parser().parse(schemaText);
    }

    private String retrieveSchema() throws SchemaNotFoundException {
        Map<String, Object> config = new HashMap<>();
        for (final String name: props.stringPropertyNames())
            config.put(name, props.getProperty(name));
        SchemaRegistryClient client = new SchemaRegistryClient(config);
        try {
            SchemaVersionInfo latest = client.getLatestSchemaVersionInfo(topicName);
            LOG.info("Using version [{}] of schema [{}]", latest.getVersion(), topicName);
            return latest.getSchemaText();
        } catch (SchemaNotFoundException e) {
            LOG.info("Schema [{}] not found", topicName);
            throw e;
        }
    }

    private static class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e == null)
                LOG.info("Successfully produced message to partition [{}-{}], offset [{}]", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            else
                LOG.error("Failed to produce message. Exception: {}", e.getMessage());
        }
    }

    private void produceData() throws IOException {
        final KafkaProducer<String, GenericData.Record> producer = new KafkaProducer<>(props);
        try {
            for (GenericData.Record data : new MachineDataCollector(schema)) {
                ProducerRecord<String, GenericData.Record> producerRecord = new ProducerRecord<>(topicName, data);
                producer.send(producerRecord, new ProducerCallback());
            }
        } finally {
            producer.flush();
            producer.close(Duration.ofSeconds(5));
        }
    }

    public static void main(String[] args) throws IOException, SchemaNotFoundException {
        if (args.length < 1 || args.length > 2) {
            System.err.println("Syntax: " + MachineDataProducer.class.getName() + " <producer_properties_file> [<]schema_file]");
            System.exit(1);
        }
        String propsFile = args[0];
        String schemaFile = null;
        if (args.length > 1)
            schemaFile = args[1];
        MachineDataProducer producer = new MachineDataProducer(propsFile, schemaFile);
        producer.produceData();
    }
}
