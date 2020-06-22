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

package com.cloudera.examples.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MachineDataIterator implements Iterator<GenericData.Record> {
    private final Pattern headerLinePattern = Pattern.compile(" *(free|r)\\s.*");
    private final Pattern dataLinePattern = Pattern.compile(" *[0-9][0-9\\s]*");

    private BufferedReader br;
    private GenericData.Record data = null;

    private String[] counterNames;
    private String os;
    private String hostname;
    private Schema schema;

    public MachineDataIterator(Process process, String os, Schema schema) {
        InputStream stdout = process.getInputStream();
        this.br = new BufferedReader(new InputStreamReader(stdout));
        this.os = os;
        this.schema = schema;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "UNKNOWN";
        }
    }

    public boolean fetchNext() {
        while (data == null) {
            try {
                String line = br.readLine();
                if (line == null)
                    return false;
                data = buildRecord(parseData(line, os));
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean hasNext() {
        return fetchNext();
    }

    @Override
    public GenericData.Record next() {
        fetchNext();
        GenericData.Record thisData = data;
        data = null;
        return thisData;
    }

    private long[] parseData(String line, String os) {
        Matcher m = dataLinePattern.matcher(line);
        if (m.matches()) {
            long[] values = Arrays
                    .stream(line.split("\\s+"))
                    .filter(s -> !s.isEmpty()).mapToLong(num -> Long.parseLong(num))
                    .toArray();
            return values;
        }

        m = headerLinePattern.matcher(line);
        if (m.matches()) {
            counterNames = Arrays.stream(line.split("\\s+")).filter(s -> !s.isEmpty()).toArray(String[]::new);
        }

        return null;
    }

    public GenericData.Record buildRecord(long[] values) {
        if (values == null)
            return null;
        List<GenericData.Record> counters = IntStream
                .range(0, values.length)
                .mapToObj(i -> {
                    GenericData.Record counter = new GenericData.Record(schema.getField("counters").schema().getElementType());
                    counter.put("name", counterNames[i]);
                    counter.put("value", values[i]);
                    return counter;
                })
                .collect(Collectors.toList());
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        if (schema.getField("os_type") != null)
            builder.set("os_type", os);
        return builder
                .set("timestamp", Instant.now().toEpochMilli())
                .set("hostname", hostname)
                .set("counters", counters)
                .build();
    }
}
