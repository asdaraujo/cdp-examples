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

import java.io.IOException;
import java.util.Iterator;

public class MachineDataCollector implements Iterable<GenericData.Record> {
    public static final String OS_MAC = "mac";
    public static final String OS_LINUX = "linux";

    private String os;
    private Schema schema;

    public MachineDataCollector(Schema schema) {
        this.schema = schema;
        this.os = System.getProperty("os.name").toLowerCase();
        if (os.indexOf(OS_MAC) >= 0) {
            os = OS_MAC;
        } else if (os.indexOf(OS_LINUX) >= 0) {
            os = OS_LINUX;
        } else {
            throw new RuntimeException("OS not supported: " + os);
        }
    }

    @Override
    public Iterator<GenericData.Record> iterator() {
        Process process = collect();
        if (process != null)
            return new MachineDataIterator(process, os, schema);
        return null;
    }

    private Process collect() {
        Process process;
        try {
            process = Runtime.getRuntime().exec(collectDataCommand(os));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return process;
    }

    private String collectDataCommand(String os) {
        if (os.equals(OS_MAC)) {
            return "vm_stat 1";
        } else if (os.equals(OS_LINUX)) {
            return "vmstat 1";
        }
        return "true";
    }
}
