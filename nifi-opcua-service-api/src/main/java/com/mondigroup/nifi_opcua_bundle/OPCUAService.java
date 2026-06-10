/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mondigroup.nifi_opcua_bundle;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;
import java.util.concurrent.BlockingQueue;


@Tags({"opcua", "opc", "industrial", "iot", "milo"})
@CapabilityDescription("Provides OPC UA read/write/browse and subscription capabilities for NiFi processors.")
public interface OPCUAService extends ControllerService {

    public static final String VALUE_SEPARATOR = ";";

    /**
     * Writes a value to a single OPC UA node.
     */
    void putValue(String namespace, String variable, Object value, Boolean withTimestamp) throws ProcessException;

    /**
     * Reads values from multiple OPC UA nodes.
     */
    byte[] getValue(List<String> reqTagNames, String returnTimestamp, boolean excludeNullValue, String nullValueString) throws ProcessException;

    /**
     * Browses the OPC UA node tree starting at the given root and returns a serialized representation.
     */
    byte[] getNodes(String printIndent, int maxRecursiveDepth, int maxReferencePerNode, boolean printNonLeafNode, String rootNodeId) throws ProcessException;

    /**
     * Creates a subscription for the given nodes and pushes incoming data change notifications to the provided queue.
     */
    String subscribe(List<String> reqTagNames, BlockingQueue<String> queue, boolean tsChangedNotify, long minPublishInterval) throws ProcessException;

    /**
     * Cancels a subscription created via {@link #subscribe(List, BlockingQueue, boolean, long)}.
     */
    void unsubscribe(String subscriberUid);
}
