/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.integrations.debezium.internals;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.airbyte.cdk.integrations.debezium.CdcMetadataInjector;
import io.airbyte.protocol.models.v0.AirbyteMessage;

public class RelationalDbDebeziumEventConverter implements DebeziumEventConverter {

  private final CdcMetadataInjector cdcMetadataInjector;
  private final Instant emittedAt;
  private final Map<String, List<String>> jsonbColumnsPerTable;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public RelationalDbDebeziumEventConverter(CdcMetadataInjector cdcMetadataInjector, Instant emittedAt) {
    this.cdcMetadataInjector = cdcMetadataInjector;
    this.emittedAt = emittedAt;
    this.jsonbColumnsPerTable = new HashMap<>();
  }

  public RelationalDbDebeziumEventConverter(CdcMetadataInjector cdcMetadataInjector, Instant emittedAt,
      Map<String, List<String>> jsonbColumnsPerTable) {
    this.cdcMetadataInjector = cdcMetadataInjector;
    this.emittedAt = emittedAt;
    this.jsonbColumnsPerTable = jsonbColumnsPerTable;
  }

  @Override
  public AirbyteMessage toAirbyteMessage(ChangeEventWithMetadata event) {
    final JsonNode debeziumEvent = event.eventValueAsJson();
    final JsonNode before = debeziumEvent.get(DebeziumEventConverter.BEFORE_EVENT);
    final JsonNode after = debeziumEvent.get(DebeziumEventConverter.AFTER_EVENT);
    final JsonNode source = debeziumEvent.get(DebeziumEventConverter.SOURCE_EVENT);

    final ObjectNode baseNode = (ObjectNode) (after.isNull() ? before : after);
    convertJsonb(baseNode, source);
    final JsonNode data = DebeziumEventConverter.addCdcMetadata(baseNode, source, cdcMetadataInjector, after.isNull());
    return DebeziumEventConverter.buildAirbyteMessage(source, cdcMetadataInjector, emittedAt, data);
  }

  private void convertJsonb(final ObjectNode data, final JsonNode source) {
    final String tableName = source.get("table").asText();
    List<String> jsonbColumns = this.jsonbColumnsPerTable.get(tableName);
    if (jsonbColumns != null) {
      jsonbColumns.forEach(c -> {
        final String stringifiedJsonb = data.get(c).asText();
        try {
          data.set(c, this.objectMapper.readTree(stringifiedJsonb));
        } catch (JsonProcessingException e) {
          // should this throw or silently swallow?
          throw new RuntimeException("Could not parse 'jsonb' value:" + e);
        }
      });
    }
  }

}
