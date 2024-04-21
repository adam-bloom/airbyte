/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.cdk.integrations.debezium.internals

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.cdk.integrations.debezium.CdcMetadataInjector
import io.airbyte.protocol.models.v0.AirbyteMessage
import java.time.Instant


class RelationalDbDebeziumEventConverter(
    private val cdcMetadataInjector: CdcMetadataInjector<*>,
    private val emittedAt: Instant,
    private val jsonbColumnsPerTable: Map<String, List<String>> = HashMap<String, List<String>>()
) : DebeziumEventConverter {
    private val objectMapper: ObjectMapper = ObjectMapper();
    override fun toAirbyteMessage(event: ChangeEventWithMetadata): AirbyteMessage {
        val debeziumEvent = event.eventValueAsJson()
        val before: JsonNode = debeziumEvent!!.get(DebeziumEventConverter.Companion.BEFORE_EVENT)
        val after: JsonNode = debeziumEvent.get(DebeziumEventConverter.Companion.AFTER_EVENT)
        val source: JsonNode = debeziumEvent.get(DebeziumEventConverter.Companion.SOURCE_EVENT)

        val baseNode = (if (after.isNull) before else after) as ObjectNode
        convertJsonb(baseNode, source, jsonbColumnsPerTable);
        val data: JsonNode =
            DebeziumEventConverter.Companion.addCdcMetadata(
                baseNode,
                source,
                cdcMetadataInjector,
                after.isNull
            )
        return DebeziumEventConverter.Companion.buildAirbyteMessage(
            source,
            cdcMetadataInjector,
            emittedAt,
            data
        )
    }

    private fun convertJsonb(data: ObjectNode, source: JsonNode, jsonbColumnsPerTable: Map<String, List<String>>) {
    val tableName: String = source.get("table").asText();
    val jsonbColumns: List<String>? = jsonbColumnsPerTable.get(tableName);
    if (jsonbColumns != null) {
        for (c in jsonbColumns)  {
            val stringifiedJsonb: String = data.get(c).asText();
            try {
            data.set(c, objectMapper.readTree(stringifiedJsonb));
            } catch (e: JsonProcessingException) {
            // should this throw or silently swallow?
            throw RuntimeException("Could not parse 'jsonb' value:" + e);
            }
        }
    }
  }
}
