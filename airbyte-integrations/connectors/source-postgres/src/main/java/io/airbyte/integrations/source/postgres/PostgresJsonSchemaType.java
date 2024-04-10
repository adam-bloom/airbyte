package io.airbyte.integrations.source.postgres;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import io.airbyte.protocol.models.JsonSchemaPrimitiveUtil.JsonSchemaPrimitive;
import io.airbyte.protocol.models.JsonSchemaType;

public class PostgresJsonSchemaType extends JsonSchemaType {

    private PostgresJsonSchemaType(final Map<String, Object> jsonSchemaTypeMap) {
        super(jsonSchemaTypeMap);
    }

    public static class UnionBuilder {
        private final ImmutableMap.Builder<String, Object> typeMapBuilder;

        private UnionBuilder(final List<JsonSchemaPrimitive> unionTypes) {
            typeMapBuilder = ImmutableMap.builder();

            final List<String> typeList = new ArrayList<>();
            unionTypes.forEach(x -> typeList.add(x.name().toLowerCase()));
            typeMapBuilder.put(TYPE, typeList);
        }

        public PostgresJsonSchemaType build() {
            return new PostgresJsonSchemaType(typeMapBuilder.build());
        }

    }

    public static UnionBuilder union(final List<JsonSchemaPrimitive> unionTypes) {
        return new UnionBuilder(unionTypes);
    }

    public static PostgresJsonSchemaType UNKNOWN() {
        final ImmutableMap.Builder<String, Object> typeMapBuilder = ImmutableMap.builder();
        return new PostgresJsonSchemaType(typeMapBuilder.build());

    }

}
