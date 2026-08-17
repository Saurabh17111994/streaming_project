package com.trading.common.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for the schema-manifest (de)serialization format. */
class SchemaManifestSerializationTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void roundTripsWithSnakeCasePropertyNames() throws Exception {
    SchemaManifestEntry e = new SchemaManifestEntry();
    e.tableName = "raw_ticks";
    e.tableKind = "LOG";
    e.bucketKey = "instrument_id";
    e.ddlSha256 = "abc";

    SchemaManifest manifest = new SchemaManifest();
    manifest.tables = List.of(e);

    String json = mapper.writeValueAsString(manifest);
    assertThat(json).contains("\"table_name\"").contains("\"bucket_key\"").contains("\"ddl_sha256\"");

    SchemaManifest back = mapper.readValue(json, SchemaManifest.class);
    assertThat(back.tables).hasSize(1);
    assertThat(back.tables.get(0).tableName).isEqualTo("raw_ticks");
    assertThat(back.tables.get(0).bucketKey).isEqualTo("instrument_id");
  }
}
