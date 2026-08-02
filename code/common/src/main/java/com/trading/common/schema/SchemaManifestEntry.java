package com.trading.common.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One table entry in the {@link SchemaManifest}.
 * Mirrors the manifest fields required by docs/08_implementation/01-foundation.md (orig L400).
 */
public class SchemaManifestEntry {

    @JsonProperty("table_name") public String tableName;
    @JsonProperty("schema_version") public String schemaVersion;
    @JsonProperty("ddl_path") public String ddlPath;
    @JsonProperty("ddl_sha256") public String ddlSha256;
    @JsonProperty("table_kind") public String tableKind;          // LOG | KV
    @JsonProperty("writer_owner") public String writerOwner;
    @JsonProperty("primary_key") public String primaryKey;
    @JsonProperty("bucket_key") public String bucketKey;          // non-null for LOG (routing identity)
    @JsonProperty("retention_policy") public String retentionPolicy;
    @JsonProperty("lake_policy") public String lakePolicy;
    @JsonProperty("compatibility_class") public String compatibilityClass;
    @JsonProperty("validated_matrix") public String validatedMatrix;  // version matrix id
    // R-267: typed — the same package already defines SchemaState
    // (PROPOSED/APPROVED/APPLYING/OBSERVED/REJECTED), whose values exactly
    // match the old inline comment; a raw String accepted any typo.
    @JsonProperty("schema_state") public SchemaState schemaState;
}
