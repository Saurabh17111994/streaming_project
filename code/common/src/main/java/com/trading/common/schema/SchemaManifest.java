package com.trading.common.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Machine-readable schema manifest
 * (docs/08_implementation/01-foundation.md &rarr; "Schema manifest", orig L400).
 */
public class SchemaManifest {

    @JsonProperty("schema_manifest_version")
    public String schemaManifestVersion = "1";

    @JsonProperty("tables")
    public List<SchemaManifestEntry> tables;
}
