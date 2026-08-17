package com.trading.common.schema;

/**
 * Schema evolution classification
 * (docs/08_implementation/01-foundation.md &rarr; "Schema evolution classes", orig L499).
 */
public enum SchemaEvolutionClass {
    ADDITIVE,               // new optional column / table
    BEHAVIORAL,             // semantics change without wire change
    STATE_INCOMPATIBLE,     // affects managed state / checkpoint compatibility
    WIRE_INCOMPATIBLE,      // changes on-the-wire / storage format
    BREAKING_CLEAN_BREAK    // requires a clean break (replay from source)
}
