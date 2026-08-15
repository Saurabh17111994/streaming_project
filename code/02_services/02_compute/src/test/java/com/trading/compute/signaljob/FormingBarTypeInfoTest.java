package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.model.FormingBar;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.junit.jupiter.api.Test;

/**
 * {@link FormingBarTypeInfo} pin — the in-process forming-bar event rides a
 * deterministic hand-written serializer (no Kryo, no GenericTypeInfo
 * fallback at operator boundaries), round-tripping all 13 fields including
 * nullable strings. STATE-COMPAT-001 discipline: the serializer is explicit,
 * not a TypeExtractor fallback.
 */
class FormingBarTypeInfoTest {

    @Test
    void serializerRoundTripsAllFields() throws Exception {
        FormingBar bar = new FormingBar(
                7L, 1_710_000_000_000L, 1_710_000_015_000L,
                100L, 120L, 90L, 110L, 1_000L, 42L, 1_710_000_014_999L,
                "fp-1", "NSE", "RELIANCE");

        TypeSerializer<FormingBar> ser = FormingBarTypeInfo.INSTANCE.createSerializer(null);
        DataOutputSerializer out = new DataOutputSerializer(128);
        ser.serialize(bar, out);
        FormingBar back = ser.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));

        assertEquals(bar, back, "13-field round trip must be lossless");
        assertEquals("NSE", back.exchange());
        assertEquals("RELIANCE", back.symbol());
    }

    @Test
    void nullableStringsRoundTripAsNull() throws Exception {
        FormingBar bar = new FormingBar(
                1L, 100L, 200L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, null, null, null);

        TypeSerializer<FormingBar> ser = FormingBarTypeInfo.INSTANCE.createSerializer(null);
        DataOutputSerializer out = new DataOutputSerializer(128);
        ser.serialize(bar, out);
        FormingBar back = ser.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));

        assertEquals(bar, back);
        assertEquals(null, back.lastFingerprint());
        assertEquals(null, back.exchange());
        assertEquals(null, back.symbol());
    }

    @Test
    void typeInfoIsExplicitNotKryoFallback() {
        // The type class is the record; the information is a singleton of the
        // explicit FormingBarTypeInfo — the discipline that avoids
        // GenericTypeInfo (Kryo) at operator boundaries.
        assertEquals(FormingBar.class, FormingBarTypeInfo.INSTANCE.getTypeClass());
        assertTrue(FormingBarTypeInfo.INSTANCE.equals(FormingBarTypeInfo.INSTANCE));
        assertFalse(FormingBarTypeInfo.INSTANCE.isBasicType());
        assertFalse(FormingBarTypeInfo.INSTANCE.isTupleType());
        assertEquals(13, FormingBarTypeInfo.INSTANCE.getArity());
    }
}
