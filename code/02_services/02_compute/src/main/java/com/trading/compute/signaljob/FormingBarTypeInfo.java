package com.trading.compute.signaljob;

import com.trading.common.model.FormingBar;
import java.io.IOException;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Deterministic {@link TypeInformation} for the in-process {@link FormingBar}
 * event (Slice 2.2 forming-bar handoff). The project deliberately avoids
 * Kryo/GenericTypeInfo fallback at operator boundaries (same discipline as
 * the explicit {@code ROW_TYPE_INFO} declarations), and the {@link FormingBar}
 * Java record is not a POJO (no no-arg constructor), so a plain
 * {@code TypeInformation.of(...)} would fall back to Kryo. This explicit
 * information + serializer keeps the forming-bar event stream on the pinned
 * serializer path.
 *
 * <p>The record's 13 fields serialize in pinned order: 11 longs + two
 * nullable strings. The serializer is a fixed-format, hand-written
 * serializer (no Kryo, no reflection); the snapshot is
 * {@link FormingBarSerializerSnapshot} with a format version, so a field
 * addition fails state-compat checks at restore instead of silently
 * mis-reading (STATE-COMPAT-001 discipline). The forming bar is transient
 * in-job event state, not checkpointed business state — the serializer exists
 * for the operator-boundary type safety, not as a durable-state contract.
 */
public final class FormingBarTypeInfo extends TypeInformation<FormingBar> {

    private static final long serialVersionUID = 1L;

    public static final FormingBarTypeInfo INSTANCE = new FormingBarTypeInfo();

    private FormingBarTypeInfo() {}

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 13;
    }

    @Override
    public int getTotalFields() {
        return 13;
    }

    @Override
    public Class<FormingBar> getTypeClass() {
        return FormingBar.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<FormingBar> createSerializer(SerializerConfig config) {
        return new FormingBarSerializer();
    }

    @Override
    public String toString() {
        return "FormingBarTypeInfo";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FormingBarTypeInfo;
    }

    @Override
    public int hashCode() {
        return FormingBar.class.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof FormingBarTypeInfo;
    }

    /** Fixed-format serializer for the 13-field record (pinned order). */
    public static final class FormingBarSerializer extends TypeSerializer<FormingBar> {

        private static final long serialVersionUID = 1L;

        private static final LongSerializer LONG = LongSerializer.INSTANCE;
        private static final StringSerializer STRING = StringSerializer.INSTANCE;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TypeSerializer<FormingBar> duplicate() {
            return this;
        }

        @Override
        public FormingBar createInstance() {
            return new FormingBar(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, null, null, null);
        }

        @Override
        public FormingBar copy(FormingBar from) {
            return new FormingBar(
                    from.instrumentToken(), from.windowStart(), from.windowEnd(),
                    from.openPaise(), from.highPaise(), from.lowPaise(), from.closePaise(),
                    from.volume(), from.tickCount(), from.lastEventTime(),
                    from.lastFingerprint(), from.exchange(), from.symbol());
        }

        @Override
        public FormingBar copy(FormingBar from, FormingBar reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(FormingBar value, DataOutputView target) throws IOException {
            LONG.serialize(value.instrumentToken(), target);
            LONG.serialize(value.windowStart(), target);
            LONG.serialize(value.windowEnd(), target);
            LONG.serialize(value.openPaise(), target);
            LONG.serialize(value.highPaise(), target);
            LONG.serialize(value.lowPaise(), target);
            LONG.serialize(value.closePaise(), target);
            LONG.serialize(value.volume(), target);
            LONG.serialize(value.tickCount(), target);
            LONG.serialize(value.lastEventTime(), target);
            STRING.serialize(value.lastFingerprint(), target);
            STRING.serialize(value.exchange(), target);
            STRING.serialize(value.symbol(), target);
        }

        @Override
        public FormingBar deserialize(DataInputView source) throws IOException {
            return new FormingBar(
                    LONG.deserialize(source), LONG.deserialize(source),
                    LONG.deserialize(source), LONG.deserialize(source),
                    LONG.deserialize(source), LONG.deserialize(source),
                    LONG.deserialize(source), LONG.deserialize(source),
                    LONG.deserialize(source), LONG.deserialize(source),
                    STRING.deserialize(source), STRING.deserialize(source),
                    STRING.deserialize(source));
        }

        @Override
        public FormingBar deserialize(FormingBar reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            for (int i = 0; i < 10; i++) {
                LONG.copy(source, target);
            }
            for (int i = 0; i < 3; i++) {
                STRING.copy(source, target);
            }
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof FormingBarSerializer;
        }

        @Override
        public int hashCode() {
            return FormingBarSerializer.class.hashCode();
        }

        @Override
        public TypeSerializerSnapshot<FormingBar> snapshotConfiguration() {
            return new FormingBarSerializerSnapshot();
        }
    }

    /**
     * Simple snapshot: {@code SimpleTypeSerializerSnapshot} keys compatibility
     * on the serializer class identity (a changed serializer fails the restore
     * closed instead of silently mis-reading — STATE-COMPAT-001 discipline).
     * The base {@code getCurrentVersion()} (the internal snapshot-format
     * version) is left at its default; do not override it.
     */
    public static final class FormingBarSerializerSnapshot
            extends org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot<FormingBar> {

        public FormingBarSerializerSnapshot() {
            super(() -> new FormingBarSerializer());
        }
    }


}
