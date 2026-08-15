package com.trading.compute.signaljob;

import com.trading.common.model.FormingBar;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

/**
 * Bidirectional mapper between the in-process {@link FormingBar} record and
 * the {@code forming_bar} KV row (v1 layout, {@link FormingBarTableColumns}).
 * The durable projection is the current-state bar per instrument
 * (PK {@code instrument_token}); the in-process {@code windowEnd} is not
 * persisted (see the columns pin — one source of truth for the window
 * duration). Rehydration reads a row back into a record with
 * {@code windowEnd = 0} until the caller restores it from its own window
 * cadence.
 */
public final class FormingBarRowMapper {

    private FormingBarRowMapper() {}

    /** In-process record -> KV upsert row (INSERT maps to UPSERT downstream). */
    public static RowData toRow(FormingBar bar) {
        GenericRowData row = new GenericRowData(FormingBarTableColumns.FIELD_COUNT);
        row.setField(FormingBarTableColumns.INSTRUMENT_TOKEN, bar.instrumentToken());
        row.setField(FormingBarTableColumns.WINDOW_START, bar.windowStart());
        row.setField(FormingBarTableColumns.OPEN_PAISE, bar.openPaise());
        row.setField(FormingBarTableColumns.HIGH_PAISE, bar.highPaise());
        row.setField(FormingBarTableColumns.LOW_PAISE, bar.lowPaise());
        row.setField(FormingBarTableColumns.CLOSE_PAISE, bar.closePaise());
        row.setField(FormingBarTableColumns.VOLUME, bar.volume());
        row.setField(FormingBarTableColumns.TICK_COUNT, (int) bar.tickCount());
        row.setField(FormingBarTableColumns.LAST_EVENT_TIME, bar.lastEventTime());
        row.setField(FormingBarTableColumns.LAST_EVENT_FINGERPRINT,
                bar.lastFingerprint() == null ? null
                        : StringData.fromString(bar.lastFingerprint()));
        row.setField(FormingBarTableColumns.SCHEMA_VERSION,
                StringData.fromString(FormingBarTableColumns.SCHEMA_VERSION_V1));
        return row;
    }

    /** KV row -> in-process record (rehydration read). */
    public static FormingBar fromRow(RowData row) {
        StringData fp = row.isNullAt(FormingBarTableColumns.LAST_EVENT_FINGERPRINT)
                ? null
                : row.getString(FormingBarTableColumns.LAST_EVENT_FINGERPRINT);
        return new FormingBar(
                row.getLong(FormingBarTableColumns.INSTRUMENT_TOKEN),
                row.getLong(FormingBarTableColumns.WINDOW_START),
                0L, // windowEnd is not persisted (v1) — caller restores it
                row.getLong(FormingBarTableColumns.OPEN_PAISE),
                row.getLong(FormingBarTableColumns.HIGH_PAISE),
                row.getLong(FormingBarTableColumns.LOW_PAISE),
                row.getLong(FormingBarTableColumns.CLOSE_PAISE),
                row.getLong(FormingBarTableColumns.VOLUME),
                row.getInt(FormingBarTableColumns.TICK_COUNT),
                row.getLong(FormingBarTableColumns.LAST_EVENT_TIME),
                fp == null ? null : fp.toString());
    }
}
