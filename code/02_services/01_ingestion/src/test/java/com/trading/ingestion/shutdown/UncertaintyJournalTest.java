package com.trading.ingestion.shutdown;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ING-FAIL-003: UncertaintyJournal — persists shutdown counters and can be read back.
 */
@DisplayName("ING-FAIL-003: UncertaintyJournal")
class UncertaintyJournalTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Write entry → file exists with one line")
    void writeEntryCreatesFile() throws Exception {
        Path journalPath = tempDir.resolve("uncertainty-journal.jsonl");
        UncertaintyJournal journal = new UncertaintyJournal(journalPath);

        UncertaintyJournal.Entry entry = new UncertaintyJournal.Entry(
                "test-instance", Instant.now(), 1000, 950, 10, 40, 50000, "shutdown"
        );

        journal.write(entry);

        assertTrue(Files.exists(journalPath), "Journal file must exist after write");
        List<String> lines = Files.readAllLines(journalPath);
        assertEquals(1, lines.size(), "First write produces exactly one line");
    }

    @Test
    @DisplayName("Multiple entries append correctly")
    void multipleEntriesAppend() throws Exception {
        Path journalPath = tempDir.resolve("uncertainty-journal.jsonl");
        UncertaintyJournal journal = new UncertaintyJournal(journalPath);

        for (int i = 0; i < 5; i++) {
            journal.write(new UncertaintyJournal.Entry(
                    "instance-" + i, Instant.now(),
                    100, 90, 5, 5, 5000, "test-" + i
            ));
        }

        List<String> lines = Files.readAllLines(journalPath);
        assertEquals(5, lines.size(), "Five writes produce five lines");
    }

    @Test
    @DisplayName("Entry JSON contains all fields")
    void entryContainsAllFields() {
        UncertaintyJournal.Entry entry = new UncertaintyJournal.Entry(
                "test-1", Instant.ofEpochMilli(1719000000000L),
                1000, 950, 10, 40, 50000, "shutdown"
        );

        String json = entry.toJson();
        assertTrue(json.contains("\"instance\":\"test-1\""));
        assertTrue(json.contains("\"total_accepted\":1000"));
        assertTrue(json.contains("\"total_appended\":950"));
        assertTrue(json.contains("\"total_failed\":10"));
        assertTrue(json.contains("\"total_rejected\":40"));
        assertTrue(json.contains("\"total_bytes_accepted\":50000"));
        assertTrue(json.contains("\"reason\":\"shutdown\""));
    }

    @Test
    @DisplayName("JSON escapes special characters in instance ID")
    void jsonEscapesSpecialCharacters() {
        UncertaintyJournal.Entry entry = new UncertaintyJournal.Entry(
                "test-\\\"quote", Instant.now(), 0, 0, 0, 0, 0, "reason"
        );

        String json = entry.toJson();
        assertTrue(json.contains("\\\\\\\""), "Quotes/backslashes must be escaped");
    }

    @Test
    @DisplayName("ensureWritable creates parent directory")
    void ensureWritableCreatesParent() throws Exception {
        Path nested = tempDir.resolve("a/b/c/journal.jsonl");
        UncertaintyJournal journal = new UncertaintyJournal(nested);
        assertTrue(journal.ensureWritable(), "ensureWritable should succeed");
        assertTrue(java.nio.file.Files.isDirectory(tempDir.resolve("a/b/c")),
                "parent directory should be created");
    }
}
