package com.trading.common.observability;

/** Minimal, dependency-free JSON builder for OTLP-shaped records. */
final class Json {

    private Json() {}

    static String build(java.util.function.Consumer<Builder> body) {
        Builder w = new Builder();
        body.accept(w);
        return w.sb.toString();
    }

    static final class Builder {
        final StringBuilder sb = new StringBuilder();
        private boolean first = true;

        Builder obj(java.util.function.Consumer<Builder> body) {
            // R-045: the nested block is a VALUE in the enclosing container, so
            // it must emit a separator first; the inner contents start fresh;
            // and afterwards the enclosing container still needs separators.
            sep();
            sb.append('{');
            first = true;
            body.accept(this);
            sb.append('}');
            first = false;
            return this;
        }

        Builder arr(java.util.function.Consumer<Builder> body) {
            sep();
            sb.append('[');
            first = true;
            body.accept(this);
            sb.append(']');
            first = false;
            return this;
        }

        Builder kv(String k, String v) {
            sep();
            sb.append('"').append(escape(k)).append('"');
            if (v == null) {
                // R-077: a null value must serialize as JSON null, not the
                // empty string escape(null) produces — absent attributes and
                // empty strings are semantically different in telemetry.
                sb.append(":null");
            } else {
                sb.append(":\"").append(escape(v)).append('"');
            }
            return this;
        }

        Builder kv(String k, long v) {
            sep();
            sb.append('"').append(escape(k)).append("\":").append(v);
            return this;
        }

        private void sep() {
            if (!first) {
                sb.append(',');
            }
            first = false;
        }
    }

    /** RFC 8259 minimal escaping. */
    static String escape(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder o = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': o.append("\\\""); break;
                case '\\': o.append("\\\\"); break;
                case '\n': o.append("\\n"); break;
                case '\r': o.append("\\r"); break;
                case '\t': o.append("\\t"); break;
                case '\b': o.append("\\b"); break;
                case '\f': o.append("\\f"); break;
                default:
                    if (c < 0x20) {
                        // R-218: hand-rolled hex, no per-char String.format.
                        o.append("\\u");
                        appendHex4(o, c);
                    } else {
                        o.append(c);
                    }
            }
        }
        return o.toString();
    }

    private static void appendHex4(StringBuilder o, int v) {
        o.append(HEX[(v >>> 12) & 0xF]);
        o.append(HEX[(v >>> 8) & 0xF]);
        o.append(HEX[(v >>> 4) & 0xF]);
        o.append(HEX[v & 0xF]);
    }

    private static final char[] HEX = "0123456789abcdef".toCharArray();
}
