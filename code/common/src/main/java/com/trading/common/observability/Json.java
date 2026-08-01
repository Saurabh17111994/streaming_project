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
            sb.append('{');
            first = true;
            body.accept(this);
            sb.append('}');
            return this;
        }

        Builder arr(java.util.function.Consumer<Builder> body) {
            sb.append('[');
            first = true;
            body.accept(this);
            sb.append(']');
            return this;
        }

        Builder kv(String k, String v) {
            sep();
            sb.append('"').append(escape(k)).append("\":\"").append(escape(v)).append('"');
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
                default:
                    if (c < 0x20) {
                        o.append(String.format("\\u%04x", (int) c));
                    } else {
                        o.append(c);
                    }
            }
        }
        return o.toString();
    }
}
