package org.gbif.dp.descriptor;

public record MissingValueDescriptor(String rawValue, Source source) {

    public static final MissingValueDescriptor NULL = new MissingValueDescriptor("null", Source.DEFAULT);

    public enum Source {
        DEFAULT,
        SCHEMA,
        FIELD
    }
}
