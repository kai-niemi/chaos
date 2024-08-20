package io.roach.chaos.util;

import java.io.Closeable;
import java.util.List;

public interface Exporter extends Closeable {
    default void writeHeader(List<String> names) {

    }

    default void write(List<Object> values) {

    }
}
