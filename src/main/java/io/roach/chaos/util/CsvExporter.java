package io.roach.chaos.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CsvExporter implements Exporter {
    private final Path path;

    private final List<List<String>> header = new ArrayList<>();

    private final List<List<String>> lines = new ArrayList<>();

    public CsvExporter(Path path) {
        this.path = path;
    }

    @Override
    public void writeHeader(List<String> names) {
        header.add(names);
    }

    @Override
    public void write(List<Object> values) {
        lines.add(values.stream().map(Object::toString).toList());
    }

    @Override
    public void close() throws IOException {
        List<String> formatted = new ArrayList<>();

        lines.addAll(0, header);

        for (List<String> line : lines) {
            StringBuilder text = new StringBuilder();
            line.forEach(s -> {
                if (!text.isEmpty()) {
                    text.append(",");
                }
                text.append(s);
            });
            formatted.add(Objects.toString(text));
        }

        Files.write(path, formatted, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
    }
}
