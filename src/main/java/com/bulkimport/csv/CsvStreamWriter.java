package com.bulkimport.csv;

import com.bulkimport.config.BulkImportConfig;
import com.bulkimport.config.NullHandling;
import com.bulkimport.converter.TypeConverterRegistry;
import com.bulkimport.mapping.ColumnMapping;
import com.bulkimport.mapping.TableMapping;

import de.siegmar.fastcsv.writer.CsvWriter;
import de.siegmar.fastcsv.writer.LineDelimiter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Writes entities as CSV data for PostgreSQL COPY command.
 * Uses FastCSV for high-performance CSV generation.
 *
 * @param <T> the entity type
 */
public class CsvStreamWriter<T> {

    private final TableMapping<T> mapping;
    private final TypeConverterRegistry converterRegistry;
    private final NullHandling nullHandling;

    /**
     * Creates a new CSV stream writer.
     *
     * @param mapping the table mapping
     * @param config the import configuration
     */
    public CsvStreamWriter(TableMapping<T> mapping, BulkImportConfig config) {
        this(mapping, config, TypeConverterRegistry.getDefault());
    }

    /**
     * Creates a new CSV stream writer with a custom converter registry.
     *
     * @param mapping the table mapping
     * @param config the import configuration
     * @param converterRegistry the type converter registry
     */
    public CsvStreamWriter(TableMapping<T> mapping, BulkImportConfig config,
                           TypeConverterRegistry converterRegistry) {
        this.mapping = mapping;
        this.nullHandling = config.getNullHandling();
        this.converterRegistry = converterRegistry;
    }

    /**
     * Writes entities from a list to the output stream.
     *
     * @param entities the entities to write
     * @param outputStream the output stream to write to
     * @return the number of rows written
     * @throws IOException if writing fails
     */
    public int write(List<T> entities, OutputStream outputStream) throws IOException {
        return write(entities.iterator(), entities.size(), outputStream);
    }

    /**
     * Writes entities from a stream to the output stream.
     *
     * @param entities the entities to write
     * @param outputStream the output stream to write to
     * @return the number of rows written
     * @throws IOException if writing fails
     */
    public int write(Stream<T> entities, OutputStream outputStream) throws IOException {
        return write(entities.iterator(), -1, outputStream);
    }

    /**
     * Writes entities from an iterator to the output stream.
     *
     * @param entities the entities to write
     * @param expectedCount the expected number of entities (-1 if unknown)
     * @param outputStream the output stream to write to
     * @return the number of rows written
     * @throws IOException if writing fails
     */
    public int write(Iterator<T> entities, int expectedCount, OutputStream outputStream)
            throws IOException {

        List<ColumnMapping<T, ?>> columns = mapping.getColumns();
        int columnCount = columns.size();
        String[] values = new String[columnCount];

        Writer writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);

        try (CsvWriter csvWriter = CsvWriter.builder()
                .lineDelimiter(LineDelimiter.LF)
                .build(writer)) {

            int rowCount = 0;

            while (entities.hasNext()) {
                T entity = entities.next();

                // Extract and convert values for each column
                for (int i = 0; i < columnCount; i++) {
                    ColumnMapping<T, ?> column = columns.get(i);
                    Object value = column.extractValue(entity);
                    values[i] = convertValue(value);
                }

                csvWriter.writeRecord(values);
                rowCount++;
            }

            return rowCount;
        }
    }

    /**
     * Gets the column names for the COPY command.
     */
    public List<String> getColumnNames() {
        return mapping.getColumnNames();
    }

    /**
     * Converts a value to its CSV string representation.
     */
    @SuppressWarnings("unchecked")
    private <V> String convertValue(V value) {
        if (value == null) {
            return nullHandling.getRepresentation();
        }

        return converterRegistry.convert(value, nullHandling);
    }
}
