package example.calcite.data.source;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.extern.jackson.Jacksonized;
import org.apache.calcite.adapter.csv.CsvSchema;
import org.apache.calcite.adapter.csv.CsvTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;

@EqualsAndHashCode
public class CsvCalciteDataSource implements CalciteDataSource {
    private final String identifier;
    private final File directory;

    @Builder
    @Jacksonized
    public CsvCalciteDataSource(String identifier, File directory) {
        this.identifier = identifier;
        this.directory = directory;
    }

    @Override
    public String identifier() {
        return identifier;
    }

    @Override
    public Schema calciteSchema(SchemaPlus parentSchema, String identifier) {
        return new CsvSchema(directory, CsvTable.Flavor.SCANNABLE);
    }
}
