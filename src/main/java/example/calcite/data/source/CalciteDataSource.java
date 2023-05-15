package example.calcite.data.source;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

/**
 * @author AaronY
 * @version 1.0
 * @since 2022/11/14
 */
@JsonTypeInfo(property = "type", use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
        @JsonSubTypes.Type(value = JdbcCalciteDataSource.class, name = "jdbc"),
        @JsonSubTypes.Type(value = CsvCalciteDataSource.class, name = "csv"),
})
public interface CalciteDataSource {
    String identifier();

    Schema calciteSchema(SchemaPlus parentSchema, String name);
}
