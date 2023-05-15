package example.calcite;

import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class DataInitializationUtil {
    public static void createTable(
            String tableName,
            Map<String, DataType<?>> fields,
            DataSource dataSource,
            SQLDialect sqlDialect
    ) {
        Configuration config = new DefaultConfiguration()
                .set(dataSource)
                .set(sqlDialect);
        DSLContext dslContext = DSL.using(config);
        try (CreateTableColumnStep createTableColumnStep = dslContext.createTable(tableName)) {
            fields.forEach(createTableColumnStep::column);
            createTableColumnStep.execute();
        }
    }

    public static void insertInto(DataSource dataSource, String insertIntoSql) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            statement.executeUpdate(insertIntoSql);
        }
    }
}
