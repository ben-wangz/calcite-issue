package example.calcite;

import example.calcite.data.source.CalciteDataSource;
import lombok.Builder;
import lombok.Singular;
import lombok.extern.jackson.Jacksonized;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class CalciteManager {
    private final List<CalciteDataSource> calciteDataSourceList;
    private transient CalciteConnection connection;

    @Builder
    @Jacksonized
    public CalciteManager(@Singular("calciteDataSource") List<CalciteDataSource> calciteDataSourceList) {
        this.calciteDataSourceList = calciteDataSourceList;
    }

    public RelBuilder constructRelBuilder() {
        return RelBuilder.create(
                Frameworks.newConfigBuilder()
                        .parserConfig(SqlParser.Config.DEFAULT)
                        .defaultSchema(getConnection().getRootSchema())
                        .build()
        );
    }

    public CalciteCatalogReader constructCatalogReader() {
        CalciteConnection connection = getConnection();
        return new CalciteCatalogReader(
                CalciteSchema.from(connection.getRootSchema()),
                calciteDataSourceList.stream()
                        .map(CalciteDataSource::identifier)
                        .collect(Collectors.toList()),
                connection.getTypeFactory(),
                connection.config()
        );
    }

    public void query(RelNode relNode, Consumer<ResultSet> resultSetConsumer) throws SQLException {
        try (CalciteConnection connection = connect()) {
            try (PreparedStatement preparedStatement = connection.createPrepareContext()
                    .getRelRunner()
                    .prepareStatement(relNode)) {
                resultSetConsumer.accept(preparedStatement.executeQuery());
            }
        }
    }

    public void update(RelNode relNode) throws SQLException {
        try (CalciteConnection connection = connect()) {
            try (PreparedStatement preparedStatement = connection.createPrepareContext()
                    .getRelRunner()
                    .prepareStatement(relNode)) {
                preparedStatement.executeUpdate();
            }
        }
    }

    private CalciteConnection getConnection() {
        if (null == connection) {
            try {
                connection = connect();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return connection;
    }

    private CalciteConnection connect() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        properties.setProperty(CalciteConnectionProperty.CREATE_MATERIALIZATIONS.camelName(), Boolean.TRUE.toString());
        properties.setProperty(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(), Boolean.TRUE.toString());
        CalciteConnection connection = DriverManager.getConnection("jdbc:calcite:", properties)
                .unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = connection.getRootSchema();
        calciteDataSourceList.forEach(calciteDataSource -> {
            String schemaName = calciteDataSource.identifier();
            rootSchema.add(schemaName, calciteDataSource.calciteSchema(rootSchema, schemaName));
        });
        return connection;
    }
}
