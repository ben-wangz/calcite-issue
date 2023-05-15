package example.calcite;

import example.calcite.data.source.CsvCalciteDataSource;
import example.calcite.data.source.JdbcCalciteDataSource;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class CalciteManagerTest {
    private File csvFile;
    @Container
    private GenericContainer<?> mysqlServer;
    private String sourceTableName;
    private String targetTableName;
    private CsvCalciteDataSource csvCalciteDataSource;
    private JdbcCalciteDataSource jdbcCalciteDataSource;

    @BeforeEach
    void setUp() throws IOException, SQLException {
        // generate csv file
        csvFile = File.createTempFile("data.", ".csv");
        try (InputStream inputStream = Objects.requireNonNull(getClass()
                .getClassLoader()
                .getResourceAsStream("test-data.csv"))) {
            try (FileOutputStream fileOutputStream = new FileOutputStream(csvFile)) {
                IOUtils.copy(inputStream, fileOutputStream);
            }
        }
        csvCalciteDataSource = CsvCalciteDataSource.builder()
                .identifier("csv")
                .directory(csvFile.getParentFile())
                .build();
        // generate mysql server
        String databasePassword = RandomStringUtils.randomAlphanumeric(16);
        mysqlServer = new GenericContainer<>(DockerImageName.parse("mariadb:10.10.2-jammy"))
                .withEnv("MARIADB_ROOT_PASSWORD", databasePassword)
                .withExposedPorts(3306);
        mysqlServer.start();
        jdbcCalciteDataSource = JdbcCalciteDataSource.builder()
                .identifier("jdbc")
                .host(mysqlServer.getHost())
                .port(mysqlServer.getFirstMappedPort())
                .databaseName("mysql")
                .username("root")
                .password(databasePassword)
                .driver(JdbcCalciteDataSource.Driver.Mysql)
                .build();
        sourceTableName = "source_table";
        targetTableName = "target_table";
        Stream.of(sourceTableName, targetTableName)
                .forEach(tableName -> DataInitializationUtil.createTable(
                        tableName,
                        Map.of(
                                "year", SQLDataType.VARCHAR,
                                "manufacturer", SQLDataType.VARCHAR,
                                "model", SQLDataType.VARCHAR,
                                "description", SQLDataType.VARCHAR,
                                "price", SQLDataType.VARCHAR
                        ),
                        jdbcCalciteDataSource.dataSource(),
                        jdbcCalciteDataSource.getDriver().getSqlDialect()));
        DataInitializationUtil.insertInto(
                jdbcCalciteDataSource.dataSource(),
                "insert into source_table (year, manufacturer, model, description, price) " +
                        "values ('1996', 'Jeep', 'Grand Cherokee', 'some description', '4799.00')"
        );
    }

    @AfterEach
    void tearDown() {
        if (null != csvFile) {
            FileUtils.deleteQuietly(csvFile);
            csvFile = null;
        }
        if (null != mysqlServer) {
            mysqlServer.close();
            mysqlServer = null;
        }
    }

    @Test
    void testQueryCsv() throws SQLException {
        CalciteManager calciteManager = CalciteManager.builder()
                .calciteDataSource(csvCalciteDataSource)
                .build();
        queryResult(calciteManager, csvCalciteDataSource.identifier(), FilenameUtils.getBaseName(csvFile.getName()));
    }

    @Test
    void testQueryJdbc() throws SQLException {
        CalciteManager calciteManager = CalciteManager.builder()
                .calciteDataSource(jdbcCalciteDataSource)
                .build();
        queryResult(calciteManager, jdbcCalciteDataSource.identifier(), sourceTableName);
    }

    @Test
    void testInsertIntoJdbcSelectFromJdbc() throws SQLException {
        CalciteManager calciteManager = CalciteManager.builder()
                .calciteDataSource(jdbcCalciteDataSource)
                .build();
        RelNode selectRelNode = calciteManager.constructRelBuilder()
                .scan(jdbcCalciteDataSource.identifier(), sourceTableName)
                .build();
        insertIntoTargetTable(calciteManager, selectRelNode);
        queryResult(calciteManager, jdbcCalciteDataSource.identifier(), targetTableName);
    }

    @Test
    void testInsertIntoJdbcSelectFromCsv() throws SQLException {
        CalciteManager calciteManager = CalciteManager.builder()
                .calciteDataSource(jdbcCalciteDataSource)
                .calciteDataSource(csvCalciteDataSource)
                .build();
        RelNode selectRelNode = calciteManager.constructRelBuilder()
                .scan(csvCalciteDataSource.identifier(), FilenameUtils.getBaseName(csvFile.getName()))
                .build();
        insertIntoTargetTable(calciteManager, selectRelNode);
        queryResult(calciteManager, jdbcCalciteDataSource.identifier(), targetTableName);
    }

    private void queryResult(CalciteManager calciteManager, String schemaName, String tableName) throws SQLException {
        calciteManager.query(
                calciteManager.constructRelBuilder()
                        .scan(schemaName, tableName)
                        .build(),
                resultSet -> {
                    try {
                        while (resultSet.next()) {
                            String year = resultSet.getString("year");
                            String manufacturer = resultSet.getString("manufacturer");
                            String model = resultSet.getString("model");
                            String description = resultSet.getString("description");
                            String price = resultSet.getString("price");
                            Assertions.assertEquals("1996", year);
                            Assertions.assertEquals("Jeep", manufacturer);
                            Assertions.assertEquals("Grand Cherokee", model);
                            Assertions.assertEquals("some description", description);
                            Assertions.assertEquals("4799.00", price);
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private void insertIntoTargetTable(CalciteManager calciteManager, RelNode selectRelNode) throws SQLException {
        RelOptSchema relOptSchema = Objects.requireNonNull(calciteManager
                .constructRelBuilder()
                .getRelOptSchema());
        RelOptTable relOptTable = Objects.requireNonNull(
                relOptSchema.getTableForMember(List.of(jdbcCalciteDataSource.identifier(), targetTableName)));
        LogicalTableModify insertRelNode = LogicalTableModify.create(
                relOptTable,
                calciteManager.constructCatalogReader(),
                selectRelNode,
                TableModify.Operation.INSERT,
                null,
                null,
                false
        );
        calciteManager.update(insertRelNode);
    }
}
