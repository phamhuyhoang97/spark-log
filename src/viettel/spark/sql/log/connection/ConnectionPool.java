package viettel.spark.sql.log.connection;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConnectionPool {
    public static final String DB_USERNAME = "db.username";
    public static final String DB_PASSWORD = "db.password";
    public static final String DB_URL = "db.url";
    public static final String DB_DRIVER_CLASS = "driver.class.name";
    public static final String DB_MIN_POOL_SIZE = "db.minPoolSize";
    public static final String DB_MAX_POOL_SIZE = "db.maxPoolSize";
    public static final String DB_ACQUIRE_INCREMENT = "db.acquireIncrement";
    public static final String DB_MAX_CONNECTION_AGE = "db.maxConnectionAge";
    public static final String DB_MAX_STATEMENTS = "db.maxStatements";
    private static Properties properties = null;
    private static ComboPooledDataSource dataSource;

    static {
        try {
            properties = new Properties();
            properties.load(new FileInputStream("../conf/db.properties"));

            dataSource = new ComboPooledDataSource();
            dataSource.setDriverClass(properties.getProperty(DB_DRIVER_CLASS));

            dataSource.setJdbcUrl(properties.getProperty(DB_URL));
            dataSource.setUser(properties.getProperty(DB_USERNAME));
            dataSource.setPassword(properties.getProperty(DB_PASSWORD));

            dataSource.setMaxStatements(Integer.parseInt(properties.getProperty(DB_MAX_STATEMENTS)));
            dataSource.setMinPoolSize(Integer.parseInt(properties.getProperty(DB_MIN_POOL_SIZE)));
            dataSource.setMaxPoolSize(Integer.parseInt(properties.getProperty(DB_MAX_POOL_SIZE)));
            dataSource.setAcquireIncrement(Integer.parseInt(properties.getProperty(DB_ACQUIRE_INCREMENT)));
            dataSource.setMaxConnectionAge(Integer.parseInt(properties.getProperty(DB_MAX_CONNECTION_AGE)));

        } catch (IOException | PropertyVetoException e) {
            e.printStackTrace();
        }
    }

    public static DataSource getDataSource() {
        return dataSource;
    }
}
