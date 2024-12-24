package jm.task.core.jdbc.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Util {
    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    private Util() {
        throw new UnsupportedOperationException("Utility class should not be instantiated");
    }

    private static final String URL = "jdbc:mysql://localhost:3306/user_database";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "bibuzi34";

    public static Connection getConnection() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            logger.info("Connection to DB succeeded!");
        } catch (SQLException e) {
            logger.error("Connection failed: {}", e.getMessage());
        }
        return connection;
    }

    public static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
                logger.info("Connection closed successfully!");
            } catch (SQLException e) {
                logger.error("Error closing connection!: {}", e.getMessage());
            }
        }
    }
}