package com.moka.kafka.consumer;

import java.sql.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCTester {

    public static Logger logger = LoggerFactory.getLogger(JDBCTester.class);

    private static final String JDBC_URL = "jdbc:postgresql://192.168.64.50:5432/postgres";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "postgres";
    private static final String PING_QUERY = "SELECT 'postgresql is connected'";

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(PING_QUERY)) {

            if (rs.next()) {
                System.out.println(rs.getString(1));
            }
        } catch (SQLException e) {
            logger.error("Error connecting to database.", e);
        }
    }
}