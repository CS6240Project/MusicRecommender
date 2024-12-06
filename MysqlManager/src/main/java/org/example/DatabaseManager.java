package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseManager {
    // Database credentials
    private static String DB_URL;
    private static String USER;
    private static String PASS;

    public static Connection getConnection() throws SQLException {
        try {
            // Load the JDBC driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            // Get a connection to the database
            return DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new SQLException("MySQL JDBC Driver not found.", e);
        }
    }

//    public void insertItemSimilarity(int itemId, int similarItemId, double similarityScore) {
//        String sql = "INSERT INTO item_similarity (item_id, similar_item_id, similarity_score) VALUES (?, ?, ?)";
//        try (Connection conn = getConnection();
//             PreparedStatement pstmt = conn.prepareStatement(sql)) {
//
//            pstmt.setInt(1, itemId);
//            pstmt.setInt(2, similarItemId);
//            pstmt.setDouble(3, similarityScore);
//            pstmt.executeUpdate();
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    // Class to represent a record
    public static class ItemSimilarityRecord {
        private int itemId;
        private int similarItemId;
        private double similarityScore;

        public ItemSimilarityRecord(int itemId, int similarItemId, double similarityScore) {
            this.itemId = itemId;
            this.similarItemId = similarItemId;
            this.similarityScore = similarityScore;
        }
    }

    public List<ItemSimilarityRecord> loadDataFromFile(String filePath) {
        List<ItemSimilarityRecord> records = new ArrayList<>();
        String line;

        // Compile the regex pattern once
        Pattern pattern = Pattern.compile("\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)\\s+(\\d+\\.\\d+|\\d+\\.\\d+E[-+]?[0-9]+|\\d+)");

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            while ((line = br.readLine()) != null) {
                // Skip empty lines
                if (line.trim().isEmpty()) {
                    continue;
                }

                // Trim the line
                line = line.trim();

                // Use the pattern to match the line
                Matcher matcher = pattern.matcher(line);
                if (matcher.matches()) {
                    try {
                        // Extract and parse the integers and double
                        int itemId = Integer.parseInt(matcher.group(1));
                        int similarItemId = Integer.parseInt(matcher.group(2));
                        double similarityScore = Double.parseDouble(matcher.group(3));

                        // Collect the record
                        records.add(new ItemSimilarityRecord(itemId, similarItemId, similarityScore));
                    } catch (NumberFormatException e) {
                        System.err.println("Error parsing numbers in line: " + line);
                        e.printStackTrace();
                    }
                } else {
                    System.err.println("Invalid line format: " + line);
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading file: " + filePath);
            e.printStackTrace();
        }

        return records;
    }

    public void insertItemSimilarityBatch(List<ItemSimilarityRecord> records) {
        String sql = "INSERT INTO item_similarity (item1, item2, similarity_score) VALUES (?, ?, ?)";

        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            for (ItemSimilarityRecord record : records) {
                pstmt.setInt(1, record.itemId);
                pstmt.setInt(2, record.similarItemId);
                pstmt.setDouble(3, record.similarityScore);
                pstmt.addBatch();
            }

            pstmt.executeBatch();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void testConnection() {
        try (Connection conn = getConnection()) {
            if (conn != null) {
                System.out.println("Successfully connected to the database.");
            } else {
                System.out.println("Failed to connect to the database.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        try (InputStream input = DatabaseManager.class.getClassLoader().getResourceAsStream("db.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find db.properties");
                return;
            }
            props.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
            return;
        }

        DB_URL = props.getProperty("db.url");
        USER = props.getProperty("db.user");
        PASS = props.getProperty("db.password");

        // Print properties to verify they are loaded
        System.out.println("DB_URL: " + DB_URL);
        System.out.println("USER: " + USER);

        // Initialize DatabaseManager instance
        DatabaseManager dbManager = new DatabaseManager();

        // Test the database connection
        testConnection();

        // Specify the file path
        String filePath = "As1/src/ItemSimilarity/part-r-00000"; // Replace with your actual file path

        // Load data from the file
        List<ItemSimilarityRecord> records = dbManager.loadDataFromFile(filePath);

        // Insert data in batch
        dbManager.insertItemSimilarityBatch(records);

        System.out.println("Data loading completed.");
    }
}