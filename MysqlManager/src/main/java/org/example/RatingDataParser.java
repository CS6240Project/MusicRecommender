package org.example;

import java.io.*;
import java.sql.*;
import java.util.Properties;

public class RatingDataParser {

  public static void main(String[] args) {
    // Load database configuration
    Properties props = loadDatabaseProperties("db.properties");
    if (props == null) return;

    String DB_URL = props.getProperty("db.url");
    String USER = props.getProperty("db.user");
    String PASS = props.getProperty("db.password");

    String inputFile = "/Users/lin99nn/Downloads/dataset/ydata-ymusic-kddcup-2011-track1/testIdx1.txt";

    try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
      System.out.println("Database connected.");

      // Parse and insert ratings data
      parseAndInsertRatings(inputFile, conn);

      System.out.println("Data successfully inserted into the database!");
    } catch (SQLException e) {
      System.err.println("Database connection or query error: " + e.getMessage());
    }
  }

  /**
   * Loads database properties from a file.
   *
   * @param fileName The name of the properties file.
   * @return A Properties object with database configuration or null if loading fails.
   */
  private static Properties loadDatabaseProperties(String fileName) {
    Properties props = new Properties();
    try (InputStream input = RatingDataParser.class.getClassLoader().getResourceAsStream(fileName)) {
      if (input == null) {
        System.out.println("Can't find " + fileName);
        return null;
      }
      props.load(input);
    } catch (IOException e) {
      System.err.println("Error loading " + fileName + ": " + e.getMessage());
      return null;
    }
    return props;
  }

  /**
   * Parses the input file and inserts rating data into the database.
   *
   * @param inputFile The file containing user ratings data.
   * @param conn The database connection.
   */
  private static void parseAndInsertRatings(String inputFile, Connection conn) {
    try (
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        PreparedStatement ratingStmt = conn.prepareStatement(
            "INSERT INTO user_ratings (user_id, item_id, score) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE score=VALUES(score)"
        )
    ) {
      conn.setAutoCommit(false); // Start transaction

      String line;
      int currentUserId = -1;
      int remainingRatings = 0;

      while ((line = reader.readLine()) != null) {
        line = line.trim();

        if (line.isEmpty()) continue; // Skip empty lines

        if (line.contains("|")) {
          String[] userParts = line.split("\\|");
          currentUserId = Integer.parseInt(userParts[0].trim());
          remainingRatings = Integer.parseInt(userParts[1].trim());
        } else if (remainingRatings > 0) {
          handleRatingLine(line, ratingStmt, currentUserId);
          remainingRatings--;
        }
      }

      // Execute batched queries
      ratingStmt.executeBatch();
      conn.commit(); // Commit transaction
    } catch (IOException e) {
      System.err.println("File reading error: " + e.getMessage());
    } catch (SQLException e) {
      System.err.println("Database error: " + e.getMessage());
    }
  }

  /**
   * Processes a rating line and adds it to the batch for insertion.
   *
   * @param line The rating line from the input file.
   * @param ratingStmt The PreparedStatement for inserting rating data.
   * @param userId The user ID associated with the rating.
   * @throws SQLException If an error occurs while setting batch parameters.
   */
  private static void handleRatingLine(String line, PreparedStatement ratingStmt, int userId) throws SQLException {
    try {
      String[] parts = line.split("\\s+");
      int itemId = Integer.parseInt(parts[0].trim());
      int score = Integer.parseInt(parts[1].trim());

      ratingStmt.setInt(1, userId);
      ratingStmt.setInt(2, itemId);
      ratingStmt.setInt(3, score);
      ratingStmt.addBatch();
    } catch (NumberFormatException e) {
      System.err.println("Invalid rating line format, skipping: " + line);
    }
  }
}
