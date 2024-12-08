package org.example;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.Scanner;

public class RecommendationSystem {

  public static List<Map.Entry<Integer, Double>> getRecommendations(Connection conn, int userId, double similarityThreshold, int limit) {
    List<Map.Entry<Integer, Double>> recommendations = new ArrayList<>();

    // SQL query to calculate recommendation scores
    String query = """
                    SELECT similar_item, SUM(similarity_score * user_score) AS recommendation_score
                    FROM (
                        SELECT 
                            CASE 
                                WHEN sim.item1 = ur.item_id THEN sim.item2
                                WHEN sim.item2 = ur.item_id THEN sim.item1
                            END AS similar_item,
                            sim.similarity_score,
                            ur.score AS user_score
                        FROM item_similarity sim
                        JOIN user_ratings ur 
                            ON sim.item1 = ur.item_id OR sim.item2 = ur.item_id
                        WHERE ur.user_id = ? AND sim.similarity_score >= ?
                    ) AS candidate_items
                    GROUP BY similar_item
                    ORDER BY recommendation_score DESC
                    LIMIT ?;
                    """;
    try (PreparedStatement stmt = conn.prepareStatement(query)) {
      stmt.setInt(1, userId);
      stmt.setDouble(2, similarityThreshold);
      stmt.setInt(3, limit);
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        int itemId = rs.getInt("similar_item");
        double score = rs.getDouble("recommendation_score");
        recommendations.add(new AbstractMap.SimpleEntry<>(itemId, score));
      }

    } catch (SQLException e) {
      System.err.println("Database query error: " + e.getMessage());
    }

    return recommendations;
  }

  public static void main(String[] args) {
    // Load database configuration from properties file
    Properties props = new Properties();
    try (InputStream input = RecommendationSystem.class.getClassLoader().getResourceAsStream("db.properties")) {
      if (input == null) {
        System.out.println("Can't find db.properties file");
        return;
      }
      props.load(input);
    } catch (IOException e) {
      System.err.println("Error loading db.properties: " + e.getMessage());
      return;
    }

    String DB_URL = props.getProperty("db.url");
    String USER = props.getProperty("db.user");
    String PASS = props.getProperty("db.password");

    try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
         Scanner scanner = new Scanner(System.in)) {

      System.out.println("Database connected");

      // Prompt the user for input values
      System.out.print("Enter User ID: ");
      int userId = scanner.nextInt();

      System.out.print("Enter Similarity Threshold (e.g., 0.8): ");
      double similarityThreshold = scanner.nextDouble();

      System.out.print("Enter Number of Recommendations to Retrieve: ");
      int limit = scanner.nextInt();

      // Generate recommendations
      List<Map.Entry<Integer, Double>> recommendations = getRecommendations(conn, userId, similarityThreshold, limit);

      // Print recommendations
      System.out.println("Recommendations:");
      if (recommendations.isEmpty()) {
        System.out.println("No recommendations found for the given criteria.");
      } else {
        for (Map.Entry<Integer, Double> entry : recommendations) {
          System.out.printf("Item ID: %d, Score: %.2f%n", entry.getKey(), entry.getValue());
        }
      }

    } catch (SQLException e) {
      System.err.println("Failed to connect to database: " + e.getMessage());
    }
  }
}
