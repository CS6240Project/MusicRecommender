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
    String query =
        "SELECT similar_item, SUM(similarity_score * user_score) AS recommendation_score " +
            "FROM ( " +
            "    SELECT " +
            "        CASE " +
            "            WHEN sim.item1 = ur.item_id THEN sim.item2 " +
            "            WHEN sim.item2 = ur.item_id THEN sim.item1 " +
            "        END AS similar_item, " +
            "        sim.similarity_score, " +
            "        ur.score AS user_score " +
            "    FROM item_similarity sim " +
            "    JOIN user_ratings ur " +
            "        ON sim.item1 = ur.item_id OR sim.item2 = ur.item_id " +
            "    WHERE ur.user_id = ? AND sim.similarity_score >= ? " +
            ") AS candidate_items " +
            "GROUP BY similar_item " +
            "ORDER BY recommendation_score DESC " +
            "LIMIT ?;";

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
      System.out.println("🎵 Welcome to the Music Recommendation System 🎵");
      System.out.println("Get ready to discover songs you'll love!");
      System.out.println("--------------------------------------------------");

      // Collect user inputs
      int userId = getUserId(scanner);
      double similarityThreshold = getSimilarityThreshold(scanner);
      int limit = getRecommendationLimit(scanner);

      // Generate recommendations
      List<Map.Entry<Integer, Double>> recommendations = getRecommendations(conn, userId, similarityThreshold, limit);

      System.out.println("\n✨ Hang tight! We're fetching your recommendations...");
      // Print recommendations
      printRecommendations(recommendations);

    } catch (SQLException e) {
      System.err.println("Failed to connect to database: " + e.getMessage());
    }
  }

  private static int getUserId(Scanner scanner) {
    int userId = -1;
    System.out.println("\uD83D\uDC64 First up, let's get to know you!");
    while (userId < 0) {

      System.out.print("Enter your User ID (e.g., 123): ");
      while (!scanner.hasNextInt()) {
        System.out.println("Invalid input. Please enter a positive number.");
        scanner.next(); // Clear invalid input
      }
      userId = scanner.nextInt();
      if (userId < 0) {
        System.out.println("User ID must be positive.");
      }
    }
    return userId;
  }

  private static double getSimilarityThreshold(Scanner scanner) {
    double similarityThreshold = -1;
    System.out.println("\n" + "\uD83D\uDD0D Now, let's fine-tune your recommendation search!");
    while (similarityThreshold <= 0 || similarityThreshold > 1) {
      System.out.print("Enter the Similarity Threshold (0 < value <= 1, higher = stricter recommendations): ");
      while (!scanner.hasNextDouble()) {
        System.out.println("Invalid input. Please enter a number between 0 and 1.");
        scanner.next(); // Clear invalid input
      }
      similarityThreshold = scanner.nextDouble();
      if (similarityThreshold <= 0 || similarityThreshold > 1) {
        System.out.println("Threshold must be between 0 and 1.");
      }
    }
    return similarityThreshold;
  }

  private static int getRecommendationLimit(Scanner scanner) {
    int limit = -1;
    System.out.println("\n" +
        "\uD83C\uDFAF Lastly, how many recommendations do you want?");
    while (limit <= 0) {
      System.out.print("Enter Number of Recommendations to you'd like to see: ");
      while (!scanner.hasNextInt()) {
        System.out.println("Invalid input. Please enter a positive number.");
        scanner.next(); // Clear invalid input
      }
      limit = scanner.nextInt();
      if (limit <= 0) {
        System.out.println("Number of recommendations must be positive.");
      }
    }
    return limit;
  }
  private static void printRecommendations(List<Map.Entry<Integer, Double>> recommendations) {
    System.out.println("\n\uD83C\uDF89 Your Personalized Recommendations \uD83C\uDF89");
    if (recommendations.isEmpty()) {
      System.out.println("No recommendations found for the given criteria.");
    } else {
      // Print table header
      System.out.printf("%-10s %-10s%n", "Item ID", "Score");
      System.out.println("--------------------------------------------------");
      // Print each recommendation
      for (Map.Entry<Integer, Double> entry : recommendations) {
        System.out.printf("%-10d %-10.2f%n", entry.getKey(), entry.getValue());
      }
    }
  }
}
