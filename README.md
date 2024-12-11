# MusicRecommender
## Workflow

### Data Ingestion
1. **`user_ratings` Table**:
   - Populated by `RatingDataParser`, which parses user ratings in the specified file format and performs batch inserts into the database.
2. **`item_similarity` Table**:
   - Populated by `DatabaseManager`, which processes a similarity file to batch-insert records into the database.

### Recommendation Generation
1. The `RecommendationSystem` queries `user_ratings` to retrieve user preferences.
2. It uses `item_similarity` to find items related to those the user has rated, applying a similarity threshold.
3. Recommendation scores are calculated based on a combination of user ratings and item similarity scores.

---

## Database Overview
### Tables
#### 1. `user_ratings` Table
- **Purpose**: Stores individual user ratings for items, representing user preferences.
- **Schema**:
  - `user_id` (INT, PRIMARY KEY)
  - `item_id` (INT, PRIMARY KEY)
  - `score` (INT): The rating score assigned by the user.

- **Usage**:
  - **Insertion**: Handled by `RatingDataParser`.
  - **Query**: Provides user recommendations in conjunction with the `item_similarity` table.

---

#### 2. `item_similarity` Table
- **Purpose**: Stores similarity scores between pairs of items.
- **Schema**:
  - `item1` (INT, PRIMARY KEY)
  - `item2` (INT, PRIMARY KEY)
  - `similarity_score` (DOUBLE): The calculated similarity score between `item1` and `item2`.

- **Features**:
  - Add index to `item1` for fast searching.
  - The `similarity_score` indicates the strength of the relationship between items.

- **Usage**:
  - **Insertion**: Managed by `DatabaseManager`.
  - **Query**: Used by the `RecommendationSystem` to find related items and compute recommendation scores.
