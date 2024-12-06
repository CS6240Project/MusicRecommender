# MusicRecommender
## SQL Table format
**item_similarity(
                   item1 INT NOT NULL,
                   item2 INT NOT NULL,
                   similarity_score DOUBLE NOT NULL,
                   PRIMARY KEY (item1, item2)
)**
### Indexing 
Added index to item1 for fast searching
