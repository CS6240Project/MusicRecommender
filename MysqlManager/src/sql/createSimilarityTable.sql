DROP TABLE IF EXISTS item_similarity;

CREATE TABLE item_similarity (
                                 item1 INT NOT NULL,
                                 item2 INT NOT NULL,
                                 similarity_score DOUBLE NOT NULL,
                                 PRIMARY KEY (item1, item2)
);

CREATE INDEX idx_item_id ON item_similarity (item1);