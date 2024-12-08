CREATE TABLE user_ratings (
                              user_id INT NOT NULL,          -- The ID of the user providing the rating
                              item_id INT NOT NULL,          -- The ID of the item being rated
                              score INT NOT NULL,            -- The user's rating for the item (0-100)
                              PRIMARY KEY (user_id, item_id) -- Ensures a user can rate an item only once
);
