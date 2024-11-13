# Modify these:
train_data_path = "/home/mikewu/MusicRecommender/large/trainIdx1.txt"
validation_data_path = "/home/mikewu/MusicRecommender/large/validationIdx1.txt"
test_data_path = "/home/mikewu/MusicRecommender/large/testIdx1.txt"

def read_first_n_users(file_path, n=10):
    users_data = []
    with open(file_path, 'r') as file:
        user_count = 0
        while user_count < n:
            user_line = file.readline()
            if not user_line:
                break
            user_line = user_line.strip()
            if not user_line:
                continue
            # Parse user info
            if '|' in user_line:
                user_id, num_ratings = user_line.split('|')
                num_ratings = int(num_ratings)
                ratings = []
                # Read the next num_ratings lines
                for _ in range(num_ratings):
                    rating_line = file.readline().strip()
                    if not rating_line:
                        break
                    ratings.append(rating_line)
                # Store the user's data
                users_data.append({
                    'user_id': user_id,
                    'num_ratings': num_ratings,
                    'ratings': ratings
                })
                user_count += 1
            else:
                continue  # Skip lines that don't match expected format
    return users_data

def read_all_users(file_path):
    print(f"Reading file: {file_path}")
    users_data = []
    with open(file_path, 'r') as file:
        while True:
            user_line = file.readline()
            if not user_line:
                break  # End of file reached
            user_line = user_line.strip()
            if not user_line:
                continue  # Skip empty lines

            if '|' in user_line:
                try:
                    user_id, num_ratings = [x.strip() for x in user_line.split('|', 1)]
                    num_ratings = int(num_ratings)
                except ValueError:
                    print(f"Invalid user line format: '{user_line}'. Skipping.")
                    continue

                ratings = []
                for _ in range(num_ratings):
                    rating_line = file.readline()
                    if not rating_line:
                        print(f"End of file reached unexpectedly while reading ratings for user {user_id}.")
                        break
                    rating_line = rating_line.strip()
                    if rating_line:
                        ratings.append(rating_line)
                    else:
                        print(f"Empty rating line encountered for user {user_id}. Skipping line.")

                users_data.append({
                    'user_id': user_id,
                    'num_ratings': num_ratings,
                    'ratings': ratings
                })
            else:
                print(f"Line does not contain user data: '{user_line}'. Skipping.")
                continue
    return users_data

def parse_rating_line(line):
    fields = line.strip().split('\t')
    item_id = fields[0]
    score = int(fields[1])
    # time format: date:hh:mm:ss
    time = ':'.join(fields[2:]) if len(fields) > 2 else None
    return {
        'item_id': item_id,
        'score': score,
        'time': time
    }



train_users = read_all_users(train_data_path)
validation_users = read_all_users(validation_data_path)
test_users = read_all_users(test_data_path)

# Parse the ratings for each user
def parse_users_data(users_data):
    print("Parsing data...")
    parsed_data = []
    for user in users_data:
        user_id = user['user_id']
        num_ratings = user['num_ratings']
        ratings = [parse_rating_line(rating) for rating in user['ratings']]
        parsed_data.append({
            'user_id': user_id,
            'num_ratings': num_ratings,
            'ratings': ratings
        })
    return parsed_data

parsed_train_data = parse_users_data(train_users)
parsed_validation_data = parse_users_data(validation_users)
parsed_test_data = parse_users_data(test_users)

# Print the parsed data for verification
print("Train Data Sample:")
for i in range(10):
    user_data = parsed_train_data[i]
    print(f"User ID: {user_data['user_id']}, Number of Ratings: {user_data['num_ratings']}")
    for rating in user_data['ratings']:
        print(f"  {rating}")
    print()