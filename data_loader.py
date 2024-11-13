def read_first_n_lines(file_path, n=10):
    lines = []
    with open(file_path, 'r') as file:
        for _ in range(n):
            try:
                line = next(file).strip()
                if line:
                    lines.append(line)
            except StopIteration:
                break
    return lines

def read_all_lines(file_path):
    lines = []
    with open(file_path, 'r') as file:
        while True:
            try:
                line = next(file).strip()
                if line:
                    lines.append(line)
            except StopIteration:
                break
    return lines

# Modify these
album_data_path = "/home/mikewu/MusicRecommender/large/albumData1.txt"
artist_data_path = "/home/mikewu/MusicRecommender/large/artistData1.txt"
genre_data_path = "/home/mikewu/MusicRecommender/large/genreData1.txt"
track_data_path = "/home/mikewu/MusicRecommender/large/trackData1.txt"

album_lines = read_all_lines(album_data_path)
artist_lines = read_all_lines(artist_data_path)
genre_lines = read_all_lines(genre_data_path)
track_lines = read_all_lines(track_data_path)

# Parse to objects
def parse_album_line(line):
    fields = line.split('|')
    album_id = fields[0]
    artist_id = fields[1]
    genre_ids = fields[2:] if len(fields) > 2 else []
    return {
        'album_id': album_id,
        'artist_id': artist_id,
        'genre_ids': genre_ids
    }

def parse_artist_line(line):
    artist_id = line.strip()
    return {'artist_id': artist_id}

def parse_genre_line(line):
    genre_id = line.strip()
    return {'genre_id': genre_id}

def parse_track_line(line):
    fields = line.split('|')
    track_id = fields[0]
    album_id = fields[1]
    artist_id = fields[2]
    genre_ids = fields[3:] if len(fields) > 3 else []
    return {
        'track_id': track_id,
        'album_id': album_id,
        'artist_id': artist_id,
        'genre_ids': genre_ids
    }

# Parse the lines into structured data
parsed_album_data = [parse_album_line(line) for line in album_lines]
parsed_artist_data = [parse_artist_line(line) for line in artist_lines]
parsed_genre_data = [parse_genre_line(line) for line in genre_lines]
parsed_track_data = [parse_track_line(line) for line in track_lines]

# print("Album Data Sample:")
# for data in parsed_album_data:
#     print(data)

# print("\nArtist Data Sample:")
# for data in parsed_artist_data:
#     print(data)

# print("\nGenre Data Sample:")
# for data in parsed_genre_data:
#     print(data)

# print("\nTrack Data Sample:")
# for data in parsed_track_data:
#     print(data)