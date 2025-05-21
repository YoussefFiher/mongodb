import json
import os
from pymongo import MongoClient
from collections import defaultdict

client = MongoClient('mongodb://localhost:27017/')
db = client['Straming_db']
movies_collection = db['movies']
workers_collection = db['workers']

# utilise ton propre path ou il se situe les données 
data_dir = "C:/Users/LEGION/OneDrive/Bureau/BIG_DATA/projet_groupe/projet_2025/data/generated_data/small/mongodb"

def load_json_files(filename):
    with open(os.path.join(data_dir, filename), 'r') as f:
        return json.load(f)



workers = {w["id"]: w for w in load_json_files("workers.json")}
genres = {g["id"]: g for g in load_json_files("genres.json")}
awards = {a["id"]: a for a in load_json_files("awards.json")}
movies = {m["id"]: m for m in load_json_files("movies.json")}


movies_genres = load_json_files("movies_genres.json")
movies_actors = load_json_files("movies_actors.json")
movies_awards = load_json_files("movies_awards.json")
ratings = load_json_files("ratings.json")



related_genres = defaultdict(list)
for mg in movies_genres:
    related_genres[mg["movie_id"]].append(genres[mg["genre_id"]])

related_actors = defaultdict(list)
for ma in movies_actors:
    actor = workers[ma["actor_id"]]
    related_actors[ma["movie_id"]].append({
        "_id": actor["id"],
        "name": actor["name"],
        "role": ma["role"]
    })

related_awards = defaultdict(list)
for ma in movies_awards:
    award = awards[ma["award_id"]]
    related_awards[ma["movie_id"]].append({
        "_id": award["id"],
        "name": award["name"],
        "category": award["category"],
        "year": ma["year"]
    })


ratings_per_movie = defaultdict(list)
for r in ratings:
    ratings_per_movie[r["movie_id"]].append(r["rating"])

def compute_avg_rating(ratings):
    if not ratings:
        return 0
    return round(sum(ratings) / len(ratings), 2)


transformed_movies = []

for movie_id, movie in movies.items():
    director = workers[movie["director_id"]]
    
    transformed_movies.append({
        "_id": movie["id"],
        "title": movie["title"],
        "year": movie["year"],
        "director": {
            "_id": director["id"],
            "name": director["name"]
        },
        "genres": [
            {"_id": g["id"], "name": g["name"]} 
            for g in related_genres.get(movie_id, [])
        ],
        "actors": related_actors.get(movie_id, []),
        "awards": related_awards.get(movie_id, []),
        "metadata": movie["metadata"],
        "average_rating": compute_avg_rating(ratings_per_movie.get(movie_id, [])),
        "ratings_count": len(ratings_per_movie.get(movie_id, []))
    })


movies_collection.insert_many(transformed_movies)
print(f" {len(transformed_movies)} films insérés dans la collection 'movies'")