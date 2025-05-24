import json
import os
from pymongo import MongoClient
from collections import defaultdict

client = MongoClient('mongodb://localhost:27017/')
db = client['Straming_db']
movies_collection = db['movies']
workers_collection = db['workers']
Users_collection = db['users']
# utilise ton propre path ou il se situe les données 
data_dir = "C:/Users/LEGION/OneDrive/Bureau/BIG_DATA/projet_groupe/projet_2025/data/generated_data/medium/mongodb"

def load_json_files(filename):
    with open(os.path.join(data_dir, filename), 'r') as f:
        return json.load(f) 
    

ratings = load_json_files("ratings.json")

ratings_per_movie = defaultdict(list)
for r in ratings:
    ratings_per_movie[r["movie_id"]].append(r["rating"])

print(ratings_per_movie)
    




