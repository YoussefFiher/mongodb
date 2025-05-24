from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['Streaming_db']



#request 1
def GetBestMovies():
    return db.movies.find({ "average_rating": { "$gt": 4.5 } })

for movie in GetBestMovies():
    print(movie["title"], "-", movie["average_rating"])

def get_movie_by_title(title):
    return db.movies.find_one({ "title": title })

movie = get_movie_by_title("Success may")
if movie:
    print("\n--- Fiche complète du film ---")
    print("Titre :", movie["title"])
    print("Année :", movie["year"])
    print("Réalisateur :", movie["director"]["name"])
    print("Acteurs :")
    for actor in movie["actors"]:
        print("-", actor["name"], "dans le rôle de", actor["role"])
    print("Récompenses :")
    for award in movie.get("awards", []):
        print("-", award["name"], "(", award["category"], ")", "-", award["year"])
    print("Note moyenne :", movie["average_rating"])
else:
    print("Film non trouvé.")


# def recommend_movies_based_on_actors(user_id):
#     pipeline = [
#         # Étape 1 : trouver les films que l'utilisateur a déjà vus
#         { "$match": { "user_id": user_id } },
#         { "$lookup": {
#             "from": "movies",
#             "localField": "movie_id",
#             "foreignField": "_id",
#             "as": "watched_movies"
#         }},
#         { "$unwind": "$watched_movies" },
#         { "$unwind": "$watched_movies.actors" },
#         { "$group": {
#             "_id": None,
#             "actors_seen": { "$addToSet": "$watched_movies.actors._id" }
#         }},
        
#         # Étape 2 : récupérer les films avec ces acteurs
#         { "$lookup": {
#             "from": "movies",
#             "let": { "actorsSeen": "$actors_seen" },
#             "pipeline": [
#                 { "$match": {
#                     "$expr": {
#                         "$gt": [
#                             { "$size": {
#                                 "$setIntersection": [
#                                     "$$actorsSeen",
#                                     { "$map": { "input": "$actors", "as": "a", "in": "$$a._id" } }
#                                 ]
#                             }},
#                             0
#                         ]
#                     }
#                 }}
#             ],
#             "as": "recommended_movies"
#         }},
#         { "$unwind": "$recommended_movies" },
        
#         # Voir le résultat intermédiaire
#         { "$replaceRoot": { "newRoot": "$recommended_movies" } },
#         { "$project": { "_id": 1, "title": 1, "actors": 1 } }
#     ]

#     return list(db.watch_history.aggregate(pipeline))

def recommend_movies_based_on_actors(user_id):
    pipeline = [
        # Étape 1 : trouver les films que l'utilisateur a déjà vus
        { "$match": { "user_id": user_id } },
        { "$lookup": {
            "from": "movies",
            "localField": "movie_id",
            "foreignField": "_id",
            "as": "watched_movies"
        }},
        { "$unwind": "$watched_movies" },
        { "$unwind": "$watched_movies.actors" },
        { "$group": {
            "_id": None,
            "actors_seen": { "$addToSet": "$watched_movies.actors._id" },
            "movies_seen": { "$addToSet": "$watched_movies._id" }
        }},
        
        # Étape 2 : récupérer les films avec ces acteurs
        { "$lookup": {
            "from": "movies",
            "let": {
                "actorsSeen": "$actors_seen",
                "moviesSeen": "$movies_seen"
            },
            "pipeline": [
                { "$match": {
                    "$expr": {
                        "$and": [
                            # Au moins un acteur en commun
                            { "$gt": [
                                { "$size": {
                                    "$setIntersection": [
                                        "$$actorsSeen",
                                        { "$map": {
                                            "input": "$actors",
                                            "as": "a",
                                            "in": "$$a._id"
                                        }}
                                    ]
                                }},
                                0
                            ]},
                            # Le film ne doit pas déjà être vu
                            { "$not": { "$in": [ "$_id", "$$moviesSeen" ] }}
                        ]
                    }
                }}
            ],
            "as": "recommended_movies"
        }},
        { "$unwind": "$recommended_movies" },

        # Étape 3 : éliminer les doublons
        { "$replaceRoot": { "newRoot": "$recommended_movies" } },
        { "$group": {
            "_id": "$_id",
            "title": { "$first": "$title" },
            "actors": { "$first": "$actors" }
        }}
    ]

    return list(db.watch_history.aggregate(pipeline))


results = recommend_movies_based_on_actors("c28599e2-8361-492f-afe2-0513bca8955f")

for movie in results:
    print(movie['title'])
