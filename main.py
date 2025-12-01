'''
SI 201 Final Project CinemaSynapse
Team Members: Julian Liao, Daniel Xiao
'''

import sqlite3
import json
import os
import requests

def get_list_of_movies_to_add(filename):
    full_path = os.path.join(os.path.dirname(__file__), filename)
    f = open(full_path)
    file_data = f.read()
    f.close()
    json_data = json.loads(file_data)
    return json_data


def set_up_database(db_name):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),db_name)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    return cur, conn

def get_tmdb_api_key(fname):
    full_path = os.path.join(os.path.dirname(__file__), fname)
    with open(full_path) as f:
        key = f.read()
    return key

def get_omdb_api_key(fname):
    full_path = os.path.join(os.path.dirname(__file__), fname)
    with open(full_path) as f:
        key = f.read()
    return key

def setup_table(db_name):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),db_name)
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    cur.execute('''
    CREATE TABLE IF NOT EXISTS movies (
    tmdb_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    release_date TEXT,
    overview TEXT,
    genre_ids TEXT,
    tmdb_rating REAL,
    tmdb_vote_count INTEGER,
    tmdb_popularity REAL,
    imdb_id TEXT,
    omdb_rating REAL,
    omdb_vote_count INTEGER,
    rotten_tomatoes_rating TEXT,
    metacritic_score TEXT,
    maturity_rating TEXT,
    omdb_genres TEXT
    )
    '''
    )
    conn.commit()
    conn.close()

def storetmdb(movie_titles, start_index, db_name, batch_size, TMDBKEY):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),db_name)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    end_index = min(start_index + batch_size, len(movie_titles))
    for title in movie_titles[start_index:end_index]:
        url = (
            f"https://api.themoviedb.org/3/search/movie"
            f"?api_key={TMDBKEY}&query={title}"
        )
        response = requests.get(url)
        data = response.json()
        if not data.get("results"):
            print(f"TMDb: No results for “{title}”")
            continue
        tmdb_json = data["results"][0]
        

        print(f"TMDb fetched for: {title}")

        # inserting data
        tmdb_id = tmdb_json.get("id")
        title = tmdb_json.get("title")
        release_date = tmdb_json.get("release_date")
        overview = tmdb_json.get("overview")
        genre_ids = json.dumps(tmdb_json.get("genre_ids", []))
        tmdb_rating = tmdb_json.get("vote_average")
        tmdb_vote_count = tmdb_json.get("vote_count")
        tmdb_popularity = tmdb_json.get("popularity")

        cur.execute("""
            INSERT OR REPLACE INTO movies (
                tmdb_id, title, release_date, overview,
                genre_ids, tmdb_rating, tmdb_vote_count, tmdb_popularity
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            tmdb_id, title, release_date, overview,
            genre_ids, tmdb_rating, tmdb_vote_count, tmdb_popularity,
        ))

        # Writing to a file so I can see the output when testing
        full_path = os.path.join(os.path.dirname(__file__), "tmdbmovies_output.json")
        with open(full_path, "w", encoding="utf-8") as f:
            json.dump(tmdb_json, f, ensure_ascii=False, indent=4)
    conn.commit()
    conn.close()

def storeomdb(movie_titles, start_index, db_name, batch_size, OMDBKEY):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), db_name)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    end_index = min(start_index + batch_size, len(movie_titles))
    for title in movie_titles[start_index:end_index]:
        url = (f"http://www.omdbapi.com/?t={title}&apikey={OMDBKEY}")
        response = requests.get(url)
        data = response.json()
        if data.get("Response") == "False":
            print(f"OMDb: No results for {title}")
            continue
        omdb_json = data

        print(f"OMDb fetched for: {title}")

        # Extract data from omdb; check for none
        omdb_rating = float(omdb_json.get("imdbRating")) if omdb_json.get("imdbRating") not in (None, "N/A") else None
        omdb_vote_count = int(omdb_json.get("imdbVotes").replace(",", "")) if omdb_json.get("imdbVotes") not in (None, "N/A") else None
        maturity_rating = omdb_json.get("Rated") if omdb_json.get("Rated") not in (None, "N/A") else None
        omdb_genres = omdb_json.get("Genre") if omdb_json.get("Genre") not in (None, "N/A") else None
        ratings = omdb_json.get("Ratings", [])
        rotten_tomatoes_rating = None
        metacritic_score = None
        for r in ratings:
            source = r.get("Source")
            value = r.get("Value")
            if source == "Rotten Tomatoes":
                rotten_tomatoes_rating = value
            elif source == "Metacritic":
                metacritic_score = value
        title = omdb_json.get("Title")

        cur.execute("""
            UPDATE movies
            SET
                imdb_id = ?,
                omdb_rating = ?,
                omdb_vote_count = ?,
                rotten_tomatoes_rating = ?,
                metacritic_score = ?,
                maturity_rating = ?,
                omdb_genres = ?
            WHERE title = ?
        """, (
            omdb_json.get("imdbID"),
            omdb_rating,
            omdb_vote_count,
            rotten_tomatoes_rating,
            metacritic_score,
            maturity_rating,
            omdb_genres,
            title
        ))

    # Writing to a file so I can see the output when testing
        full_path = os.path.join(os.path.dirname(__file__), "omdbmovies_output.json")
        with open(full_path, "w", encoding="utf-8") as f:
            json.dump(omdb_json, f, ensure_ascii=False, indent=4)

    conn.commit()
    conn.close()

def batchmovies(movie_titles, database_name, tmdbapi, omdbapi):
    index = 0
    batch_size = 25
    while index < len(movie_titles):
        print(f"\n=== Processing batch starting at {index} ===")

        storetmdb(movie_titles, index, database_name, batch_size, tmdbapi)
        storeomdb(movie_titles, index, database_name, batch_size, omdbapi)

        index += batch_size

def main():
    movies = get_list_of_movies_to_add("Movietitles.json")
    tmdbapi = get_tmdb_api_key("tmdbapi.txt")
    omdbapi = get_omdb_api_key("omdbapi.txt")
    set_up_database("Movies.db")
    setup_table("Movies.db")
    batchmovies(movies, "Movies.db", tmdbapi, omdbapi)
if __name__ == "__main__":
    main()


