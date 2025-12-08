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
    omdb_vote_count INTEGER
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
            # print(f"TMDb: No results for “{title}”")
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
        # full_path = os.path.join(os.path.dirname(__file__), "tmdbmovies_output.json")
        # with open(full_path, "w", encoding="utf-8") as f:
        #     json.dump(tmdb_json, f, ensure_ascii=False, indent=4)
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
            # print(f"OMDb: No results for {title}")
            continue
        omdb_json = data

        print(f"OMDb fetched for: {title}")

        # Extract data from omdb; check for none
        omdb_rating = float(omdb_json.get("imdbRating")) if omdb_json.get("imdbRating") not in (None, "N/A") else None
        omdb_vote_count = int(omdb_json.get("imdbVotes").replace(",", "")) if omdb_json.get("imdbVotes") not in (None, "N/A") else None
        title = omdb_json.get("Title")

        cur.execute("""
            UPDATE movies
            SET
                imdb_id = ?,
                omdb_rating = ?,
                omdb_vote_count = ?
            WHERE title = ?
        """, (
            omdb_json.get("imdbID"),
            omdb_rating,
            omdb_vote_count,
            title
        ))

    # Writing to a file so I can see the output when testing
        # full_path = os.path.join(os.path.dirname(__file__), "omdbmovies_output.json")
        # with open(full_path, "w", encoding="utf-8") as f:
        #     json.dump(omdb_json, f, ensure_ascii=False, indent=4)

    conn.commit()
    conn.close()

def batchmovies(movie_titles, database_name, tmdbapi, omdbapi):
    index = 0
    batch_size = 25
    while index < len(movie_titles):
        storetmdb(movie_titles, index, database_name, batch_size, tmdbapi)
        storeomdb(movie_titles, index, database_name, batch_size, omdbapi)

        index += batch_size

def create_genre_db(database_name):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), database_name)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    tmdbgenres = {
            28: "Action",
            12: "Adventure",
            16: "Animation",
            35: "Comedy",
            80: "Crime",
            99: "Documentary",
            18: "Drama",
            10751: "Family",
            14: "Fantasy",
            36: "History",
            27: "Horror",
            10402: "Music",
            9648: "Mystery",
            10749: "Romance",
            878: "Science Fiction",
            10770: "TV Movie",
            53: "Thriller",
            10752: "War",
            37: "Western"
        }  
    cur.execute('''
        CREATE TABLE IF NOT EXISTS genres (genre_id INTEGER PRIMARY KEY, genre_name TEXT)
        ''')
    for id, genre in tmdbgenres.items():
        cur.execute('''
            INSERT OR REPLACE INTO genres (genre_id, genre_name) VALUES (?,?)
                    ''', (id, genre))
    conn.commit()
    conn.close()
    
def calculate_average_omdb_rating_by_genre(database_name):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), database_name)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    
    cur.execute('CREATE TEMPORARY TABLE moviesgenres (tmdb_id INTEGER, genre_id INTEGER)')
    cur.execute('SELECT tmdb_id, genre_ids FROM movies')
    movrows = cur.fetchall()
    for tmdb_id, genre_ids in movrows:
        if genre_ids:
            x = json.loads(genre_ids)
            # print(type(x))
            for genre in x:
                cur.execute('INSERT INTO moviesgenres (tmdb_id, genre_id) VALUES (?, ?)', (tmdb_id, genre))
        else: 
            print(f"No genres identified for {tmdb_id}")
            
    cur.execute('''
        SELECT genres.genre_name, ROUND(AVG(movies.omdb_rating), 2) FROM moviesgenres as movgen 
        JOIN movies ON movgen.tmdb_id = movies.tmdb_id
        JOIN genres ON movgen.genre_id = genres.genre_id
        WHERE movies.omdb_rating IS NOT NULL 
        GROUP BY genres.genre_name
        ORDER BY AVG(movies.omdb_rating)
                ''')
    results = cur.fetchall()
    # cur.execute("DROP TABLE moviesgenres")
    conn.commit()
    conn.close()
    return results

def calculate_releases_per_year(database_name):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), database_name)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("SELECT release_date FROM movies WHERE release_date IS NOT NULL AND release_date != ''")
    rows = cur.fetchall()
    year_counts = {}
    for (release_date,) in rows:
        year = release_date.split('-')[0]
        if year:
            if year not in year_counts:
                year_counts[year] = 0
            year_counts[year] += 1
    conn.close()
    sorted_years = dict(sorted(year_counts.items()))
    return sorted_years

def calculate_popularity_by_genre(database_name):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), database_name)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    
    cur.execute('CREATE TEMPORARY TABLE moviesgenres (tmdb_id INTEGER, genre_id INTEGER)')
    cur.execute('SELECT tmdb_id, genre_ids FROM movies')
    movrows = cur.fetchall()
    for tmdb_id, genre_ids in movrows:
        if genre_ids:
            for genre in json.loads(genre_ids):
                cur.execute('INSERT INTO moviesgenres (tmdb_id, genre_id) VALUES (?, ?)', (tmdb_id, genre))
        else: 
            print(f"No genres identified for {tmdb_id}")
            
    cur.execute('''
        SELECT genres.genre_name, ROUND(AVG(movies.tmdb_popularity), 2) FROM moviesgenres as movgen 
        JOIN movies ON movgen.tmdb_id = movies.tmdb_id
        JOIN genres ON movgen.genre_id = genres.genre_id
        WHERE movies.tmdb_popularity IS NOT NULL 
        GROUP BY genres.genre_name
        ORDER BY AVG(movies.tmdb_popularity)
                ''')
    results = cur.fetchall()
    # cur.execute("DROP TABLE moviesgenres")
    conn.commit()
    conn.close()
    return results


def plot_genre_rating_heatmap(genre_dict):
    import altair as alt
    import pandas as pd
    #make list of tuples a list of dict instead
    data = []
    for genre, rating in genre_dict:
        data.append({'Genre': genre, 'Average Rating': rating})
    #make data frame
    df = pd.DataFrame(data)
    chart = alt.Chart(df).mark_rect().encode(
        x=alt.X('Genre:N', title='Genre'),
        y=alt.Y('Average Rating:Q', title='Average Rating'),
        color=alt.Color('Average Rating:Q', 
                       scale=alt.Scale(scheme='viridis'),
                       title='Rating'),
        tooltip=['Genre', 'Average Rating']
    ).properties(
        width=600,
        height=400,
        title='Average OMDb Rating by Genre'
    )
    
    return chart

def plot_releases_by_year(year_dict):
    import altair as alt
    import pandas as pd
    #convert dict to list of dictionaries
    data = []
    for year, count in year_dict.items():
        data.append({'Year': int(year), 'Number of Movies': count})  
    #create data frame
    df = pd.DataFrame(data) 
    #create bar chart
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('Year:O', title='Year'),
        y=alt.Y('Number of Movies:Q', title='Number of Movies Released'),
        color=alt.Color('Number of Movies:Q', 
                       scale=alt.Scale(scheme='blues'),
                       title='Count'),
        tooltip=['Year', 'Number of Movies']
    ).properties(
        width=700,
        height=400,
        title='Number of Movies Released Per Year'
    )
    
    return chart

def plot_genre_popularity_heatmap(genre_dict):
    import altair as alt
    import pandas as pd
    #make list of tuples a list of dict instead
    data = []
    for genre, popscore in genre_dict:
        data.append({'Genre': genre, 'Average Popularity': popscore})
    #make data frame
    df = pd.DataFrame(data)
    chart = alt.Chart(df).mark_rect().encode(
        x=alt.X('Genre:N', title='Genre'),
        y=alt.Y('Average Popularity:Q', title='Average Popularity'),
        color=alt.Color('Average Popularity:Q', 
                       scale=alt.Scale(scheme='viridis'),
                       title='Popularity'),
        tooltip=['Genre', 'Average Popularity']
    ).properties(
        width=600,
        height=400,
        title='Average TMDb Popularity by Genre'
    )
    
    return chart

def writeresults(one, two, three):
    full_path = os.path.join(os.path.dirname(__file__), "results.txt")
    with open(full_path, "w") as f:
        txt = f"Average Rating by Genre: {one}\nReleases Per Year: {two}\nPopularity by Genre: {three}"
        f.writelines(txt)

def setup():
    movies = get_list_of_movies_to_add("Movietitles.json")
    tmdbapi = get_tmdb_api_key("tmdbapi.txt")
    omdbapi = get_omdb_api_key("omdbapi.txt")
    set_up_database("Movies.db")
    setup_table("Movies.db")
    batchmovies(movies, "Movies.db", tmdbapi, omdbapi)

def doingthings():
    #create genredb
    create_genre_db("Movies.db")
    #calculate average rating by genre
    genre_ratings = calculate_average_omdb_rating_by_genre("Movies.db")
    print("Average Rating by Genre:")
    print(genre_ratings)
    #calculate releases per year 
    releases_per_year = calculate_releases_per_year("Movies.db")
    print("\nReleases Per Year:")
    print(releases_per_year)
    #calculate popularity by genre
    popularity_by_genre = calculate_popularity_by_genre("Movies.db")
    print("\nAverage Popularity by Genre:")
    print(popularity_by_genre)
    #write out calculations to txt results file
    writeresults(genre_ratings, releases_per_year, popularity_by_genre)
    print("\nCalculation results saved in 'results.txt'")
    #create genre rating heatmap
    heatmap = plot_genre_rating_heatmap(genre_ratings)
    heatmap.save('ratings_heatmap.html')
    print("\nHeatmap saved as 'ratings_heatmap.html'")
    #create releases by year chart 
    releases_chart = plot_releases_by_year(releases_per_year)
    releases_chart.save('releases_by_year.html')
    print("\nReleases chart saved as 'releases_by_year.html'")
    #create genre popularity heatmap
    heatmap = plot_genre_popularity_heatmap(popularity_by_genre)
    heatmap.save('popularity_heatmap.html')
    print("\nHeatmap saved as 'popularity_heatmap.html'")

def main():
    setup()
    doingthings()

if __name__ == "__main__":
    main()


