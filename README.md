This program takes data from the TMDB and OMDB APIs and performs several calculations and data visualizations.
In order to get started, acquire an API key from https://www.omdbapi.com/ and https://developer.themoviedb.org/docs/getting-started.

In the same directory your Python file is located, create a file called "omdbapi.txt" and "tmdbapi.txt", and paste your omdb api key and tmdb api key in each one's respective file. 
Additionally, download the "Movietitles.json" file for an example list of movies, or create your own "Movietitles.json" file in the same directory as your Python file in order to search up and store a data base of your own movies.

In your directory, run the following two commands to ensure the data visualizations are properly created:
pip install altair
pip install pandas

The program will create an SQLite database in the same folder as your directory. 
Note: The function automatically runs itself with a batch size of 25, until it has exhausted the limit of the list of movies in "Movietitles.json" - the function only batches in groups of 25, but will add all movies to its database in one run.

Repo Contents:
main.py - This is the main codebase and contains all functions which create the databases using APIs, join them, perform calculations, and develop visualizations.
Movietitles.json - This is an example input of a json file of a list of movies which our program then looks up using APIs
Movies.db - This is an example database output of what the program will build, given a list of movies
ratings_heatmap.html - This is an example output of the heatmap file which is created by calculating the average ratings of movies, categorized by genre
releases_by_year.html - This is an example output of the bargraph file which is created by counting how many movies were released every year
popularity_heatmap.html - This is an example output of the heatmap file which is created by calculating the average popularity of movies, categorized by genre
results.txt - This is the file which contains the text final calculations of the functions we used.
