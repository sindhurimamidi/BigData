movies = LOAD '/Users/Sattya/Documents/Sattya_MS/Big_Data/HW6/movies.dat' USING org.apache.pig.piggybank.storage.MyRegExLoader('([^\\:]+)::([^\\:]+)::([^\\:]+)') as (movieId, title, genre);

ratings = LOAD '/Users/Sattya/Documents/Sattya_MS/Big_Data/HW6/ratings.dat' USING org.apache.pig.piggybank.storage.MyRegExLoader('([^\\:]+)::([^\\:]+)::([^\\:]+)::([^\\:]+)') as (userId, movieId, rating, timestamp);

movieRatings = join movies by movieId, ratings by movieId;
grpd = group movieRatings by movies::title;
avgRating = foreach grpd { movie = movieRatings.movies::title; generate group, AVG(movieRatings.ratings::rating) as ratingVal;};
sortedRating = order avgRating by ratingVal desc;
top25RatedMovies = limit sortedRating 25;
dump top25RatedMovies;