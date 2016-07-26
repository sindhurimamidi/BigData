ratings = LOAD '/Users/Sattya/Documents/Sattya_MS/Big_Data/HW6/ratings.dat' USING org.apache.pig.piggybank.storage.MyRegExLoader('([^\\:]+)::([^\\:]+)::([^\\:]+)::([^\\:]+)') as (userId, movieId, rating, timestamp);

 grpd = group ratings by movieId;
 ratingsCnt = foreach grpd { movieId = ratings.movieId; generate group, COUNT(movieId) as userCnt;};
 multipleUsers = filter ratingsCnt by userCnt > 1;
 dump multipleUsers;