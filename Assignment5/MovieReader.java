import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;



import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileReader;

public class MovieReader {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();


        HBaseAdmin admin = new HBaseAdmin(conf);


        // Create tables for books, users, ratings
        HTableDescriptor moviesTableDescriptor = new HTableDescriptor(TableName.valueOf("movies"));
        HTableDescriptor ratingsTableDescriptor = new HTableDescriptor(TableName.valueOf("moviesRatings"));
        HTableDescriptor usersTableDescriptor = new HTableDescriptor(TableName.valueOf("moviesUsers"));

        moviesTableDescriptor.addFamily(new HColumnDescriptor("movieData"));
        ratingsTableDescriptor.addFamily(new HColumnDescriptor("ratingData"));
        usersTableDescriptor.addFamily(new HColumnDescriptor("userData"));

        admin.createTable(moviesTableDescriptor);
        admin.createTable(ratingsTableDescriptor);
        admin.createTable(usersTableDescriptor);

        // Get a handle to the tables and read data into them from files
        HTable moviesTable = new HTable(conf, "movies");
        HTable ratingsTable = new HTable(conf, "moviesRatings");
        HTable usersTable = new HTable(conf, "moviesUsers");

        String path = new File("movies.dat").getAbsolutePath();
        File file = new File(path);
        BufferedReader bufferReader = new BufferedReader(new FileReader(file));
        String line;
        while ((line = bufferReader.readLine()) != null)   {
            String[] token= line.split("::");
            System.out.println("line is "+line);
            Put movie = new Put(Bytes.toBytes(token[0]));
            movie.add(Bytes.toBytes("movieData"), Bytes.toBytes("title"), Bytes.toBytes(token[1]));
            movie.add(Bytes.toBytes("movieData"), Bytes.toBytes("genre"), Bytes.toBytes(token[2]));
            moviesTable.put(movie);
        }
        bufferReader.close();
        moviesTable.flushCommits();
        moviesTable.close();

        path = new File("users.dat").getAbsolutePath();
        file = new File(path);
        bufferReader = new BufferedReader(new FileReader(file));
        while ((line = bufferReader.readLine()) != null)   {
            System.out.println("line is "+line);
            String[] token= line.split("::");
            Put user = new Put(Bytes.toBytes(token[0]));
            user.add(Bytes.toBytes("userData"), Bytes.toBytes("gender"), Bytes.toBytes(token[1]));
            user.add(Bytes.toBytes("userData"), Bytes.toBytes("age"), Bytes.toBytes(token[2]));
            user.add(Bytes.toBytes("userData"), Bytes.toBytes("occupation"), Bytes.toBytes(token[3]));
            user.add(Bytes.toBytes("userData"), Bytes.toBytes("zipcode"), Bytes.toBytes(token[4]));
            usersTable.put(user);
        }
        bufferReader.close();
        usersTable.flushCommits();
        usersTable.close();

        path = new File("ratings.dat").getAbsolutePath();
        file = new File(path);
        bufferReader = new BufferedReader(new FileReader(file));
        bufferReader.readLine();
        while ((line = bufferReader.readLine()) != null)   {
            String[] token= line.split("::");
            Put rating = new Put(Bytes.toBytes(token[0]+" "+token[1]));
            rating.add(Bytes.toBytes("ratingData"), Bytes.toBytes("userId"), Bytes.toBytes(token[0]));
            rating.add(Bytes.toBytes("ratingData"), Bytes.toBytes("movieId"), Bytes.toBytes(token[1]));
            rating.add(Bytes.toBytes("ratingData"), Bytes.toBytes("rating"), Bytes.toBytes(token[2]));
            rating.add(Bytes.toBytes("ratingData"), Bytes.toBytes("timestamp"), Bytes.toBytes(token[3]));
            ratingsTable.put(rating);
        }
        bufferReader.close();
        ratingsTable.flushCommits();
        ratingsTable.close();
    }
}
