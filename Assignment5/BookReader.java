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

public class BookReader {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();


        HBaseAdmin admin = new HBaseAdmin(conf);


        // Create tables for books, users, ratings
        HTableDescriptor booksTableDescriptor = new HTableDescriptor(TableName.valueOf("books"));
        HTableDescriptor ratingsTableDescriptor = new HTableDescriptor(TableName.valueOf("ratings"));
        HTableDescriptor usersTableDescriptor = new HTableDescriptor(TableName.valueOf("users"));

        booksTableDescriptor.addFamily(new HColumnDescriptor("bookData"));
        ratingsTableDescriptor.addFamily(new HColumnDescriptor("ratingData"));
        usersTableDescriptor.addFamily(new HColumnDescriptor("userData"));

        admin.createTable(booksTableDescriptor);
        admin.createTable(ratingsTableDescriptor);
        admin.createTable(usersTableDescriptor);

        // Get a handle to the tables and read data into them from files
        HTable booksTable = new HTable(conf, "books");
        HTable ratingsTable = new HTable(conf, "ratings");
        HTable usersTable = new HTable(conf, "users");

        String path = new File("BX-Books.csv").getAbsolutePath();
        File file = new File(path);
        BufferedReader bufferReader = new BufferedReader(new FileReader(file));
        String line;
        line = bufferReader.readLine(); // Do not include the first line
        while ((line = bufferReader.readLine()) != null)   {
            String[] token= line.split(";");
            Put book = new Put(Bytes.toBytes(token[0]));
            book.add(Bytes.toBytes("bookData"), Bytes.toBytes("title"), Bytes.toBytes(token[1]));
            book.add(Bytes.toBytes("bookData"), Bytes.toBytes("author"), Bytes.toBytes(token[2]));
            book.add(Bytes.toBytes("bookData"), Bytes.toBytes("publicationYear"), Bytes.toBytes(token[3]));
            book.add(Bytes.toBytes("bookData"), Bytes.toBytes("publisher"), Bytes.toBytes(token[4]));
            book.add(Bytes.toBytes("bookData"), Bytes.toBytes("image-url-s"), Bytes.toBytes(token[5]));
            book.add(Bytes.toBytes("bookData"), Bytes.toBytes("image-url-m"), Bytes.toBytes(token[6]));
            book.add(Bytes.toBytes("bookData"), Bytes.toBytes("image-url-l"), Bytes.toBytes(token[7]));
            booksTable.put(book);
        }
        bufferReader.close();
        booksTable.flushCommits();
        booksTable.close();

        path = new File("BX-Users.csv").getAbsolutePath();
        file = new File(path);
        bufferReader = new BufferedReader(new FileReader(file));
        bufferReader.readLine();
        while ((line = bufferReader.readLine()) != null)   {
            String[] token= line.split(";");
            Put user = new Put(Bytes.toBytes(token[0]));
            user.add(Bytes.toBytes("userData"), Bytes.toBytes("location"), Bytes.toBytes(token[1]));
            user.add(Bytes.toBytes("userData"), Bytes.toBytes("age"), Bytes.toBytes(token[2]));
            usersTable.put(user);
        }
        bufferReader.close();
        usersTable.flushCommits();
        usersTable.close();

        path = new File("BX-Book-Ratings.csv").getAbsolutePath();
        file = new File(path);
        bufferReader = new BufferedReader(new FileReader(file));
        bufferReader.readLine();
        while ((line = bufferReader.readLine()) != null)   {
            String[] token= line.split(";");
            Put rating = new Put(Bytes.toBytes(token[0]+" "+token[1]));
            rating.add(Bytes.toBytes("ratingData"), Bytes.toBytes("userId"), Bytes.toBytes(token[0]));
            rating.add(Bytes.toBytes("ratingData"), Bytes.toBytes("bookId"), Bytes.toBytes(token[1]));
            rating.add(Bytes.toBytes("ratingData"), Bytes.toBytes("rating"), Bytes.toBytes(token[2]));
            ratingsTable.put(rating);
        }
        bufferReader.close();
        ratingsTable.flushCommits();
        ratingsTable.close();
    }
}
