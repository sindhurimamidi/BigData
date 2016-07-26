package com.company;

import java.awt.*;
import java.awt.geom.Ellipse2D;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.Cluster;

import javax.imageio.ImageIO;


public class MahoutClustering {


    public static void main(String args[]) throws IOException{
        //Get Hadoop Conf since we will run a MapReduce Job.
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        //Set IO Path
        String inpFile = "data_input.csv";
        String outFile = "clustering_seq/";
        Path inFileDir = new Path(inpFile);
        Path outFileDir = new Path(outFile);
        if (!fileSystem.exists(inFileDir)) {
            System.out.println("Input file not found");
            return;
        }
        if (!fileSystem.isFile(inFileDir)) {
            System.out.println("Input should be a file");
        }

        if (fileSystem.exists(outFileDir)) {
            System.out.println("Output already exists");
            fileSystem.delete(outFileDir, true);
            System.out.println("deleted output directory");
        }
        //Read the file and its lines.
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(fileSystem.open(inFileDir)));
        String line = bufferedReader.readLine();
        //Set Output Directory
        SequenceFile.Writer writer = new SequenceFile.Writer(fileSystem, conf,
                outFileDir, LongWritable.class, VectorWritable.class);
        int counter = 0;
        int number_of_col=0;

        // Convert line to Double Array.
        line = bufferedReader.readLine();
        while (line != null) {
            String[] columnDetail = line.split(",", -1);
            double[] d = new double[columnDetail.length-1];
            number_of_col=columnDetail.length-1;
            for (int i = 0; i < number_of_col; i++) {
                try {
                    d[i] = Double.parseDouble(columnDetail[i+1]);

                } catch (Exception e) {
                    d[i] = 0;
                }
            }
            // Convert double array into Vector
            String name = "Community "+columnDetail[0];
            Vector vec = new RandomAccessSparseVector(number_of_col);
            NamedVector namedVec = new NamedVector(vec, name);

            namedVec.assign(d);

            //Write it into output
            VectorWritable writable = new VectorWritable();
            writable.set(namedVec);
            writer.append(new LongWritable(counter++), writable);
            line = bufferedReader.readLine();
        }
        writer.close();

        // Specify Paramter for Kmeans
        boolean runSequential = false;
        EuclideanDistanceMeasure measure = new EuclideanDistanceMeasure();

        System.out.println("Number of lines written=" + counter);
        Path outputPath = new Path("clustering_output");

        if (fileSystem.exists(outputPath)) {
            System.out.println("Output already exists");
            fileSystem.delete(outputPath, true);
            System.out.println("deleted output directory");
        }
        //Path where we will have initial cluster and its writer
        Path cluster_init_path = new Path("clustering_initial/part-00000");
        SequenceFile.Writer writerClusterInitial = new SequenceFile.Writer(fileSystem, conf, cluster_init_path, Text.class, Kluster.class);
        // Write initial centroids into file
        for (int i = 0; i < 5; i++) {
            double[] array=new double[number_of_col];
            Arrays.fill(array,i+1);
            Vector vec= new SequentialAccessSparseVector(number_of_col);
            vec.assign(array);
            Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
            writerClusterInitial.append(new Text(cluster.getIdentifier()), cluster);
        }
        writerClusterInitial.close();
        //Specify where final output will be
        Path kmeans_output=new Path("clustering_output");
        if (fileSystem.exists(kmeans_output)) {
            System.out.println("Output already exists");
            fileSystem.delete(kmeans_output, true);
            System.out.println("deleted output directory");
        }
        // Run K Means
        try {
            KMeansDriver.run(conf,
                    outFileDir,
                    cluster_init_path,
                    kmeans_output,
                    0.001,
                    50,
                    true,
                    0,
                    false);

            System.out.println("Kmeans completed");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IndexOutOfBoundsException e){
            System.out.println("IndexOutOfBoundsException while runnig Kmeans");
            e.printStackTrace();

        }
		System.out.println("Reading output clustering file");
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem,
                new Path("clustering_output/clusteredPoints" /*+ Cluster.CLUSTERED_POINTS_DIR*/ + "/part-m-00000"), conf);

        IntWritable key = new IntWritable();
        WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
        while (reader.next(key, value)) {
            NamedVector vector = (NamedVector) value.getVector();
            System.out.println(vector.getName() + " (" + value.toString() + ") belongs to cluster " + key.toString());
         //   System.out.println(vector.getName() + " belongs to cluster " + key.toString());
        }
        reader.close();

        //Details about each cluster
        reader = new SequenceFile.Reader(fileSystem,
                new Path("clustering_output/clusters-22-final/part-r-00000"), conf);

        IntWritable key2 = new IntWritable();
        ClusterWritable value2 = new ClusterWritable();

        ArrayList<Cluster> clusterList = new ArrayList<Cluster>();

        while (reader.next(key2, value2)) {
            System.out.println(value2.getValue()+ " ------------ "+ key2.toString());
            Cluster cluster = value2.getValue();
            clusterList.add(cluster);
            //plotEllipse(g2, cluster.getCenter(), cluster.getRadius().times(3));
        }
        int clusterCnt = 0;

        reader.close();

    }



}
