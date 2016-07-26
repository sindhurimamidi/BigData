
object arrests extends App {
  val lines = sc.textFile("file:///Users/Sattya/Documents/Sattya_MS/Big_Data/Project/Datasets/crimes.csv").cache()
  val pairs = lines.map(x => (x.split(",")(17) + x.split(",")(8), 1))
  val counts = pairs.reduceByKey((a, b) => a + b)
  counts.coalesce(1).saveAsTextFile("file:///Users/Sattya/Documents/Sattya_MS/Big_Data/Project/Spark/spark_mapreduce3")
}