import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

object GetTags {

  def main(args: Array[String]) {
    val appName = "GetTags"
    val conf = new SparkConf()
    conf.setAppName(appName).setMaster("local[3]")
    val spark = SparkSession.builder().appName(appName).config(conf).getOrCreate()

    import spark.implicits._
    val df = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/datalake/preprocessed/youtube/top100Videos.csv")
    df.filter($"country" === "US" && $"tags".isNotNull).select("tags").repartition(1).write.format("com.databricks.spark.csv").option("header", "false").save("/opt/flume/data/keywordsTmp.csv")
    
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path("/opt/flume/data/keywordsTmp.csv"), hdfs, new Path("/opt/flume/data/keywords.csv"), false, hadoopConfig, null)

  }
}