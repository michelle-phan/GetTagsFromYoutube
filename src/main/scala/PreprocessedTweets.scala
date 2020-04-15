import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object PreprocessedTweets {
  
  def mainPreprocessedTweets(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: Please enter yesterday time: yyyy mm dd")
      System.exit(1)
    }
    val appName = "PreprocessingTweets"
    val conf = new SparkConf()
    conf.setAppName(appName).setMaster("local[3]")
    val spark = SparkSession.builder().appName(appName).config(conf).getOrCreate()

    val year = args(0);
    val month = args(1);
    val day = args(2);
    import spark.implicits._
    val jsonDF = spark.read.format("json").load(s"hdfs://localhost:9000/datalake/raw/twitter/year=$year/month=$month/day=$day")
//    val tmpDF = spark.read.load(s"hdfs://localhost:9000/datalake/preprocessed/twitter/year=2020/month=4/day=11/tweetsWithSentiment.parquet")
//    tmpDF.select("text").write.save("namesAndFavColors.parquet")
    val jsonDF1_1 = jsonDF.select($"created_at", $"text", $"lang", explode($"extended_tweet.entities.hashtags") as "tweet_flat")
    val jsonDF1_2 = jsonDF1_1.select($"created_at", $"text", $"lang", $"tweet_flat.text" as "hashtags")
    jsonDF1_2.write.format("parquet").save(s"hdfs://localhost:9000/datalake/preprocessed/twitter/year=$year/month=$month/day=$day/tweets.parquet")
  }
}