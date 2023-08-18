
import com.fdx.rec.processing.history.ContentBased
import com.fdx.rec.utils.MyConstants
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec


class MyTest extends AnyFlatSpec {
  val master = "local[*]"
  private val spark = SparkSession.builder().master(master).appName("test").getOrCreate()

  // 读取log.csv
  private val logDF = spark.read.format("csv")
    .option("sep", ",")
    .option("header", "false")
    .schema(MyConstants.logSchema)
    .load("src/main/resources/log1.csv")

  // 读取product.csv
  private val prdDF = spark.read.format("csv")
    .option("sep", ",")
    .option("header", "false")
    .schema(MyConstants.productSchema)
    .load("src/main/resources/product1.csv")

  ContentBased.setSparkSession(spark)
  ContentBased.process(logDF, prdDF)

  if (spark != null) {
    spark.stop()
  }

}