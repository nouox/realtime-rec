
import com.fdx.rec.processing.history.ContentBased
import com.fdx.rec.utils.MyConstants
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec


class MyTest extends AnyFlatSpec{
  val master = "local"
  private val sc = SparkSession.builder().master(master).appName("test").getOrCreate()

  // 读取log.csv
  private val logDF = sc.read.format("csv")
    .option("sep", ",")
    .option("header", "false")
    .schema(MyConstants.logSchema)
    .load("src/main/resources/log1.csv")

  // 读取product.csv
  private val prdDF = sc.read.format("csv")
    .option("sep", ",")
    .option("header", "false")
    .schema(MyConstants.productSchema)
    .load("src/main/resources/product1.csv")

  ContentBased.process(sc, logDF, prdDF)

  if (sc != null) {
    sc.stop()
  }

}