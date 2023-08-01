import com.fdx.rec.processing.history.ContentBased
import com.fdx.rec.utils.MyConstants
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class MyTest extends AnyFlatSpec with BeforeAndAfter {
  val master = "local[*]"
  var sc: SparkSession = SparkSession.builder().master(master).appName("MyTest").getOrCreate()

  // 读取 log.csv
  private val logDF = sc.read.format("csv")
    .option("sep", ",")
    .option("header", "false")
    .schema(MyConstants.logSchema)
    .load("src/main/resources/log.csv")

  // 读取product.csv
  private val prdDF = sc.read.format("csv")
    .option("sep", ",")
    .option("header", "false")
    .schema(MyConstants.productSchema)
    .load("src/main/resources/product.csv")

  ContentBased.process(sc, logDF, prdDF)

  if (sc != null) {
    sc.stop()
  }

}