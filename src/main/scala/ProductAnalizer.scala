import java.nio.file.Paths
import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


case class Amount(positionId: Long,
                  var amount: BigDecimal,
                  eventTime: Timestamp) {
  amount = amount.setScale(2)
}

case class Warehouse(positionId: Long,
                     warehouse: String,
                     product: String,
                     eventTime: Timestamp)

object ProductAnalizer {

  val sparkSession: SparkSession =
    SparkSession
      .builder()
      .appName("Product Analizer")
      .config("spark.master", "local")
      .getOrCreate()

  // For implicit conversions like converting DataFrames to DataSets
  import sparkSession.implicits._

  def main(args: Array[String]): Unit = {
    val amountsDS: Dataset[Amount] = getAmountsDF("amounts.csv").persist()
    val warehouseDS: Dataset[Warehouse] = getWarehouseDF("warehouses.csv").persist()

    val currentAmounts = amountsDS
      .groupBy(col("positionId"))
      .agg(max(col("eventTime"))
        .cast(TimestampType).as("eventTime")
      )
    val fullCurrentAmounts = amountsDS
      .join(currentAmounts, Seq("eventTime", "positionId"))

    warehouseDS
      .join(fullCurrentAmounts, "positionId")
      .drop("eventTime")
      .show()

    val aggregatedAmounts =
      amountsDS
        .groupBy("positionId")
        .agg(
          max(col("amount")),
          min(col("amount")),
          avg(col("amount"))
        )

    warehouseDS
      .join(aggregatedAmounts, "positionId")
      .drop("eventTime", "positionId")
      .show()
  }

  def getAmountsDF(resource: String): Dataset[Amount] =
    sparkSession.read
      .option("header", "true")
      .csv(fsPath(resource))
      .withColumn("positionId", col("positionId").cast(LongType))
      .withColumn("amount", col("amount").cast(DataTypes.createDecimalType(10, 2)))
      .withColumn("eventTime", from_unixtime(col("eventTime")))
      .as[Amount]

  def getWarehouseDF(resource: String): Dataset[Warehouse] =
    sparkSession.read
      .option("header", "true")
      .csv(fsPath(resource))
      .withColumn("positionId", col("positionId").cast(LongType))
      .withColumn("eventTime", from_unixtime(col("eventTime")))
      .as[Warehouse]

  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

}