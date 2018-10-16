import java.io.File
import java.util.Properties

import Config.{getClass, _}
import com.typesafe.scalalogging.Logger
import net.iharder.Base64.InputStream
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.io.Source



object TcpDsBenchmark {

  val cores = Runtime.getRuntime.availableProcessors
  val logger = Logger(LoggerFactory.getLogger(TcpDsBenchmark.getClass))

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("DB Warehouse Project SQL").master("local").getOrCreate
    LogManager.getLogger("org").setLevel(Level.ERROR)

    logger.info("Attempting to Start Apache Spark")
    val mysqlConnProperties = new Properties()
    mysqlConnProperties.setProperty("user", USERNAME)
    mysqlConnProperties.setProperty("password", PASSWORD)

    //We use partitioning only for big tables
    //if we are using item key as partitiong key, then the Upper bound of 1 million means that we are assuming that there are million items.
    //TODO : The upper bound should be dynamic (i.e should vary with data set size)
    val callCenterTable = spark.read.jdbc(URL, CALL_CENTER,  mysqlConnProperties)
    val catalogPageTable = spark.read.jdbc(URL, CATALOG_PAGE, mysqlConnProperties)
    val catalogReturnsTable = spark.read.jdbc(URL, CATALOG_RETURNS, mysqlConnProperties)
    val catalogSalesTable = spark.read.jdbc(URL, CATALOG_SALES, CATALOG_SALES_PARTITIONING_KEY, 10000, 1000000, cores * 3 , mysqlConnProperties)
    val customerTable = spark.read.jdbc(URL, CUSTOMER, mysqlConnProperties)
    val dateTimTable = spark.read.jdbc(URL, DATE_DIM, mysqlConnProperties)
    val customAddressTable = spark.read.jdbc(URL, CUSTOMER_ADDRESS, mysqlConnProperties)
    val customerDemographicsTable = spark.read.jdbc(URL, CUSTOMER_DEMOGRAPHICS, CUSTOMER_DEMOGRAPHICS_PARTITIONING_KEY, 10000, 100000, cores, mysqlConnProperties)
    val dbGenVersionTable = spark.read.jdbc(URL, DBGEN_VERSION, mysqlConnProperties)
    val houseHoldDemographicsTable = spark.read.jdbc(URL, HOUSEHOLD_DEMOGRAPHICS, mysqlConnProperties)
    val incomeBandTable = spark.read.jdbc(URL, INCOME_BAND, mysqlConnProperties)
    val inventoryTable = spark.read.jdbc(URL, INVENTORY,INVENTORY_PARTITIONING_KEY, 30000, 100000, cores * 3, mysqlConnProperties)
    val itemTable = spark.read.jdbc(URL, ITEM, mysqlConnProperties)
    val promotionTable = spark.read.jdbc(URL, PROMOTION, mysqlConnProperties)
    val reasonsTable = spark.read.jdbc(URL, REASON, mysqlConnProperties)
    val shipModeTable = spark.read.jdbc(URL, SHIP_MODE, mysqlConnProperties)
    val storeTable = spark.read.jdbc(URL, STORE, mysqlConnProperties)
    val storeReturnsTable = spark.read.jdbc(URL, STORE_RETURNS, mysqlConnProperties)
    val storesSalesTable = spark.read.jdbc(URL, STORE_SALES, STORES_SALES_PARTITIONING_KEY,  10000, 1000000, cores * 3 , mysqlConnProperties)
    val timeDimTable = spark.read.jdbc(URL, TIME_DIM, mysqlConnProperties)
    val webPageTable = spark.read.jdbc(URL, WEB_PAGE, mysqlConnProperties)
    val webReturnsTable = spark.read.jdbc(URL, WEB_RETURNS, mysqlConnProperties)
    val warehouseTable = spark.read.jdbc(URL, WAREHOUSE, mysqlConnProperties)
    val webSalesTable = spark.read.jdbc(URL, WEB_SALES, mysqlConnProperties)
    val webSiteTable = spark.read.jdbc(URL, WEB_SITE, mysqlConnProperties)

    callCenterTable.createOrReplaceTempView(CALL_CENTER)
    catalogPageTable.createOrReplaceTempView(CATALOG_PAGE)
    catalogReturnsTable.createOrReplaceTempView(CATALOG_RETURNS)
    catalogSalesTable.createOrReplaceTempView(CATALOG_SALES)
    customerTable.createOrReplaceTempView(CUSTOMER)
    customAddressTable.createOrReplaceTempView(CUSTOMER_ADDRESS)
    customerDemographicsTable.createOrReplaceTempView(CUSTOMER_DEMOGRAPHICS)
    dateTimTable.createOrReplaceTempView(DATE_DIM)
    dbGenVersionTable.createOrReplaceTempView(DBGEN_VERSION)
    houseHoldDemographicsTable.createOrReplaceTempView(HOUSEHOLD_DEMOGRAPHICS)
    incomeBandTable.createOrReplaceTempView(INCOME_BAND)
    inventoryTable.createOrReplaceTempView(INVENTORY)
    itemTable.createOrReplaceTempView(ITEM)
    promotionTable.createOrReplaceTempView(PROMOTION)
    reasonsTable.createOrReplaceTempView(REASON)
    shipModeTable.createOrReplaceTempView(SHIP_MODE)
    storeTable.createOrReplaceTempView(STORE)
    storeReturnsTable.createOrReplaceTempView(STORE_RETURNS)
    storesSalesTable.createOrReplaceTempView(STORE_SALES)
    timeDimTable.createOrReplaceTempView(TIME_DIM)
    webPageTable.createOrReplaceTempView(WEB_PAGE)
    webReturnsTable.createOrReplaceTempView(WEB_RETURNS)
    warehouseTable.createOrReplaceTempView(WAREHOUSE)
    webSalesTable.createOrReplaceTempView(WEB_SALES)
    webSiteTable.createOrReplaceTempView(WEB_SITE)

    var start = System.currentTimeMillis()
    val sparkSqlContext = spark.sqlContext

    val sqlQueries = getQueries

    for((sqlQuery, index) <- sqlQueries.zipWithIndex) {
      val queryNo = index + 40
      var start = System.currentTimeMillis()
      logger.info ("Executing Query {}", queryNo)
      val query = sparkSqlContext.sql(sqlQuery)
      val stop = System.currentTimeMillis()
      logger.info ("Time taken to execute query {} is {}", queryNo, stop-start )
//      query.show()
    }


    logger.info("Stopping Apache Spark")
    spark.stop()
  }

  val EMPTY : String = ""
  private def getQueries: Seq[String] = {
    val queriesDirectory = new File(getClass.getResource("/queries").getFile)

    queriesDirectory.listFiles()
          .map( file => Source.fromFile(file).getLines)
          .map(lines => {
            val stringBuilder = new StringBuilder
            for(line <- lines) {
              stringBuilder.append(line)
              stringBuilder.append("\n")
            }
            stringBuilder.toString
          })
          .map(query => if (query.contains(";"))  query.replaceAll(";", EMPTY) else query)
          .toSeq
  }


}
