package org.itc.stockmarket

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession, types}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import vegas._
import vegas.sparkExt._

object StockMarketAnalysis extends App{
  System.setProperty("hadoop.home.dir", "C:\\winutils")
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "Stock Analysis Project")
  //sparkconf.set("spark.master", "local[*]")
  val spark = SparkSession.builder().config(sparkconf).getOrCreate()
  val dfstockcompanies = spark.read.option("header", true).
    option("inferSchema", true).csv(args(0))
  val dfstockprice = spark.read.option("header", true).
    option("inferSchema", true).csv(args(1))
  val defaultcount = dfstockcompanies.count();
  val distinctcount = dfstockcompanies.dropDuplicates("Name")
  val newdata = dfstockcompanies.exceptAll(distinctcount).distinct()
  def isduplicate:Boolean = !newdata.isEmpty

  //create stockdata

  var stockcompanyView = dfstockcompanies.createTempView("stock_company")
  var stockpriceView = dfstockprice.createTempView("stock_price")
  //store stock data with trading year and month
  val query = "select trading_year, trading_month, sc.code as company_code,name as company_name,trim(SUBSTRING_INDEX(Headquarter,';',1)) as headquarters, sector, sub_industry,open as opening, close as closing, low, high, volume from stock_company sc " +
    "inner join (select code,EXTRACT(YEAR FROM to_date(trading_date,'dd/MM/yyyy')) as trading_year,  EXTRACT(MONTH FROM to_date(trading_date,'dd/MM/yyyy')) as trading_month,round(avg(open),2) open, round(avg(close),2) close, round(avg(low),2) low, round(avg(high),2) high,round(avg(volume),2) volume from stock_price group by code,trading_year,trading_month) sp on sc.code=sp.code"
  val dfstockdata = spark.sql(query)
  var stockdataView = dfstockdata.createTempView("stock_data")

  //store companies max min - trading year trading month
  val queryy = "select company_code,company_name, min(trading_year) min_year,max(trading_year) max_year, min(trading_month) min_month, max(trading_month) max_month from stock_data group by company_name,company_code"
  val dfstocktable1 = spark.sql(queryy)
  var stocktable1View = dfstocktable1.createTempView("stock_table1")

  //Stock Analysis
  //1. Top 5 companies that are good for investment
  val query1 = "select stock_start.company_code, round(((closing-opening)/opening)*100,2) growth_percent from " +
    "(select sd.company_code,t1.company_name, opening from stock_data sd inner join stock_table1 t1 on (t1.company_name = sd.company_name) " +
    "where sd.trading_year = t1.min_year and sd.trading_month = t1.min_month and sd.company_name = t1.company_name) stock_start " +
    "inner join (select sd.company_code,t1.company_name,closing from stock_data sd, stock_table1 t1 " +
    "where sd.trading_year = t1.max_year and sd.trading_month = t1.max_month and sd.company_name = t1.company_name) stock_end on (stock_end.company_code = stock_start.company_code)" +
    " where stock_start.company_name = stock_end.company_name order by growth_percent desc limit 10"
  val dfanalysis1 = spark.sql(query1)
  dfanalysis1.show()
  val singlePartitionDataFrame1 = dfanalysis1.coalesce(1)
  singlePartitionDataFrame1.write.mode("overwrite").format("csv").save(args(2))

  //2. Worst year & Best Year

  val query2 = "select headquarters, sub_industry, stock_start.company_name,round((((stock_end.closing - stock_start.opening)/stock_start.opening)*100),2) as growth_percent from (select t1.company_name,opening from stock_data sd, stock_table1 t1 where sd.trading_year=t1.min_year and sd.trading_month=t1.min_month and sd.company_name=t1.company_name) stock_start, (select t1.company_name, closing from stock_data sd, stock_table1 t1 where sd.trading_year=t1.max_year and sd.trading_month=t1.max_month and sd.company_name=t1.company_name) stock_end, (select company_name, headquarters, sub_industry from stock_data group by company_name,headquarters,sub_industry) sd where (stock_end.closing-stock_start.opening) > 0 and (stock_start.company_name = stock_end.company_name) and (sd.company_name=stock_start.company_name)"
  val dfstocktable2 = spark.sql(query2)
  var stocktable2View = dfstocktable2.createTempView("stock_table2")
  val query3 = "select open.sector, open.trading_year,round((close - open), 2) growth from(select sector, trading_year, round(avg(opening), 2) open from stock_data where trading_month = 1 group by sector, trading_year) open, (select sector, trading_year, round(avg(closing), 2) close from stock_data where trading_month = 12 group by sector, trading_year) close where open.sector = close.sector and open.trading_year = close.trading_year"
  val dfstocktable3 = spark.sql(query3)
  var stocktable3View = dfstocktable3.createTempView("stock_table3")
  val query4 = "select x.sector,x.trading_year,x.growth from stock_table3 x,(select sector,min(growth) growth from stock_table3 group by sector) y where x.sector=y.sector and x.growth=y.growth order by x.growth"
  val dfanalysis2 = spark.sql(query4)
  dfanalysis2.show()
  val singlePartitionDataFrame2 = dfanalysis2.coalesce(1)
  singlePartitionDataFrame2.write.mode("overwrite").format("csv").save(args(3))
  val query5 = "select a.sector,a.trading_year,a.growth from stock_table3 a, (select sector,max(growth) growth from stock_table3 group by sector) b where a.sector=b.sector and a.growth=b.growth order by a.growth desc"
  val dfanalysis3 = spark.sql(query5)
  dfanalysis3.show()
  val singlePartitionDataFrame3 = dfanalysis3.coalesce(1)
  singlePartitionDataFrame3.write.mode("overwrite").format("csv").save(args(4))
 // val plot = Vegas().withDataFrame(dfanalysis3).encodeX("trading_year + sector",Nominal).encodeY("growth",Quantitative).mark(Bar)
 // plot.show
  /* dfanalysis1.write
          .format("jdbc")
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .option("url", "jdbc:mysql://localhost:3306/stockdb")
          .option("dbtable", "stock_table1")
          .option("user", "root")
          .option("password", "123456")
          .save()
  dfanalysis3.write
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/stockdb")
      .option("dbtable", "stock_table3")
      .option("user", "root")
      .option("password", "123456")
      .save()
     dfanalysis2.write
        .format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", "jdbc:mysql://localhost:3306/stockdb")
        .option("dbtable", "stock_table2")
        .option("user", "root")
        .option("password", "123456")
        .save() */

}
