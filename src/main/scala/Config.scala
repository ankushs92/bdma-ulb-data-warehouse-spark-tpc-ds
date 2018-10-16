import java.io.File

import TcpDsBenchmark.getClass

import scala.io.Source

object Config {

    //Global Config
    val USERNAME : String = "root"
    val PASSWORD : String = "root"
    val DB : String = "tpc-ds-db-warehouse-project"
    val URL : String = "jdbc:mysql://localhost:3306/" + DB

    //Table names
    val CALL_CENTER : String = "call_center"
    val CATALOG_PAGE : String = "catalog_page"
    val CATALOG_RETURNS : String = "catalog_returns"
    val CATALOG_SALES : String = "catalog_sales"
    val CUSTOMER : String = "customer"
    val CUSTOMER_ADDRESS : String = "customer_address"
    val CUSTOMER_DEMOGRAPHICS : String = "customer_demographics"
    val DATE_DIM : String = "date_dim"
    val DBGEN_VERSION : String = "dbgen_version"
    val HOUSEHOLD_DEMOGRAPHICS : String = "household_demographics"
    val INCOME_BAND : String = "income_band"
    val INVENTORY : String = "inventory"
    val ITEM : String = "item"
    val PROMOTION : String = "promotion"
    val REASON : String = "reason"
    val SHIP_MODE : String = "ship_mode"
    val STORE : String = "store"
    val STORE_RETURNS : String = "store_returns"
    val STORE_SALES : String = "store_sales"
    val TIME_DIM : String = "time_dim"
    val WAREHOUSE : String = "warehouse"
    val WEB_PAGE : String = "web_page"
    val WEB_RETURNS : String = "web_returns"
    val WEB_SALES : String = "web_sales"
    val WEB_SITE : String = "web_site"

    //Partitioning Keys
    val CATALOG_SALES_PARTITIONING_KEY = "cs_item_sk"
    val CUSTOMER_DEMOGRAPHICS_PARTITIONING_KEY = "cd_demo_sk"
    val INVENTORY_PARTITIONING_KEY = "inv_item_sk"
    val STORES_SALES_PARTITIONING_KEY = "ss_item_sk"

    def main(args: Array[String]): Unit = {
        val list = List(1,2,3)

        for((value, index) <- list.zipWithIndex) {
            println(value)
            println(index)
        }
        val s = "== SQL ==\n-- start query 1 in stream 0 using template query1.tpl and seed QUALIFICATIONwith customer_total_return as(select sr_customer_sk as ctr_customer_sk,sr_store_sk as ctr_store_sk,sum(SR_RETURN_AMT) as ctr_total_returnfrom store_returns,date_dimwhere sr_returned_date_sk = d_date_skand d_year =2000group by sr_customer_sk,sr_store_sk) select  c_customer_idfrom customer_total_return ctr1,store,customerwhere ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2from customer_total_return ctr2where ctr1.ctr_store_sk = ctr2.ctr_store_sk)and s_store_sk = ctr1.ctr_store_skand s_state = 'TN'and ctr1.ctr_customer_sk = c_customer_skorder by c_customer_id limit 100"
        //    val query1Results = sparkSqlContext.sql(
        //      """
        //        |with customer_total_return as
        //        |(select sr_customer_sk as ctr_customer_sk
        //        |,sr_store_sk as ctr_store_sk
        //        |,sum(SR_RETURN_AMT) as ctr_total_return
        //        |from store_returns
        //        |,date_dim
        //        |where sr_returned_date_sk = d_date_sk
        //        |and d_year =2000
        //        |group by sr_customer_sk
        //        |,sr_store_sk)
        //        | select  c_customer_id
        //        |from customer_total_return ctr1
        //        |,store
        //        |,customer
        //        |where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
        //        |from customer_total_return ctr2
        //        |where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
        //        |and s_store_sk = ctr1.ctr_store_sk
        //        |and s_state = 'TN'
        //        |and ctr1.ctr_customer_sk = c_customer_sk
        //        |order by c_customer_id limit 100
        //        |"""
        //        .stripMargin)
        //
        //    query1Results.show(100000)
        //    var stop = System.currentTimeMillis()
        //
        //    val query2Results = sparkSqlContext.sql(
        //      """
        //        |-- start query 2 in stream 0 using template query2.tpl and seed QUALIFICATION
        //        | with wscs as
        //        | (select sold_date_sk
        //        |        ,sales_price
        //        |  from  (select ws_sold_date_sk sold_date_sk
        //        |              ,ws_ext_sales_price sales_price
        //        |        from web_sales
        //        |        union all
        //        |        select cs_sold_date_sk sold_date_sk
        //        |              ,cs_ext_sales_price sales_price
        //        |        from catalog_sales) x ),
        //        | wswscs as
        //        | (select d_week_seq,
        //        |        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        //        |        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        //        |        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
        //        |        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        //        |        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        //        |        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        //        |        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
        //        | from wscs
        //        |     ,date_dim
        //        | where d_date_sk = sold_date_sk
        //        | group by d_week_seq)
        //        | select d_week_seq1
        //        |       ,round(sun_sales1/sun_sales2,2)
        //        |       ,round(mon_sales1/mon_sales2,2)
        //        |       ,round(tue_sales1/tue_sales2,2)
        //        |       ,round(wed_sales1/wed_sales2,2)
        //        |       ,round(thu_sales1/thu_sales2,2)
        //        |       ,round(fri_sales1/fri_sales2,2)
        //        |       ,round(sat_sales1/sat_sales2,2)
        //        | from
        //        | (select wswscs.d_week_seq d_week_seq1
        //        |        ,sun_sales sun_sales1
        //        |        ,mon_sales mon_sales1
        //        |        ,tue_sales tue_sales1
        //        |        ,wed_sales wed_sales1
        //        |        ,thu_sales thu_sales1
        //        |        ,fri_sales fri_sales1
        //        |        ,sat_sales sat_sales1
        //        |  from wswscs,date_dim
        //        |  where date_dim.d_week_seq = wswscs.d_week_seq and
        //        |        d_year = 2001) y,
        //        | (select wswscs.d_week_seq d_week_seq2
        //        |        ,sun_sales sun_sales2
        //        |        ,mon_sales mon_sales2
        //        |        ,tue_sales tue_sales2
        //        |        ,wed_sales wed_sales2
        //        |        ,thu_sales thu_sales2
        //        |        ,fri_sales fri_sales2
        //        |        ,sat_sales sat_sales2
        //        |  from wswscs
        //        |      ,date_dim
        //        |  where date_dim.d_week_seq = wswscs.d_week_seq and
        //        |        d_year = 2001+1) z
        //        | where d_week_seq1=d_week_seq2-53
        //        | order by d_week_seq1
        //        |
        //      """.stripMargin
        //    )
        //
        //    query2Results.show(1000000)
        //    logger.info("Time Taken to execute query1 is {}", stop - start)

    }


}
