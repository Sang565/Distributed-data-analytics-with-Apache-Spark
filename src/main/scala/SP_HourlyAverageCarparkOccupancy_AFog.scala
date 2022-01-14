/**
 * ======================================================================================
 * - Functions:
 *  This Spark-based code is run at the A-Fog tier to perform large-scale parking occupancy analytics (carpark-based hourly average occupancy) based on the long-term parking data stored in its HDFS.
 * 
 * - Technologies: Spark core, Hadoop HDFS
 * =======================================================================================
 * - Two arguments:
 *    + Master node 
 *    + Storage location 
 */

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//import org.apache.log4j._
import org.apache.spark.sql.streaming.Trigger //SANG: 23/10

object SP_HourlyAverageCarparkOccupancy_AFog {

  def main(args: Array[String]): Unit = {

    //--- Do comment these lines below if run with the local mode
    if (args.length < 2) {
      println("Usage: SP_HourlyAverageCarparkOccupancy_AFog <master_node> e.g. \"spark://lpc02-master:7077\", <hdfs-path> e.g. \"/5th-paper/dataset-1\"")
      System.exit(1)
    }

    //========================================================
    //=== Step 1: Create a Spark session and variables =======
    val sparkMaster = args(0)
    //val sparkMaster = "local[*]"    //--- for local running      
    //val sparkMaster = "spark://afog-master:7077"  //--- fixed the master node

    //--- Storage path ------------------
    val storageDirectory = args(1)
    //val storageDirectory = "./datasets/dataset-1"  //--- for local running    
    //val storageDirectory = "/5th-paper/dataset-1"

  val spark = SparkSession
    .builder
    .appName("SP_HourlyAverageCarparkOccupancy_AFog")
    //.config("spark.sql.warehouse.dir", "/spark-warehouse")
    .master(sparkMaster)
    .getOrCreate()

    import spark.implicits._
    val rootLogger = Logger.getRootLogger().setLevel(Level.ERROR)   //only display ERROR, not display WARN

    //=============================================================================
    //--- 1.2. read parking records (static) from file sink directory ----------------
    val schemaDefined = new StructType()
      .add("processing-time", StringType)
      .add("carparkID", StringType)
      .add("slotOccupancy", StringType)

    println("=== read files from storage place ====")

    var multiCarparkOccupancyRecord = spark
      .read
      .schema(schemaDefined)
      .json(storageDirectory)
      .select("processing-time", "carparkID", "slotOccupancy")

    multiCarparkOccupancyRecord.printSchema()
    //multiCarparkOccupancyRecord.show(false)

    /*
    val multiCarparkOccupancyRecordsCount = multiCarparkOccupancyRecord.count()
    println("Total no. of records:" + multiCarparkOccupancyRecordsCount)*/

    val multiCarparkOccupancyData = multiCarparkOccupancyRecord
      .withColumn("event_time", $"processing-time".cast(TimestampType))	
      .withColumn("carparkID", $"carparkID".cast(StringType))
      .withColumn("slotOccupancy", $"slotOccupancy".cast(IntegerType))

    val  multiCarparkOccupancyData_2 = multiCarparkOccupancyData
      .selectExpr("carparkID", "slotOccupancy", "event_time")
      .groupBy(col("carparkID"), window($"event_time", "1 hour", "1 hour"))
      .agg(round(avg("slotOccupancy"), 0)) //rounded the output
      .withColumn("Average_CP_Occupancy", $"round(avg(slotOccupancy), 0)".cast(IntegerType))
      .withColumn("Time", $"window.end")
      .select("carparkID", "Time", "Average_CP_Occupancy")

      //=== sort by window --> testing ====
      .sort(desc("Time"), desc("carparkID"))

    multiCarparkOccupancyData_2.show(30)

    spark.stop()
  }
}
