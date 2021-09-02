import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions._

import java.io.Serializable
import java.nio.file.{Path, Paths}
//import spark.sqlContext.implicits._

// case class enable us to define stucture
case class AccessLog(ip: String,ident: String,user: String,datetime: String,request: String,status: String,size: String,referer: String,userAgent: String,unk: String)
case class DateConCount(date: String, count: BigInt)
case class IpAddressCount(date: String, ip: String, count: BigInt)
case class UriCount(date: String, request: String, count: BigInt)
//case class AccessLog(ip: String, ident: String, user: String, datetime: String, request: String, status: String, size: String, referer: String, userAgent: String, unk: String)


/**
object Logger {
  val uris = List("/", "/list", "/update", "/detail")
  def uri = uris(fabricator.Fabricator.alphaNumeric.randomInt(0, uris.length))
  def ip = fabricator.Fabricator.internet.ip
  def cookieId = fabricator.Fabricator.alphaNumeric.randomString("0123456789abcdef", 10)
  def generate(date: String, time: String)(): AccessLog = AccessLog(time, ip, uri, cookieId)
}
**/

object LogProcessor {

  def main(args: Array[String]): Unit = {
  //def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("Log Analysis")
      .getOrCreate()

    import spark.implicits._

    ReportTrigger(args, spark)

    spark.stop
  }

  //def exec() {
def ReportTrigger(args: Array[String], sparkSession: SparkSession): Unit = {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()

    import spark.implicits._

    import org.joda.time._
    //val startDate = new LocalDate("2019-01-01")

    val inSource = "/home/sabe/IdeaProjects/sabe/LogAnalysisXtra/LogProcessor1"
    val outTarget = "/home/sabe/IdeaProjects/sabe/LogAnalysisXtra/LogProcessor1/reports"

    createReport(inSource: String, outTarget: String)

    def createReport(gzPath: String, outputPath: String): Unit = {
      //extractFromGz(Paths.get(gzPath), Paths.get(Utils.AccessLogPath))

      val gzPath = "/home/sabe/IdeaProjects/sabe/LogAnalysisXtra/LogProcessor1"
      val logs = spark.read.text(gzPath)
      logs.count
      // should be 2660050
      //logs.take(20).foreach(println)
      //val outputPath = "/home/sabe/dp2/spark-hands-on/output"

      //convert a Row to a String:
      val logAsString = logs.map(_.getString(0))
      val stringExample = logAsString.take(2).drop(1).head

      // case class enable us to define stucture
      val l1 = AccessLog("ip", "ident", "user", "datetime", "request", "status", "size", "referer", "userAgent", "unk")

      // you can now access the field directly, with l1.ip for example
      AccessLog.apply _

      //We can use regex:
      //val R = """^(?<ip>[0-9.]+) (?<identd>[^ ]) (?<user>[^ ]) \[(?<datetime>[^\]]+)\] \"(?<request>[^\"]*)\" (?<status>[^ ]*)

      val R = """^(?<ip>[0-9.]+) (?<ident>[^ ]) (?<user>[^ ]) \[(?<datetime>[^\]]+)\] \"(?<request>[^\"]*)\" (?<status>[^ ]*) (?<size>[^ ]*) \"(?<referer>[^\"]*)\" \"(?<useragent>[^\"]*)\" \"(?<unk>[^\"]*)\"""".r
      val dsParsed = logAsString.flatMap(x => R.unapplySeq(x))
      dsParsed.count
      val parsed = R.unapplySeq(stringExample)
      def toAccessLog(params: List[String]) = AccessLog(params(0), params(1), params(2), params(3), params(4), params(5), params(6), params(7), params(8), params(9))

      //toAccessLog(parsed.get)

      parsed.map(toAccessLog)

      //convert the time string to a timestamp:
      //val ds: Dataset[AccessLog] = dsParsed.map(toAccessLog _)
      //val ds = dsParsed.map(toAccessLog _)
      //def run(spark: SparkSession, ...) {
       // [...]
        val ds: Dataset[AccessLog] = dsParsed.map(toAccessLog _)
        //[...]
     // }
      val dsWithTime = ds.withColumn("datetime", to_timestamp(ds("datetime"), "dd/MMM/yyyy:HH:mm:ss X"))
      dsWithTime.cache

      //use directly this data as SQL:
      dsWithTime.createOrReplaceTempView("AccessLog")

      //find all the dates having too big number of connection (> 20000)
      //spark.sql("select cast(datetime as date) as date, count(*) as count from AccessLog group by date having count > 20000 order by count desc").show(false)
      val dateConCountDateGrouped = spark.sql("select cast(datetime as date) as date, count(*) as count from AccessLog group by date having count > 20000 order by count desc")
        .as[DateConCount](Encoders.product[DateConCount])
      //dateConCountDateGrouped.show(false)

      retrieveDateConReport(dateConCountDateGrouped, s"${outTarget}/json_reportdates")

      //For each date, list of number of access by URI for each URI
      spark.sql("select cast(datetime as date) as date, request, count(*) as count from AccessLog group by date,request having count > 20000 order by date,request,count desc").show(false)

      //For each date, list of number of access per IP address for each IP address
      spark.sql("select cast(datetime as date) as date, ip, count(*) as count from AccessLog group by date,ip having count > 20000 order by date,ip,count desc").show(false)

    }
/**
  private def extractFromGz(sourceFile: Path, destFile: Path): Unit = {
    CustomFileUtils.decompressGzipNio(sourceFile, destFile)
  }
**/
    def retrieveDateConReport(dataset: Dataset[DateConCount], outputPath: String): Unit = {
      baseReportExtractor[DateConCount](dataset, "date", "dates", "requestCount", outputPath)
    }

    def retrieveUrisReport(dataset: Dataset[UriCount], outputPath: String): Unit = {
      baseReportExtractor[UriCount](dataset, "request", "requests", "requestCount", outputPath)
    }

    def retrieveIpReport(dataset: Dataset[IpAddressCount], outputPath: String): Unit = {
      baseReportExtractor[IpAddressCount](dataset, "ip", "ips", "ipCount", outputPath)
    }

    def baseReportExtractor[T](dataset: Dataset[T], colName: String, alias: String, updatedColName: String, outputPath: String): Unit = {
      //CustomFileUtils.deleteIfExists(Paths.get(outputPath))
      dataset.groupBy("date").agg(collect_list(col(colName))
        .as(alias), collect_list("count")
        .as("counts")).select(col("date"), map_from_arrays(col(alias), col("counts")))
        .withColumnRenamed(s"map_from_arrays(${alias}, counts)", updatedColName)
        .coalesce(1).write.format("json").save(outputPath)
    }

   // spark.stop()
  }
}

