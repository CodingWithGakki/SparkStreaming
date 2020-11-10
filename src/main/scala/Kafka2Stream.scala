import java.lang
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Kafka2Stream {

  val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Kafka2StreamDeno")
  val ssc = new StreamingContext(sparkConf, Seconds(10))
  val spark: SparkSession = SparkSession.builder().master("local[4]").appName("Kafka2Stream").getOrCreate()

  //程序入口
  def main(args: Array[String]): Unit = {
    //设置话题组
    val topics: Array[String] = Array("cxhcip")
    //设置检查点，使用updateStateByKey必须要设检查点
    ssc.checkpoint("checkpoint")
    //kafka参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "os01:9092,os02:9092,os03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: lang.Boolean)
    )
    //通过KafkaUtils来消费kafka信息
    val kafkaData: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //输出时，只能数据 10s内的数据，之前的数据无法显示，所以我们要用`updateStateByKey`算子，保留之前的数据

    //列定义：行健，用户名，年龄，性别，商品ID，价格，门店ID，购物行为，电话，邮箱，购买日期
    //211604040371,Kwsgz,46,woman,580016,776.51,313019,pv,15552086341,oUiChfKAqr@163.com,2019-08-05
    //输出结果(230121,2019-08-06,852.56)

    //按天统计购物行为是buy的商品总销售额
    val tupleDate: DStream[(String, Double)] = kafkaData.map(x => {
      val arr: Array[String] = x.value().split(",")
      (arr(7) + "," + arr(10), arr(5).toDouble)
    }).filter(_._1.split(",")(0) == "buy")
    //用updateStateByKey显示之前的数据需要设置检查点

    // <1> 商品总的销售额（按天统计）数据输出, 根据需求发现，我们需要计算相同商品的销售额，所以需要将商品id作为key值进行累计
    //按照日期和购物行为 把所有商品价格进行相加
    val salesData: DStream[(String, Double)] = tupleDate.updateStateByKey[Double]((current: Seq[Double], pro: Option[Double]) => {
      val currents: Double = current.sum
      val pros: Double = pro.getOrElse(0)
      Some(currents + pros)
    })

    //测试从kafka中消费所需数据
    //salesData.print(100)

    //写入数据库
    Function.writeJdbc(salesData, "goods_amount_count")

    //StreamingContext启动
    ssc.start()
    ssc.awaitTermination()
  }

}

//读写数据库操作
object Function {
  def readJDBC(tablename: String): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("JDBCRDD").master("local[*]")
      .getOrCreate()
    val props = new Properties()
    props.setProperty("driver", "com.mysql.jdbc.Driver")
    props.setProperty("user", "root")
    props.setProperty("password", "1234")
    spark.read.jdbc("jdbc:mysql://os01:3306/spark", tablename, props)
  }

  def writeJdbc(dstream: DStream[(String, Double)], tablename: String): Unit = {
    dstream.foreachRDD(x => x.foreachPartition(line => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      val conn: Connection = DriverManager.getConnection("jdbc:mysql://os01:3306/spark", "root", "1234")
      //每次需要先清空表中数据，否则会出现重复数据
      val statement1: PreparedStatement = conn.prepareStatement("truncate goods_amount_count")
      statement1.execute()
      for (i <- line) {
        val statement: PreparedStatement = conn.prepareStatement("insert into goods_amount_count values (?,?)")
        statement.setFloat(1, i._2.toFloat)
        statement.setString(2, i._1.split(",")(1))
        statement.executeUpdate()
      }
    }))
  }

}
