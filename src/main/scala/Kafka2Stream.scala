package Practice

import java.sql.{Date, DriverManager}
import java.util.Properties

import Practice.Kafka2Stream.{addShopPrice, readJdbc, writeJdbc}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Kafka2Stream {

  val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Kafka2StreamDeno")
  val ssc = new StreamingContext(sparkConf, Seconds(10))
  val spark: SparkSession = SparkSession.builder().master("local[4]").appName("readmysql").getOrCreate()


  def readJdbc(tablename: String): DataFrame = {

    val properties = new Properties()
    properties.setProperty("driver", "com.mysql.jdbc.Driver")
    properties.setProperty("user", "root")
    properties.setProperty("password", "1234")
    spark.read.format("jdbc").jdbc("jdbc:mysql://os01:3306/spark", tablename, properties)
  }

  def writeJdbc(dstream: DStream[(String, Int, String)], tablename: String) = {
    dstream.foreachRDD(x => x.foreachPartition(line => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      val conn = DriverManager.getConnection("jdbc:mysql://os01:3306/company", "root", "1234")
      //      获取需要的数据，并进行类型转换，写入对应表中
      for (i <- line) {
        val statement = conn.prepareStatement("insert into goods_amount_count values (?,?)")
        statement.setFloat(1, i._2.toFloat)
        statement.setDate(2, Date.valueOf(i._3))
        statement.executeUpdate()
      }
    }))

  }

  def main(args: Array[String]): Unit = {
    //检查点
    ssc.checkpoint("checkpoint")
    //话题组
    val topics: Array[String] = Array("cxhcip")
    //kafka参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "os01:9092,os02:9092,os03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    //kafkaDstream 即kafka消费到的数据
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //kafkaDstream.map(x=>x.value()).print(200)


    //商品总的销售额（按天统计）数据输出, 根据需求发现，我们需要计算相同商品的销售额，所以需要将商品id作为key值进行累计
    val dataRDD: DStream[(String, Int)] = kafkaDstream.map(x => {
      var arr: Array[String] = x.value().split(",")
      (arr(4) + "," + arr(arr.length - 1), arr(5).toDouble.toInt * 100)
    }).updateStateByKey[Int](addShopPrice(_, _))

    //调用方法，读取表中数据  将jdbc数据转为rdd的 k v 形式
    val jdbcRDD: RDD[(String, String)] = readJdbc("goods_info").rdd.map { x =>
      (x(0).toString, x(1).toString)
    }

    //将 jdbc数据和streaming数据进行 join关联
    val goodsRDD: DStream[(String, Int, String)] =
      dataRDD.transform(rdd => {
        //由于 updateStateByKey是 根据 商品id和天进行 计算的，所以下边join时，应该分开，只在key中放商品id
        rdd.map(x => {
          val strings: Array[String] = x._1.split(",")
          //key：商品id  value中是元祖(价格，天)
          (strings(0), (x._2, strings(1)))
        }
        ).leftOuterJoin(jdbcRDD)
      })
        .map(x => {
          //(230121,((74700,2019-08-06),Some(杭州百鸟朝凤)))  从此数据格式中获取商品名字，价格和时间
          (x._2._2.getOrElse("0"), x._2._1._1 / 100, x._2._1._2)
        })

    writeJdbc(goodsRDD, "goods_amount_count")

    ssc.start()
    ssc.awaitTermination()
  }

  def addShopPrice(currents: Seq[Int], historys: Option[Int]): Option[Int] = {
    val sum: Int = currents.sum
    val d: Int = historys.getOrElse(0)
    Some(sum + d)
  }
}
