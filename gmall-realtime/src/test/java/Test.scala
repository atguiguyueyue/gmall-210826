import com.alibaba.fastjson.JSON

object Test {
  def main(args: Array[String]): Unit = {
    val order: Order = new Order("101","111")
//    JSON.toJSONString(order)
  }

  case class Order(id:String,
                         order_id: String)

}
