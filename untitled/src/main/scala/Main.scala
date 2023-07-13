import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip, ZipN, ZipWith, ZipWithN}
import akka.NotUsed


object Homework_Akka {
  implicit val system = ActorSystem("fusion")
  implicit val materializer = ActorMaterializer()

  val graph = GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
    import GraphDSL.Implicits._

    val input  = builder.add(Source(1 to 10))
    val x10    = builder.add(Flow[Int].map(_ * 10))
    val x2     = builder.add(Flow[Int].map(_ * 2))
    val x3     = builder.add(Flow[Int].map(_ * 3))
    val output = builder.add(Sink.foreach(println))
    val zip    = builder.add(ZipWith[Int, Int, Int, (Int, Int, Int)] { (a, b, c) => (a, b, c) })

    val broadcast = builder.add(Broadcast[Int](3))
    broadcast.out(0) ~> x10 ~> zip.in0
    broadcast.out(1) ~> x2 ~> zip.in1
    broadcast.out(2) ~> x3 ~> zip.in2

    input ~> broadcast
    zip.out ~> output

    ClosedShape
  }

  def main(args: Array[String]): Unit = {
    RunnableGraph.fromGraph(graph).run()
  }
}