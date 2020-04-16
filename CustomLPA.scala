import org.apache.spark.graphx._

import scala.reflect.ClassTag

object CustomLPA {

    var treshold = 0.0
    var direction = EdgeDirection.Both
    def sendMessage[VD,ED:ClassTag](e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Map[VertexId, Long])] = {
      if (direction == EdgeDirection.Both) {
        Iterator((e.dstId, Map(e.srcAttr -> 1L)), (e.srcId, Map(e.dstAttr -> 1L)))
      }
      else {
        Iterator((e.srcId, Map(e.dstAttr -> 1L)))
      }
    }
    def mergeMessage(inMessage: Map[VertexId, Long], outMessage: Map[VertexId, Long])
    : Map[VertexId, Long] = {
      (inMessage.keySet ++ outMessage.keySet).map(key => key -> {
        var res = 0L
        if (scala.util.Random.nextFloat() > treshold)
          res = (inMessage.getOrElse(key, 0L) + outMessage.getOrElse(key, 0L))
        res
      }).toMap

    }
    def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]): VertexId = {
      if (message.isEmpty) attr else message.maxBy(_._2)._1
    }

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED], edgeDirection: EdgeDirection, tresh: Double, maxIter: Int): Graph[VertexId, ED] = {
      treshold = tresh
      direction = edgeDirection
      val lpaGraph = graph.mapVertices { case (vid, _) => vid }

      val initialMessage = Map[VertexId, Long]()
      Pregel(lpaGraph, initialMessage, maxIterations = maxIter)(
        vprog = vertexProgram,
        sendMsg = sendMessage,
        mergeMsg = mergeMessage
      )
  }
}
