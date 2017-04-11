package org.apache.spark.storage

/**
  * Created by zdove on 2017/3/14.
  */
class BlockExInfo (val blockId: RDDBlockId) extends Comparable[BlockExInfo] {

      var size: Long = 1024
    var creatStartTime: Long = 0
    var creatFinTime: Long = _
    var creatCost: Long = 1

      var serStartTime: Long = _
    var serFinTime: Long = _
    var serCost: Long = 0
    var serAndDeCost: Long = _

      var fakeSerCost: Long = 0

      var isExist: Int = 0
    // 0: not exist; 1: in-memory; 2: ser in disk
      var norCost: Double = _ // normalized cost

      var sonSet: Set[BlockId] = Set()
  var value: Int = 1
      // write the creatFinTime and cal the creatFinTime
      var kind: Double = 1.0
      // write the creatFinTime and cal the creatFinTime
    def writeFinAndCalCreatCost(finTime: Long) {
        creatFinTime = finTime
        creatCost = creatFinTime - creatStartTime
        // norCost = creatCost.toDouble * value / (size / 1024 / 1024)
      norCost = creatCost.toDouble * value / kind
        isExist = 1
      }

    def refreshnorCost(): Unit = {
      norCost = norCost / value * (value-1)
    }
      def writeAndCalSerCost(serStart: Long, serFin: Long): Unit = {
        serStartTime = serStart
        serFinTime = serFin
        serCost = serFinTime - serStartTime
        isExist = 2
      }

      def decidePolicy: Int = {
        if (creatCost < serAndDeCost) {
            norCost = creatCost.toDouble / size
            3 // creat Cost is low so just remove from memory
          } else {
            norCost = serAndDeCost.toDouble / size
            4 // ser and deser cost is low, so just ser to disk
          }
      }

      override def compareTo(o: BlockExInfo): Int = {
        this.norCost.compare(o.norCost)
      }
  }
