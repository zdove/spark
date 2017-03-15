package org.apache.spark

/**
  * Created by zdove on 2017/3/14.
  */
import scala.collection.mutable

 class StageExInfo (val stageId: Int,
                    val alreadyPerRddSet: Set[Int], // prs
                  val afterPerRddSet: Set[Int], // aprs
                  val depMap: mutable.HashMap[Int, Set[Int]],
                  val curRunningRddMap: mutable.HashMap[Int, Set[Int]]) {

  }
