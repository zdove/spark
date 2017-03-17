def getOrCompute[T](
      rdd: RDD[T],
      partition: Partition,
      context: TaskContext,
      storageLevel: StorageLevel): Iterator[T] = {

    val key = RDDBlockId(rdd.id, partition.index)
    logDebug(s"Looking for partition $key")
    blockManager.get(key) match {
      case Some(blockResult) =>
        // Partition is already materialized, so just return its values
        val existingMetrics = context.taskMetrics
          .getInputMetricsForReadMethod(blockResult.readMethod)
        existingMetrics.incBytesRead(blockResult.bytes)

        val iter = blockResult.data.asInstanceOf[Iterator[T]]
        new InterruptibleIterator[T](context, iter) {
          override def next(): T = {
            existingMetrics.incRecordsRead(1)
            delegate.next()
          }
        }
      case None =>
      //  val blockManager = SparkEnv.get.blockManager
        if (!blockManager.blockExInfo.containsKey(key)) {
          blockManager.blockExInfo.put(key, new BlockExInfo(key))
        }
        blockManager.stageExInfos.get(blockManager.currentStage) match {
          case Some(curStageExInfo) =>
            var parExist = true
            if(curStageExInfo.depMap.contains(rdd.id) && curStageExInfo.depMap(rdd.id).size!= 0 ){
            for (par <- curStageExInfo.depMap(rdd.id)) {
              val parBlockId = new RDDBlockId(par, partition.index)
              if (blockManager.blockExInfo.containsKey(parBlockId) &&
                blockManager.blockExInfo.get(parBlockId).isExist
                  == 1) { // par is exist

              } else { // par not exist now, add this key to it's par's watching set
                parExist = false
                if (!blockManager.blockExInfo.containsKey(parBlockId)) {
                  blockManager.blockExInfo.put(parBlockId, new BlockExInfo(parBlockId))
                }
                blockManager.blockExInfo.get(parBlockId).sonSet += key
              }
            }
            if (parExist) { // par are all exist so we update this rdd's start time
              logTrace("par all exist, store start time of " + key)
              blockManager.blockExInfo.get(key).creatStartTime = System.currentTimeMillis()
            }
            }
          case None =>
            logError("Some Thing Wrong")
        }
        // Acquire a lock for loading this partition
        // If another thread already holds the lock, wait for it to finish return its results
        val storedValues = acquireLockForPartition[T](key)
        if (storedValues.isDefined) {
          return new InterruptibleIterator[T](context, storedValues.get)
        }

      // val blockkey = new RDDBlockId(rdd.id, partition.index)
      logInfo(s"Partition $key not found, computing it")

        // Otherwise, we have to load the partition ourselves
        try {
          logInfo(s"Partition $key not found, computing it")
          val computedValues = rdd.computeOrReadCheckpoint(partition, context)

          // 此partition计算结束返回计算时间，得到createcost

          blockManager.blockExInfo.get(key).writeFinAndCalCreatCost(System.currentTimeMillis())

          // If the task is running locally, do not persist the result
          if (context.isRunningLocally) {
            return computedValues
          }

          // Otherwise, cache the values and keep track of any updates in block statuses
          val updatedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
          val cachedValues = putInBlockManager(key, computedValues, storageLevel, updatedBlocks)
          val metrics = context.taskMetrics
          val lastUpdatedBlocks = metrics.updatedBlocks.getOrElse(Seq[(BlockId, BlockStatus)]())
          metrics.updatedBlocks = Some(lastUpdatedBlocks ++ updatedBlocks.toSeq)
          new InterruptibleIterator(context, cachedValues)

        } finally {
          loading.synchronized {
            loading.remove(key)
            loading.notifyAll()
          }
        }
    }
  }

