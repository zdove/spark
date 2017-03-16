package org.apache.spark.storage
import java.util
/**
  * Created by zdove on 2017/3/14.
  */
trait MemoryEntryManager [K, V] {
    def getEntry(blockId: K): V

      def putEntry(key: K, value: V): V

      def removeEntry(key: K): V

      def clear()

      def containsEntry(key: K): Boolean
  }

class FIFOMemoryEntryManager[K, V] extends MemoryEntryManager[K, V] {
    val entries = new util.LinkedHashMap[K, V](32, 0.75f)

      override def getEntry(key: K): V = {
        entries.synchronized {
            entries.get(key)
          }
      }

      override def putEntry(key: K, value: V): V = {
        entries.synchronized {
            entries.put(key, value)
          }
      }

      def clear() {
        entries.synchronized {
            entries.clear()
          }
      }

      override def removeEntry(key: K): V = {
        entries.synchronized {
            entries.remove(key)
          }
  }

      override def containsEntry(key: K): Boolean = {
        entries.synchronized {
            entries.containsKey(key)
          }
      }
  }

class LRUMemoryEntryManager[K, V] extends MemoryEntryManager[K, V] {
    def entrySet() : util.Set[util.Map.Entry[K, V]] = {
        entries.entrySet()
      }

      val entries = new util.LinkedHashMap[K, V](32, 0.75f, true)

      override def getEntry(key: K): V = {
        entries.synchronized {
            entries.get(key)
          }
      }

      override def putEntry(key: K, value: V): V = {
        entries.synchronized {
            entries.put(key, value)
          }
      }

      def clear() {
        entries.synchronized {
            entries.clear()
          }
      }

      override def removeEntry(key: K): V = {
        entries.synchronized {
            entries.remove(key)
          }
      }

      override def containsEntry(key: K): Boolean = {
        entries.synchronized {
            entries.containsKey(key)
          }
      }
  }
