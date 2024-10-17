package com.devsisters.shardcake

import com.devsisters.shardcake.interfaces.Storage
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.topic.{ Message, MessageListener }
import zio.stream.ZStream
import zio.{ Queue, Task, Unsafe, ZIO, ZLayer }

import scala.jdk.CollectionConverters._

object StorageHazelcast {

  /**
   * A layer that returns a Storage implementation using Hazelcast
   */
  val live: ZLayer[HazelcastInstance with HazelcastConfig, Nothing, Storage] =
    ZLayer {
      for {
        config            <- ZIO.service[HazelcastConfig]
        hazelcastInstance <- ZIO.service[HazelcastInstance]
        assignmentsMap     = hazelcastInstance.getMap[String, String](config.assignmentsKey)
        podsMap            = hazelcastInstance.getMap[String, String](config.podsKey)
        assignmentsTopic   = hazelcastInstance.getTopic[String](config.assignmentsKey)
      } yield new Storage {
        def getAssignments: Task[Map[ShardId, Option[PodAddress]]] =
          ZIO
            .attemptBlocking(assignmentsMap.entrySet())
            .map(
              _.asScala.toList
                .flatMap(entry =>
                  entry.getKey.toIntOption.map(
                    _ -> (if (entry.getValue.isEmpty) None
                          else PodAddress(entry.getValue))
                  )
                )
                .toMap
            )

        def saveAssignments(assignments: Map[ShardId, Option[PodAddress]]): Task[Unit] =
          ZIO.attemptBlocking(assignmentsMap.putAll(assignments.map { case (k, v) =>
            k.toString -> v.fold("")(_.toString)
          }.asJava)) *>
            ZIO.attemptBlocking(assignmentsTopic.publish("ping")).unit

        def assignmentsStream: ZStream[Any, Throwable, Map[ShardId, Option[PodAddress]]] =
          ZStream.unwrap {
            for {
              queue   <- Queue.unbounded[String]
              runtime <- ZIO.runtime[Any]
              _       <- ZIO.attemptBlocking(
                           assignmentsTopic.addMessageListener(
                             new MessageListener[String] {
                               def onMessage(msg: Message[String]): Unit =
                                 Unsafe.unsafe(implicit unsafe => runtime.unsafe.run(queue.offer(msg.getMessageObject)))
                             }
                           )
                         )
            } yield ZStream.fromQueueWithShutdown(queue).mapZIO(_ => getAssignments)
          }

        def getPods: Task[Map[PodAddress, Pod]]              =
          ZIO
            .attemptBlocking(podsMap.entrySet())
            .map(
              _.asScala
                .flatMap(entry => PodAddress(entry.getKey).map(address => address -> Pod(address, entry.getValue)))
                .toMap
            )
        def savePods(pods: Map[PodAddress, Pod]): Task[Unit] =
          ZIO.fromCompletionStage(podsMap.putAllAsync(pods.map { case (k, v) => k.toString -> v.version }.asJava)).unit
      }
    }
}
