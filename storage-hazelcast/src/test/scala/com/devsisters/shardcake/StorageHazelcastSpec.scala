package com.devsisters.shardcake

import com.devsisters.shardcake.interfaces.Storage
import com.dimafeng.testcontainers.GenericContainer
import com.hazelcast.client.HazelcastClient
import com.hazelcast.client.config.{ ClientConfig, ClientNetworkConfig }
import com.hazelcast.core.HazelcastInstance
import zio.Clock.ClockLive
import zio._
import zio.stream.ZStream
import zio.test.TestAspect.sequential
import zio.test._

object StorageHazelcastSpec extends ZIOSpecDefault {
  val container: ZLayer[Any, Nothing, GenericContainer] =
    ZLayer.scoped {
      ZIO.acquireRelease {
        ZIO.attemptBlocking {
          val container = new GenericContainer(dockerImage = "hazelcast/hazelcast:5.1.2", exposedPorts = Seq(5701))
          container.start()
          container
        }.orDie
      }(container => ZIO.attemptBlocking(container.stop()).orDie)
    }

  val hazelcast: ZLayer[GenericContainer, Throwable, HazelcastInstance] =
    ZLayer {
      for {
        container     <- ZIO.service[GenericContainer]
        uri            = s"${container.host}:${container.mappedPort(container.exposedPorts.head)}"
        hazelConfig    = new ClientConfig
        hazelNetConfig = new ClientNetworkConfig
        _              = hazelNetConfig.addAddress(uri)
        _              = hazelConfig.setNetworkConfig(hazelNetConfig)
      } yield HazelcastClient.newHazelcastClient(hazelConfig)
    }

  def spec: Spec[TestEnvironment with Scope, Any] =
    suite("StorageRedisSpec")(
      test("save and get pods") {
        val expected = List(Pod(PodAddress("host1", 1), "1.0.0"), Pod(PodAddress("host2", 2), "2.0.0"))
          .map(p => p.address -> p)
          .toMap
        for {
          _      <- ZIO.serviceWithZIO[Storage](_.savePods(expected))
          actual <- ZIO.serviceWithZIO[Storage](_.getPods)
        } yield assertTrue(expected == actual)
      },
      test("save and get assignments") {
        val expected = Map(1 -> Some(PodAddress("host1", 1)), 2 -> None)
        for {
          _      <- ZIO.serviceWithZIO[Storage](_.saveAssignments(expected))
          actual <- ZIO.serviceWithZIO[Storage](_.getAssignments)
        } yield assertTrue(expected == actual)
      },
      test("assignments stream") {
        val expected = Map(1 -> Some(PodAddress("host1", 1)), 2 -> None)
        for {
          p      <- Promise.make[Nothing, Map[Int, Option[PodAddress]]]
          _      <- ZStream.serviceWithStream[Storage](_.assignmentsStream).runForeach(p.succeed(_)).fork
          _      <- ClockLive.sleep(1 second)
          _      <- ZIO.serviceWithZIO[Storage](_.saveAssignments(expected))
          actual <- p.await
        } yield assertTrue(expected == actual)
      }
    ).provideLayerShared(
      container >>> hazelcast ++ ZLayer.succeed(HazelcastConfig.default) >>> StorageHazelcast.live
    ) @@ sequential
}
