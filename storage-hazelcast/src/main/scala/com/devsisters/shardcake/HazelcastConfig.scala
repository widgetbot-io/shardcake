package com.devsisters.shardcake

/**
 * The configuration for the Hazelcast storage implementation.
 * @param assignmentsKey the key to store shard assignments
 * @param podsKey the key to store registered pods
 */
case class HazelcastConfig(assignmentsKey: String, podsKey: String)

object HazelcastConfig {
  val default: HazelcastConfig = HazelcastConfig(assignmentsKey = "shard_assignments", podsKey = "pods")
}
