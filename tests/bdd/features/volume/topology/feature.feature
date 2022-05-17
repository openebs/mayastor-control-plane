Feature: Volume Topology
  The volume topology feature is for selection of particular pools to schedule a replica on.
  All the volume topology key and values should have an exact match with pool labels, although pools
  can have extra labels, for the selection of that pool. No volume topology symbolizes the volume replicas
  can be scheduled on any available pool.

  Background:
    Given a control plane, two Io-Engine instances, two pools

  Scenario: sufficient suitable pools which contain volume topology labels
    Given a request for a volume with topology same as pool labels
    When the number of volume replicas is less than or equal to the number of suitable pools
    Then volume creation should succeed with a returned volume object with topology
    And pool labels must contain all the volume request topology labels

  Scenario: desired number of replicas cannot be created
    Given a request for a volume with topology different from pools
    When the number of suitable pools is less than the number of desired volume replicas
    Then volume creation should fail with an insufficient storage error
    And pool labels must not contain the volume request topology labels

  Scenario: sufficient suitable pools which do not contain volume pool topology labels
    Given a request for a volume without pool topology
    When the number of volume replicas is less than or equal to the number of suitable pools
    Then volume creation should succeed with a returned volume object without pool topology
    And volume request should not contain any pool topology labels

  Scenario: successfully adding a replica to a volume with pool topology
    Given an existing published volume with a topology matching pool labels
    And additional unused pools with labels containing volume topology
    When a user attempts to increase the number of volume replicas
    Then an additional replica should be added to the volume
    And pool labels must contain all the volume topology labels

  Scenario: cannot add a replica to a volume with pool topology labels
    Given an existing published volume with a topology not matching pool labels
    And a pool which does not contain the volume topology label
    When a user attempts to increase the number of volume replicas
    Then replica addition should fail with an insufficient storage error
    And pool labels must not contain the volume topology labels

  Scenario: successfully adding a replica to a volume without pool topology
    Given an existing published volume without pool topology
    And suitable available pools with labels
    When a user attempts to increase the number of volume replicas
    Then an additional replica should be added to the volume
    And volume should not contain any pool topology labels
