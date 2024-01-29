Feature: Volume Topology
  The volume topology feature is for selection of particular pools to schedule a replica on.
  All the volume topology key and values should have an exact match with nodes labels, although nodes
  can have extra labels, for the selection of that pool. No volume topology symbolizes the volume replicas
  can be scheduled on any available pool.

  Background:
    Given a control plane, three Io-Engine instances, three pools

  Scenario: suitable pool on node  whose labels contain volume topology labels
    Given a request for a volume with its node topology inclusion labels matching certain nodes with same labels
    When the volume replicas has node topology inclusion labels matching to the labels as that of certain nodes
    Then volume creation should succeed with a returned volume object with node topology
    And node labels must contain all the volume request topology labels

  Scenario: unsuitable pool on node whose labels contain volume topology labels
    Given a request for a volume with its node topology inclusion labels matching lesser number of nodes with same labels
    When the volume replicas count is more than the node topology inclusion labels matching to the labels as that of certain nodes
    Then replica creation should fail with an insufficient storage error

  Scenario: desired number of replicas cannot be created
    Given a request for a volume with its topology node inclusion labels not matching with any node with labels
    When the volume replicas has node topology inclusion labels not matching with any node with labels
    Then replica creation should fail with an insufficient storage error

  Scenario: desired number of replicas cannot be created if node topology inclusion labels and exclusion label as same
    Given a request for a volume with its node topology inclusion labels and exclusion label as same
    When the volume replicas has node topology inclusion labels and exclusion labels same
    Then replica creation should fail with an insufficient storage error

  Scenario: suitable pools which caters volume exclusion topology labels
    Given a request for a volume with its node topology exclusion labels key matching with any node with labels
    When the volume replicas has node topology exclusion labels key matching with any node with labels key and not value
    Then volume creation should succeed with a returned volume object with exclusion topology key same as pool key and different value
