Feature: Volume Topology
  The volume topology feature is for selection of particular pools to schedule a replica on.
  All the volume topology key and values should have an exact match with pool labels, although pools
  can have extra labels, for the selection of that pool. No volume topology symbolizes the volume replicas
  can be scheduled on any available pool.

  Background:
    Given a control plane, three Io-Engine instances, three pools

  Scenario: suitable pools which contain volume topology labels
    Given a request for a volume with its topology inclusion labels matching certain pools with same labels
    When the volume replicas has topology inclusion labels matching to the labels as that of certain pools
    Then volume creation should succeed with a returned volume object with topology
    And pool labels must contain all the volume request topology labels

  Scenario: suitable pools which contain volume topology labels key with empty value
    Given a request for a volume with its topology inclusion labels key with empty value matching certain pools with same labels
    When the volume replicas has topology inclusion labels key with empty value matching to the labels as that of certain pools
    Then volume creation should succeed with a returned volume object with topology label key with empty value

  Scenario: desired number of replicas cannot be created
    Given a request for a volume with its topology inclusion labels not matching with any pools with labels
    When the volume replicas has topology inclusion labels not matching with any pools with labels
    Then volume creation should fail with an insufficient storage error
    And pool labels must not contain the volume request topology labels

  Scenario: suitable pools which caters volume exclusion topology labels
    Given a request for a volume with its topology exclusion labels key matching with any pools with labels
    When the volume replicas has topology exclusion labels key matching with any pools with labels key and not value
    Then volume creation should succeed with a returned volume object with exclusion topology key same as pool key and different value

  Scenario: suitable pools which caters volume exclusion topology labels key with empty value
    Given a request for a volume with its topology exclusion labels key with empty value matching with any pools with labels
    When the volume replicas has topology exclusion labels key with empty value not matching with any pools with labels key
    Then volume creation should succeed with a returned volume object with exclusion topology key different as pool key

  Scenario: desired number of replicas cannot be created if topology inclusion labels and exclusion label as same
    Given a request for a volume with its topology inclusion labels and exclusion label as same
    When the volume replicas has topology inclusion labels and exclusion labels same
    Then replica creation should fail with an insufficient storage error
