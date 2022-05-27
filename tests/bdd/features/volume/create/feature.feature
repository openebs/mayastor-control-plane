Feature: Volume creation

  Background:
    Given a control plane, Io-Engine instances and a pool

  Scenario: sufficient suitable pools
    Given a request for a volume
    When the number of volume replicas is less than or equal to the number of suitable pools
    Then volume creation should succeed with a returned volume object

  Scenario: desired number of replicas cannot be created
    Given a request for a volume
    When the number of suitable pools is less than the number of desired volume replicas
    Then volume creation should fail with an insufficient storage error

  Scenario: provisioning failure due to missing Io-Engine
    Given a request for a volume
    When there are no available Io-Engine instances
    Then volume creation should fail with a precondition failed error

  Scenario: provisioning failure due to gRPC timeout
    Given a request for a volume
    When a create operation takes longer than the gRPC timeout
    Then volume creation should fail with a precondition failed error
    And there should not be any specs relating to the volume
