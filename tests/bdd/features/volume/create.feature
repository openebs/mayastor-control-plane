Feature: Volume creation

  Background:
    Given one or more Mayastor instances
    And a control plane

  Scenario: successful creation
    When a user attempts to create a volume
    And there are at least as many suitable pools as there are requested replicas
    Then the volume should be created
    And the volume object should be returned

  Scenario: spec cannot be satisfied
    When a user attempts to create a volume
    And the spec of the volume cannot be satisfied
    Then the volume should not be created
    And the reason the volume could not be created should be returned

  Scenario: provisioning failure
    When a user attempts to create a volume
    And the request can initially be satisfied
    And during the provisioning there is a failure
    Then the volume should not be created
    And the reason the volume could not be created should be returned
    And the partially created volume should eventually be cleaned up