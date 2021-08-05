Feature: Adjusting the volume replicas

  Background:
    Given an existing volume

  Scenario: successfully adding a replica
    When a user attempts to increase the number of volume replicas
    And the control plane finds a suitable pool
    Then an additional replica should be added to the volume

  Scenario: successfully removing a replica
    When a user attempts to decrease the number of volume replicas
    And the number of existing volume replicas is greater than one
    Then a replica should be removed from the volume

  Scenario: setting volume replicas to zero
    When a user attempts to decrease the number of volume replicas to zero
    Then the replica removal should fail
    And the reason the replica removal failed should be returned

  Scenario: replacing a faulted replica
    When Mayastor has marked a replica as faulted
    And the number of volume replicas is greater than the number of healthy replicas
    And there are suitable pools where a replacement replica can be created
    Then a new replica should eventually be added to the volume
    And the faulted replica should eventually be deleted