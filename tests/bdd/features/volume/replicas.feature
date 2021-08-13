Feature: Adjusting the volume replicas

  Background:
    Given an existing volume

  Scenario: successfully adding a replica
    Given a suitable available pool
    When a user attempts to increase the number of volume replicas
    Then an additional replica should be added to the volume

  Scenario: successfully removing a replica
    Given the number of volume replicas is greater than one
    When a user attempts to decrease the number of volume replicas
    Then a replica should be removed from the volume

  Scenario: setting volume replicas to zero
    Then setting the number of replicas to zero should fail with a suitable error

  # TODO: Enable this after handling the simple cases
#  Scenario: replacing a faulted replica
#    When Mayastor has marked a replica as faulted
#    And the number of volume replicas is greater than the number of healthy replicas
#    And there are suitable pools where a replacement replica can be created
#    Then a new replica should eventually be added to the volume
#    And the faulted replica should eventually be deleted