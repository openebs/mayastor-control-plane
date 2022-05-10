Feature: Rebuilding a volume

  Scenario: exceeding the maximum number of rebuilds when increasing the replica count
    Given a user defined maximum number of rebuilds
    And an existing published volume
    Then adding a replica should fail if doing so would exceed the maximum number of rebuilds

  Scenario: exceeding the maximum number of rebuilds when replacing a replica
    Given a user defined maximum number of rebuilds
    And an existing published volume
    When a replica is faulted
    Then replacing the replica should fail if doing so would exceed the maximum number of rebuilds