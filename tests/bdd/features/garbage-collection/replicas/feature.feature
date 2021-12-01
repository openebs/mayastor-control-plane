Feature: Garbage collection of replicas

  Scenario: destroying an orphaned replica
    Given a replica which is managed but does not have any owners
    Then the replica should eventually be destroyed