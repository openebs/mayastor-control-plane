Feature: Volume Snapshot Clone Creation

  Background:
    Given a deployer cluster
    And a valid snapshot of a single replica volume

  Scenario: Create a new volume as a snapshot clone from a valid snapshot
    When we create a new volume with the snapshot as its source
    Then a new replica will be created for the new volume
    And the replica's capacity will be same as the snapshot

  Scenario: Create multiple new volumes as snapshot clones for a valid snapshot
    When we attempt to create 4 new volumes with the snapshot as their source
    Then all requests should succeed

  Scenario: Create a chain of restored volumes
    Given we can create volume restore 1 with the previous snapshot as its source
    And we create a snapshot from volume restore 1
    Then we can create volume restore 2 with the previous snapshot as its source
    And we create a snapshot from volume restore 2
    Then we can create volume restore 3 with the previous snapshot as its source
    And we create a snapshot from volume restore 3
    Then we can create volume restore 4 with the previous snapshot as its source