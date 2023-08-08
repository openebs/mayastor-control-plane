Feature: Create Volume From Snapshot

  Background:
    Given a deployer cluster
    And a valid snapshot of a single replica volume

  Scenario: Create a new volume as a snapshot restore from a valid snapshot
    When we create a new volume with the snapshot as its source
    Then a new replica will be created for the new volume
    And the replica's capacity will be same as the snapshot
    And the restored volume own allocated_snapshot size should be 0

  Scenario: Create multiple new volumes as snapshot restores for a valid snapshot
    When we attempt to create 4 new volumes with the snapshot as their source
    Then all requests should succeed
    And the restored volume's own allocated_snapshot size should be 0

  Scenario: Snapshotting a restored volume
    Given we create a new volume with the snapshot as its source
    Then the restored volume own allocated_snapshot size should be 0
    When we allocate 4MiB of data of the restored volume
    Then the restored volume allocation size should be 4MiB
    And we create a snapshot of the restored volume
    Then the snapshot allocated size should be 4MiB
    And the restored volume total allocation size should be 4MiB
    Then we allocate 4MiB of data of the restored volume
    And we create a snapshot of the restored volume
    Then the snapshot allocated size should be 4MiB
    And the restored volume total allocation size should be 8MiB

  Scenario: Create a chain of restored volumes
    Given we can create volume restore 1 with the previous snapshot as its source
    And we create a snapshot from volume restore 1
    Then we can create volume restore 2 with the previous snapshot as its source
    And we create a snapshot from volume restore 2
    Then we can create volume restore 3 with the previous snapshot as its source
    And we create a snapshot from volume restore 3
    Then we can create volume restore 4 with the previous snapshot as its source
