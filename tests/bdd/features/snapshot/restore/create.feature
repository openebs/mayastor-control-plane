Feature: Create Volume From Snapshot

  Background:
    Given a deployer cluster
    And a valid snapshot of a multi replica volume

  Scenario Outline: Create a new volume as a snapshot restore from a valid snapshot
    When we create a new volume with <repl_count> replicas using snapshot as its source
    Then a new <repl_count> replicas will be created for the new volume
    And all replicas <repl_count> capacity will be same as the snapshot
    And the restored volume own allocated_snapshot size should be 0
    Examples:
    | repl_count |
    | 1          |
    | 3          |

  Scenario: Create multiple new volumes as snapshot restores for a valid snapshot
    When we attempt to create 4 new volumes with the snapshot as their source
    Then all requests should succeed
    And the restored volume's own allocated_snapshot size should be 0

  Scenario Outline: Snapshotting a restored volume
    Given we create a new volume with <repl_count> replicas using snapshot as its source
    Then the restored volume own allocated_snapshot size should be 0
    When we allocate 4MiB of data of the restored volume
    Then the restored volume allocation size should be <repl_count> * 4MiB
    And we create a snapshot of the restored volume
    Then the snapshot allocated size should be 4MiB
    And the restored volume total allocation size should be <repl_count> * 4MiB
    Then we allocate 4MiB of data of the restored volume
    And we create another snapshot of the restored volume
    Then the snapshot 2 allocated size should be 4MiB
    And the restored volume total allocation size should be <repl_count> * 8MiB
    Examples:
    | repl_count  |
    | 1           |
    | 3           |

  Scenario: Create a chain of restored volumes
    Given we can create volume restore 1 with the previous snapshot as its source
    And we create a snapshot from volume restore 1
    Then we can create volume restore 2 with the previous snapshot as its source
    And we create a snapshot from volume restore 2
    Then we can create volume restore 3 with the previous snapshot as its source
    And we create a snapshot from volume restore 3
    Then we can create volume restore 4 with the previous snapshot as its source

  Scenario Outline: Create a new volume as a snapshot restore with one node down
    When we have a node down
    And a request to create a new volume with the snapshot as its source
    Then the request should fail with PreconditionFailed
    When we bring the node back up
    And we create a new volume with <repl_count> replicas using snapshot as its source
    Then the request should succeed with replica count as <repl_count>
    Examples:
    | repl_count |
    | 1          |
    | 3          |
