Feature: Deleting Restored Snapshot Volume

  Background:
    Given a deployer cluster
    And a single replica volume with 8MiB allocated
    And a snapshot
    And we allocate 4MiB of the original volume
    And a single replica volume from the snapshot
    And the restored volume 1 allocated_replica size should be zero
    And the restored volume 1 allocated_snapshot size should be zero
    And we allocate 4MiB of the restored volume 1
    And the restored volume 1 allocated_replica size should be 4MiB

  Scenario: Deleting in reverse creation order
    When we delete the restored volume
    Then the snapshot should still exist
    And we delete the snapshot
    And the pool space usage should reflect the original volume
    When we delete the original volume
    Then the pool space usage should be zero

  Scenario: Deleting in creation order
    When we delete the original volume
    Then the snapshot and the restored volume should still exist
    And the pool space usage should reflect the snapshot and restored volume
    And we delete the snapshot
    And the pool space usage should reflect the snapshot and restored volume
    When we delete the restored volume
    Then the pool space usage should be zero

  Scenario: Delete a chain of restored volumes
    Given we create restored volume 1 snapshot 1
    Then the restored volume 1 allocated_replica size should be zero
    And the restored volume 1 allocated_snapshot size should be 4MiB
    And the restored volume 1 snapshot 1 allocation size should be 4MiB
    Then we allocate 4MiB of the restored volume 1
    And we create restored volume 1 snapshot 2
    Then the restored volume 1 snapshot 2 allocation size should be 4MiB
    And the restored volume 1 snapshot 2 total allocation size should be 8MiB
    And the restored volume 1 allocated_snapshot size should be 8MiB
    And the restored volume 1 allocated_replica size should be zero
    Then we restore volume 1 snapshot 2 into restored volume 2
    Then we allocate 4MiB of the restored volume 2
    And the restored volume 2 allocation size should be 4MiB
    When we delete the original volume
    Then the restored volume 1 allocated_replica size should be zero
    And the restored volume 1 snapshot 1 allocation size should be 4MiB
    And the restored volume 1 snapshot 2 allocation size should be 4MiB
    And the restored volume 1 snapshot 2 total allocation size should be 8MiB
    And the pool space usage should reflect the original snapshot, restored volume 1, snapshot 1, 2, and restored volume 2 (20MiB)
    When we delete the snapshot
    Then the restored volume 1 allocated_replica size should be zero
    And the restored volume 1 snapshot 1 allocation size should be 4MiB
    And the restored volume 1 snapshot 2 allocation size should be 4MiB
    And the restored volume 1 snapshot 2 total allocation size should be 8MiB
    Then the pool space usage should reflect the restored volume 1, snapshot 1, 2, restored volume 2, and deleted snapshot (20MiB)
    When we delete the restored volume
    And the restored volume 1 snapshot 1 allocation size should be 12MiB
    And the restored volume 1 snapshot 2 allocation size should be 4MiB
    And the restored volume 1 snapshot 2 total allocation size should be 8MiB
    Then the pool space usage should reflect the snapshot 1, 2, restored volume 2, and deleted snapshot and deleted restored volume 1 (20MiB)
    When we delete the restored volume 1 snapshot 1
    Then the pool space usage should reflect the snapshot 2, restored volume 2, and deleted snapshot and deleted restored volume 1 (16MiB)
    When we delete the restored volume 1 snapshot 2
    Then the pool space usage should reflect the restored volume 2, and deleted snapshot 1,2 and deleted restored volume 1 (16MiB)
    # BUG: dataplane bug where snapshots become replicas..
    #When we delete the restored volume 2
    #Then the pool space usage should be zero
