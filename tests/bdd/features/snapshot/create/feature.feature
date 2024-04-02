Feature: Volume Snapshot Creation

  Background:
    Given a deployer cluster

  Scenario Outline: Snapshot creation of a single replica volume
    Given we have a single replica <publish_status> volume where the replica is <replica_location> to the target
    When we try to create a snapshot for the volume
    Then the snapshot creation should be successful
    And the snapshot status ready as source should be false
    And the snapshot source size should be equal to the volume size
    And the snapshot size should be equal to the volume size
    And the snapshot timestamp should be in the RFC 3339 format
    And the snapshot timestamp should be within 1 minute of creation (UTC)
    And the volume snapshot state has a single online replica snapshot
    And the replica snapshot's timestamp should be in the RFC 3339 format
    And the replica snapshot's timestamp should be within 1 minute of creation (UTC)
    Examples:
      | publish_status | replica_location |
      | published      | remote           |
      | published      | local            |
      | unpublished    | remote           |
      | unpublished    | local            |

  Scenario: Snapshot creation for a single replica volume fails when the replica is offline
    Given we have a single replica volume
    And the replica is offline
    When we try to create a snapshot for the volume
    Then the snapshot creation should fail with preconditions failed
    And the snapshot should not be listed

  Scenario: Snapshot creation for multi-replica volume
    Given we have a multi-replica volume
    When we try to create a snapshot for the volume
    Then the snapshot creation should be successful
    And the snapshot status ready as source should be false
    And the snapshot source size should be equal to the volume size
    And the snapshot size should be equal to the volume size
    And the snapshot timestamp should be in the RFC 3339 format
    And the snapshot timestamp should be within 1 minute of creation (UTC)
    And the volume snapshot state has multiple online replica snapshots

  Scenario Outline: Existence of snapshot after node restart
    Given we have a single replica volume
    And a successfully created snapshot
    When we <bring_down> the node hosting the snapshot
    Then the replica snapshot should be reported as offline
    When we bring up the node hosting the snapshot
    Then the replica snapshot should be reported as online
    Examples:
      | bring_down |
      | stop       |
      | kill       |

  Scenario: Retrying snapshot creation until it succeeds
    Given we have a single replica volume
    And the volume is published on node A
    And the node A is paused
    When we try to create a snapshot for the volume
    Then the snapshot creation should fail
    When the node A is brought back
    And we retry the same snapshot creation
    Then the snapshot creation should be successful

  Scenario: Snapshot creation of a single replica volume after its source is deleted
    Given we have a single replica volume
    When a snapshot is created for the volume
    When the volume is deleted
    And we retry the same snapshot creation
    Then it should fail with already exists

  Scenario: Retrying snapshot creation after source is deleted should fail
    Given we have a single replica volume with replica on node A
    And the node A is paused
    When we try to create a snapshot for the volume
    Then the snapshot creation should fail
    And the node A is stopped
    And we delete the source volume
    And we retry the same snapshot creation
    Then the snapshot creation should fail with failed preconditions
    When the node A is brought back
    Then eventually the snapshot should be deleted

  Scenario: Subsequent creation with same snapshot id should fail
    Given we have a single replica volume with replica on node A
    And a successfully created snapshot
    When we retry the same snapshot creation
    Then the snapshot creation should be fail with already exists

  Scenario: Multi-replica snapshot with one node down
    Given we have a multi-replica volume
    And the node A is paused
    When we try to create a snapshot for the volume
    Then the snapshot creation should fail
    When the node A is brought back
    And we try to create a snapshot for the volume
    Then the snapshot creation should be successful
