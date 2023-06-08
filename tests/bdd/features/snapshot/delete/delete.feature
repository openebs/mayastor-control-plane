Feature: Volume Snapshot deletion

  Background:
    Given a deployer cluster

  # USUAL DELETE OPERATION
  Scenario Outline: Snapshot deletion volume
    Given we have a single replica <publish_status> volume
    And we've created a snapshot for the volume
    When we attempt to delete the snapshot
    Then the snapshot deletion should not fail
    And the snapshot should not be present upon listing
    Examples:
      | publish_status |
      | published      |
      | unpublished    |


  # USUAL DELETE OPERATION WITHOUT SOURCE
  Scenario Outline: Snapshot deletion volume after volume deletion
    Given we have a single replica <publish_status> volume
    And we've created a snapshot for the volume
    When the source volume is deleted
    And we attempt to delete the snapshot
    Then the snapshot deletion should not fail
    And the snapshot should not be present upon listing
    Examples:
      | publish_status |
      | published      |
      | unpublished    |


  # SOURCE DELETION AFTER COMPLETION
  Scenario Outline: Remove volume source after snapshot creation
    Given we have a single replica <publish_status> volume
    And we've created a snapshot for the volume
    When we attempt to delete the source volume
    Then the volume should be deleted
    And the snapshot should be present upon listing
    And the volume should not be present upon listing
    Examples:
      | publish_status |
      | published      |
      | unpublished    |


  # POOL HOSTING SNAPSHOT DELETION
  Scenario Outline: Remove pool where snapshot is present
    Given we have a single replica <publish_status> volume
    And we've created a snapshot for the volume
    When we attempt to delete the pool hosting the snapshot
    Then the pool deletion should fail
    When the source volume is deleted
    And the snapshot is deleted
    Then we should be able to delete the pool
    Examples:
      | publish_status |
      | published      |
      | unpublished    |
