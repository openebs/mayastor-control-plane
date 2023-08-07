Feature: Deleting Restored Snapshot Volume

  Background:
    Given a deployer cluster
    And a single replica volume
    And a snapshot
    And a single replica volume from the snapshot

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
