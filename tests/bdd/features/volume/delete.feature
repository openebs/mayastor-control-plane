Feature: Volume deletion

  Background:
    Given an existing volume

  Scenario: delete a volume that is not shared/published
    Given a volume that is not shared/published
    When a user attempts to delete a volume
    Then the volume should be deleted

  # The following scenario reflects the current behaviour where Mayastor will unshare and delete
  # a shared volume.
  Scenario: delete a volume that is shared/published
    Given a volume that is shared/published
    When a user attempts to delete a volume
    Then the volume should be deleted

  Scenario: delete a shared/published volume whilst a replica node is inaccessible and offline
    Given a volume that is shared/published
    And an inaccessible node with a volume replica on it
    When a user attempts to delete a volume
    Then the volume should be deleted
    And the replica on the inaccessible node should become orphaned

  Scenario: delete a shared/published volume whilst the nexus node is inaccessible
    Given a volume that is shared/published
    And an inaccessible node with the volume nexus on it
    When a user attempts to delete a volume
    Then the volume should be deleted
