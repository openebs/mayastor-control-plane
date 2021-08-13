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