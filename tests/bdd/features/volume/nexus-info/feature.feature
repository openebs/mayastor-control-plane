Feature: Persistent Nexus Info

  Background:
    Given an existing volume

  Scenario: publishing a volume
    Given a volume that is not published
    When the volume is published
    Then the nexus info structure should be present in the persistent store

  Scenario: unpublishing a volume
    Given a volume that is published
    When the volume is unpublished
    Then the nexus info structure should be present in the persistent store

  Scenario: re-publishing a volume
    Given a volume that has been published and unpublished
    When the volume is re-published
    Then the nexus info structure should be present in the persistent store
    And the old nexus info structure should not be present in the persistent store

  Scenario: Deleting a published volume
    Given a volume that is published
    When the volume is deleted
    Then the nexus info structure should not be present in the persistent store