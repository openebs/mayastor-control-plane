Feature: Volume sharing and publishing

  Background:
    Given an existing volume

  Scenario: successful share/publish
    When a user attempts to share/publish a volume
    And the node on which the volume should be shared/published is specified
    Then the volume should be shared/published on the specified node
    And the share URI should be returned

  Scenario: successful unshare/unpublish
    When a user attempts to unshare/unpublish a volume
    And the volume is already shared/published
    Then the volume should be unshared/unpublished