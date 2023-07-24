Feature: Volume Snapshot Clone Listing

  Background:
    Given a deployer cluster

  Scenario: List a snapshot clone
    Given a valid snapshot of a single replica volume
    When we create a new volume with the snapshot as its source
    Then we should be able to list the new volume
    And the new volume's source should be the snapshot