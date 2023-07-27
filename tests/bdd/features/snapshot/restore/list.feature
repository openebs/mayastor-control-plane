Feature: Volume Snapshot Restore Listing

  Background:
    Given a deployer cluster

  Scenario: List new volume created from a snapshot
    Given a valid snapshot of a single replica volume
    When we create a new volume with the snapshot as its source
    Then we should be able to list the new volume
    And the new volume's source should be the snapshot