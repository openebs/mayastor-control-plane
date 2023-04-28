Feature: Switchover Robustness

  Background:
    Given a deployer cluster

  Scenario: reconnecting the new target times out
    Given a single replica volume
    And a connected nvme initiator
    And a reconnect_delay set to 15s
    When we restart the volume target node
    And the ha clustering republishes
    Then the path should be established

  Scenario: path failure with no free nodes
    Given a 2 replica volume
    And a connected nvme initiator
    When we cordon the non-target node
    And we stop the volume target node
    When the ha clustering fails a few times
    And we uncordon the non-target node
    Then the path should be established

  Scenario: temporary path failure with no other nodes
    Given a 2 replica volume
    And a connected nvme initiator
    When we cordon the non-target node
    And we stop the volume target node
    When the ha clustering fails a few times
    And we restart the volume target node
    Then the path should be established
