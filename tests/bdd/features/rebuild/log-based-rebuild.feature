Feature: Partial Rebuild

  Background:
    Given io-engine is installed and running

  Scenario: Faulted child is online again within timed-wait period
    Given a volume with three replicas, filled with user data
    When a child becomes faulted
    And the replica is online again within the timed-wait period
    Then log-based rebuild starts after some time

  Scenario: Faulted child is not online again within timed-wait period
    Given a volume with three replicas, filled with user data
    When a child becomes faulted
    But the replica is not online again within the timed-wait period
    Then a full rebuild starts after some time

  Scenario: Node goes down while log based rebuild running
    Given a volume with three replicas, filled with user data
    When a child becomes faulted
    And the replica is online again within the timed-wait period
    Then log-based rebuild starts after some time
    But the node hosting rebuilding replica crashes
    Then a full rebuild starts before the timed-wait period