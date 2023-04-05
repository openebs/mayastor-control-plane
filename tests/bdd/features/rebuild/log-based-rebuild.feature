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

  Scenario: Node goes permanently down while log-based rebuild running
    Given a volume with three replicas, filled with user data
    When a non-local child becomes faulted
    And the replica is online again within the timed-wait period
    Then log-based rebuild starts after some time
    But the node hosting rebuilding replica crashes permanently
    Then the replica is not online again within the timeout period
    And a full rebuild starts after some time

  Scenario: Volume target is moved before starting log-based rebuild
    Given a volume with three replicas, filled with user data
    When a child becomes faulted
    And the replica is online again within the timed-wait period
    When the volume target moves to a different node before the log-based rebuild starts
    Then a full rebuild starts after some time

  Scenario: Volume target is moved during log-based rebuild
    Given a volume with three replicas, filled with user data
    When a child becomes faulted
    And the replica is online again within the timed-wait period
    Then log-based rebuild starts after some time
    And the volume target moves to a different node before rebuild completion
    Then a full rebuild starts after some time

  Scenario: One more child faults during ongoing timed-wait
    Given a volume with three replicas, filled with user data
    When a child becomes faulted
    And the replica is online again within the timed-wait period
    And another child becomes faulted within the timed-wait period
    Then a log-based rebuild for the first faulted child starts after some time
    And a full rebuild for the second faulted child starts after some time