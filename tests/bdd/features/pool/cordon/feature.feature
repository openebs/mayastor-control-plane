Feature: Pool Cordoning

  Background:
    Given multiple uncordoned nodes

  Scenario: Cordoning a pool
    Given an uncordoned pool
    When we issue a cordon command
    Then the command will succeed
    And new resources cannot be scheduled on the cordoned pool

  Scenario Outline: Cordoning a pool with resources
    Given an uncordoned pool with test resources
    When we issue a cordon command with resource <resource>
    Then the command will succeed
    And new <resource> resources cannot be scheduled on the cordoned pool
    But other resources can
    Examples:
      | resource  |
      | replicas  |
      | snapshots |
      | restores  |

  Scenario: Cordoning a cordoned pool
    Given a cordoned pool
    When we issue a cordon command with additional constraints
    Then the command will succeed
    And the pool should remain cordoned
    And new resources cannot be scheduled on the cordoned pool

  Scenario: Uncordoning a pool
    Given a cordoned pool
    When we issue an uncordon command with the cordoned resources
    Then the command will succeed
    And new resources can be scheduled on the cordoned pool

  Scenario: Deleting resources on a cordoned pool
    Given a cordoned pool with resources
    When we attempt to delete resources on the cordoned pool
    Then the resources should be deleted

  Scenario: Restarting a cordoned pool
    Given a cordoned pool with resources
    When the cordoned pool is restarted
    Then the pool should be imported successfully
    And all pool resources should be Online

  Scenario: Restarting a cordoned pool with import constraint
    Given a cordoned pool with import constraint
    When the cordoned pool is restarted
    Then the pool should be not be imported

  Scenario: No replica rebuild due to pool cordon
    Given a published volume with multiple replicas
    And a cordoned pool, otherwise schedulable for the volume
    When the volume becomes degraded
    And there are insufficient uncordoned pools to accommodate new replicas
    Then the volume will remain in a degraded state
    When the pool is uncordoned
    Then the volume shall eventually rebuild become healthy

  Scenario: No replica count increase due to pool cordon
    Given a published volume with multiple replicas
    And a cordoned pool, otherwise schedulable for the volume
    When we attempt to increase the replica count
    And there are insufficient uncordoned pools to accommodate new replicas
    Then the request should fail with insufficient storage
    When the pool is uncordoned
    And we attempt to increase the replica count
    Then a new set-replica request should succeed

  Scenario: Pool should be cordoned if there is at least one cordon applied
    Given a cordoned pool with multiple cordon resources
    When we issue an uncordon command without all resources
    Then the command will succeed
    And the pool should remain cordoned

  Scenario: Pool should be uncordoned when all cordons have been removed
    Given a cordoned pool with multiple cordon resources
    When we issue an uncordon command with all resources
    Then the command will succeed
    And the pool should be uncordoned

