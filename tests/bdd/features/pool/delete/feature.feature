Feature: Pool deletion

  Background:
    Given a control plane, Io-Engine instances
    And file based pool disks

  Scenario: deleting an existing unused pool
    Given a pool with no replicas
    When the user attempts to delete the pool
    Then the pool should be deleted

  Scenario: deleting a pool that is in use
    Given a pool with replicas
    When the user attempts to delete the pool
    Then the pool deletion should fail with error kind "InUse"

  Scenario: deleting an existing pool on an unreachable node that is still marked online
    Given a pool on an unreachable online node
    When the user attempts to delete the pool
    Then the pool deletion should fail with error kind "Timeout"

  Scenario: deleting an existing pool on an unreachable offline node
    Given a pool on an unreachable offline node
    When the user attempts to delete the pool
    Then the pool deletion should fail with error kind "NodeOffline"

  Scenario: deleting a non-existing pool
    When the user attempts to delete a pool that does not exist
    Then the pool deletion should fail with error kind "NotFound"
