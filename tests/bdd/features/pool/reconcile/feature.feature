Feature: Pool reconciliation

  Background:
    Given a control plane, Io-Engine instances
    And file based pool disks

  Scenario: destroying a pool that needs to be deleted
    Given a pool "p0" that could not be deleted due to an unreachable node
    When the node comes back online
    Then the pool should eventually be deleted

  Scenario: import a pool when a node restarts
    Given a pool "p0" on an unreachable node
    When the node comes back online
    Then the pool should eventually be imported
