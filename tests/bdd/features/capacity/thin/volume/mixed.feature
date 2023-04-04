Feature: Thin Provisioning
  Background:
    Given a control plane, Io-Engine instances and pools

  # Mixing thin and thick volumes: thick volumes must not be affected by ENOSPC.
  Scenario: Out-of-space condition does not affect thick volumes on mixed pools
    Given a single replica thick volume
    And a single replica overcommitted thin volume
    And both volumes share the same pool
    When data is being written to the thick volume
    And data is being written to the thin volume which exceeds the free space on the pool
    Then a write failure with ENOSPC error is reported on the thin volume
    But the thick volume is not affected and remains healthy
    And the app using the thick volume continues to run unaffected
