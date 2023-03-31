Feature: Thin Provisioning - Volume Rebuild
  Background:
    Given a control plane, Io-Engine instances and pools

  # Thin replication.
  Scenario: Replicating a thin volume
    Given a thin provisioned volume with 1 replica
    When data is written to the volume
    And the number of replicas is increased
    When the new replica is fully rebuilt
    Then the new replica allocation equals the volume allocation

  # Out-of-space condition for a thin multi-replica volume.
  # No different from any other I/O failure.
  Scenario: Resolving out-of-space condition for a thin multi-replica volume
    Given a thin provisioned volume with 2 replicas
    When data is written to the volume which exceeds the free space on one of the pools
    Then a replica is reported as faulted due to out-of-space
    And the total number of healthy replicas of the volume decreases by one
    But the client app is not affected because other healthy replicas exists
    And the faulted replica is relocated to another pool with sufficient free space
    When the new replica is fully rebuilt
    Then the new replica allocation equals the volume allocation
    And the total number of healthy replicas is restored
