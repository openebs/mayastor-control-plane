Feature: Thin Provisioning - Cluster Wide
  Background:
    Given a control plane, Io-Engine instances and pools

  # Simultaneous out-of-space condition on several thin multi-replica volumes.
  # This is a scenario when multiple volumes get out-of-space on some replicas
  # at the same moment and some of them can be quickly recovered by
  # freeing space of pool(s).
  Scenario: Resolving out-of-space condition for several thin multi-replica volumes
    Given there are 4 overcommitted thin volumes
    And they share the same pools
    When filling up the volumes with data
    Then several replicas on the same pool are reported as faulted due to out-of-space
    And if other pools with sufficient free space exist on the cluster
    Then the largest of the failed replicas is relocated to another pool with sufficient free space
    And the other faulted replicas start to rebuild in-place
    And all volumes become healthy
    And the data transfer to all volumes completes with success

  # Reaching capacity of a whole cluster.
  # This is a situation where the storage cluster is exhausted and we cannot relocated
  # replicas because no free pools exist on the cluster.
  Scenario: Reaching physical capacity of a cluster
    Given there are 4 overcommitted thin volumes
    And no other pools with sufficient free space exist on the cluster
    When filling up the volumes with data
    Then several replicas are reported as faulted due to out-of-space
    And since no other pool can accommodate the out-of-space replicas
    Then the out-of-space replicas stay permanently faulted
    And the affected volumes remain degraded
    But the data transfer to all volumes completes with success
