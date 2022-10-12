Feature: Target Switchover test

  Background:
    Given a control plane, two Io-Engine instances, two pools

  Scenario: R/W access to older target should be restricted after switchover
    Given a published volume with two replicas
    When the volume republish on another node has succeeded
    Then the newer target should have R/W access to the replicas
    And the older target should not have R/W access the replicas

  Scenario: node offline should not fail the switchover
    Given a published volume with two replicas
    When the node hosting the nexus is killed
    And the volume republish on another node has succeeded
    And the node hosting the nexus is brought back
    Then the newer target should have R/W access to the replicas
    And the older target should not have R/W access the replicas

  Scenario: R/W access to older target should be restricted if io-path is established
    Given a published volume with two replicas
    When the node hosting the nexus has io-path broken
    And the volume republish on another node has succeeded
    And the node hosting the older nexus has io-path established
    Then the newer target should have R/W access to the replicas
    And the older target should not have R/W access the replicas

  Scenario: R/W access to older target should be restricted if io-path and grpc connection is established
    Given a published volume with two replicas
    When the node hosting the nexus has io-path broken
    And the node hosting the nexus has grpc server connection broken
    And the volume republish on another node has succeeded
    And the node hosting the older nexus has io-path established
    And the node hosting the nexus has grpc server connection established
    Then the newer target should have R/W access to the replicas
    And the older target should not have R/W access the replicas

  Scenario Outline: continuous switchover and older target destruction should be seamless with io-path broken
    Given a published volume with two replicas
    When the node hosting the nexus has io-path broken
    And the volume republish and the destroy shutdown target call has succeeded for <n> times
    Then the newer target should have R/W access to the replicas
    And the older target should not have R/W access the replicas
    When the node hosting the older nexus has io-path established
    And the volume republish and the destroy shutdown target call has succeeded for <n> times
    Then the newer target should have R/W access to the replicas
    Examples:
      | n |
      | 5 |

  Scenario Outline: continuous switchover and older target destruction should be seamless with io-path and grpc connection broken
    Given a published volume with two replicas
    When the node hosting the nexus has io-path broken
    And the node hosting the nexus has grpc server connection broken
    And the volume republish and the destroy shutdown target call has succeeded for <n> times
    Then the newer target should have R/W access to the replicas
    And the older target should not have R/W access the replicas
    When the node hosting the older nexus has io-path established
    And the node hosting the nexus has grpc server connection established
    And the volume republish and the destroy shutdown target call has succeeded for <n> times
    Then the newer target should have R/W access to the replicas
    Examples:
      | n |
      | 5 |
