Feature: Drain

  Background:
    Given Multiple uncordoned nodes

  # B2-365
  Scenario: Draining an uncordoned node
    Given an uncordoned node
    When the user issues a drain command with a label to the node
    Then the command will succeed
    Then the draining node's nexuses should be moved to other suitable nodes
    And the node should be cordoned with the given label

# Draining when an existing cordon is in effect
  # B2-366
  Scenario: Draining a node cordoned with a different label
    Given a node cordoned with a given label
    When the user issues a drain command to the node with a new label
    Then the command will succeed
    And the draining node's nexuses should be moved to other suitable nodes
    And the node should retain a cordon with the new drain label

  # B2-366
  Scenario: Draining a node cordoned with the same label
    Given a node cordoned with a given label
    When the user issues a drain command to the node with the same label
    Then the command will fail
    And the command will have no effect on its drain state

# Draining when an existing drain is in progress
  # B2-367
  Scenario: Draining a draining node
    Given a node draining with a given label
    When the user issues a drain command to the node with a different label
    Then the command will succeed
    And the draining node's nexuses should be moved to other suitable nodes
    And the node should retain a cordon with the new drain label

  # B2-367
  Scenario: Draining a node draining with the same label
    Given a node draining with a given label
    When the user issues a drain command to the node with the same label
    Then the command will succeed
    And the command will have no effect on its drain state

# Draining when an existing drain has completed
  Scenario: Draining a drained node
    Given a node drained with a given label
    When the user issues a drain command to the node with a different label
    Then the command will succeed
    And the drained node's nexuses should remain on other suitable nodes
    And the node should retain a cordon with the new drain label

  Scenario: Draining a node drained with the same label
    Given a node drained with a given label
    When the user issues a drain command to the node with the same label
    Then the command will succeed
    And the command will have no effect on its drain state

# The system is unable to move all resources from the draining node to another node
  # B2-369
  Scenario: Draining a node with insufficient system capacity
    Given insufficient system-wide capacity on other nodes to accommodate all drained resources
    When the user issues a drain command to a node
    Then the command will not complete
    And resources will remain on the draining node
    And the node should be cordoned

  # B2-369
  Scenario: Insufficient system capacity during a node drain
    Given a node is draining
    When the system-wide capacity becomes insufficient
    Then resources will remain on the draining node
    And the node should remain cordoned

  # B2-369
  Scenario: Drain completion for a partially drained node
    Given a node is draining
    And the system-wide capacity is insufficient
    When system capacity becomes sufficient
    Then the draining node's nexuses should be moved to other suitable nodes
    And the node should remain cordoned

# Cancelling a drain command
  # B2-368
  Scenario: Cancelling a drain command
    Given a node is draining with a given label
    When the user issues an uncordon command with the given label
    Then the command will succeed
    And the labelled cordon will be removed
    And the drain operation associated with this label will stop

  # B2-368
  Scenario: Cancelling a drain command with an invalid label
    Given a node is draining with a given label
    When the user issues an uncordon command with a label not associated with the node
    Then the command will fail
    And the command will have no effect on its drain state

# Restarting a node
  Scenario: Restarting a draining node
    Given a node currently being drained
    When the node is restarted
    Then the draining operation should eventually complete

# Nexus placement
  Scenario: Nexus placement on a cluster with one or more draining or drained nodes
    Given one or more draining or drained nodes
    When a volume is created and published
    Then the volume target node selected will not be a drained or draining node
