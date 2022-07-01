Feature: Drain

  Background:
    Given Multiple uncordoned nodes

  Scenario: Draining an uncordoned node
    Given an uncordoned node
    When the user issues a drain command to the node
    Then the draining nodes nexuses should be moved to other suitable nodes
    And the node should be cordoned

  Scenario: Draining a cordoned node
    Given a cordoned node
    When the user issues a drain command to the node
    Then the draining nodes nexuses should be moved to other suitable nodes
    And the node should remain cordoned

# The system is unable to move all resources from the draining node to another node
  Scenario: Draining a node with insufficient system capacity
    Given insufficient system-wide capacity on other nodes to accommodate all drained resources
    When the user issues a drain command to a node
    Then resources will remain on the draining node
    And the node should remain cordoned

  Scenario: Insufficient system capacity during a node drain
    Given a node is draining
    When the system-wide capacity becomes insufficient
    Then resources will remain on the draining node
    And the node should remain cordoned

  Scenario: Drain completion for a partially drained node
    Given a node is draining
    And the system-wide capacity is insufficient
    When system capacity becomes sufficient
    Then the draining nodes nexuses should be moved to other suitable nodes
    And the node should remain cordoned
