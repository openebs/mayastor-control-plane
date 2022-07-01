Feature: Offline Node Deletion

  Background:
    Given a cluster with multiple nodes

  Scenario: Deleting an online node
    Given an online node
    When a node delete command is issued via rest
    Then the command should fail with error kind "InUse"
    And the node should still be reported by the control-plane

  Scenario: Deleting an offline empty node
    Given an offline node with no provisioned resources
    When a node delete command is issued via rest
    Then the node should be deleted from the control-plane
    And the node should not be reported by the control-plane

  Scenario: Deleting an offline non-empty node
    Given an offline node with provisioned resources
    When a node delete command is issued via rest
    Then the node should eventually be removed from the control-plane
    And the node should not be reported by the control-plane

  Scenario: Deleting a non-existent node
    Given a non-existent node
    When a node delete command is issued via rest
    Then the command should fail with error kind "NotFound"
    And the node should not be reported by the control-plane