Feature: Cordoning

  Background:
    Given Multiple uncordoned nodes

  Scenario: Cordoning a node
    Given an uncordoned node
    When the user issues a cordon command with a label to the node
    Then the command will succeed
    And new resources deployable by the control-plane cannot be scheduled on the cordoned node

  Scenario: Cordoning a cordoned node
    Given a cordoned node
    When the user issues a cordon command to the node with a new label
    Then the command will succeed
    And the cordoned node should remain cordoned
    And new resources deployable by the control-plane cannot be scheduled on the cordoned node

  Scenario: Cordoning a node with existing resources
    Given an uncordoned node with resources
    When the user issues a cordon command with a label to the node
    Then the command will succeed
    And new resources deployable by the control-plane cannot be scheduled on the cordoned node
    And existing resources remain unaffected

  Scenario: Uncordoning a node
    Given a cordoned node
    When the user issues an uncordon command with a label to the node
    Then the command will succeed
    And new resources deployable by the control-plane can be scheduled on the uncordoned node

  Scenario: Uncordoning an uncordoned node
    Given an uncordoned node
    When the user issues an uncordon command with a label to the node
    Then the command will fail
    And the uncordoned node should remain uncordoned
    And new resources deployable by the control-plane can be scheduled on the uncordoned node

  Scenario: Uncordoning a node with existing resources
    Given a cordoned node with resources
    When the user issues an uncordon command with a label to the node
    Then the command will succeed
    And new resources deployable by the control-plane can be scheduled on the uncordoned node

  Scenario: Deleting resources on a cordoned node
    Given a cordoned node with resources
    When the control plane attempts to delete resources on a cordoned node
    Then the resources should be deleted

  Scenario: Restarting a cordoned node
    Given a cordoned node with resources
    When the cordoned node is restarted
    Then resources that existed on the cordoned node prior to the restart should be recreated on the same cordoned node

  Scenario: Cordoning a node with an existing label
    Given a cordoned node
    When the user issues a cordon command with the same label to the node
    Then the command will succeed
    And the cordoned node should remain cordoned

  Scenario: Uncordoning a node with an unknown label
    Given a cordoned node
    When the user issues an uncordon command with an unknown cordon label to the node
    Then the command will fail
    And the cordoned node should remain cordoned

  Scenario: Unschedulable replicas due to node cordon
    Given a published volume with multiple replicas
    And a cordoned node
    When the volume becomes degraded
    And there are insufficient uncordoned nodes to accommodate new replicas
    Then the volume will remain in a degraded state

  Scenario: Node should be cordoned if there is at least one cordon applied
    Given a cordoned node with multiple cordons
    When the user issues an uncordon command with a label to the node
    Then the command will succeed
    And new resources deployable by the control-plane cannot be scheduled on the cordoned node

  Scenario: Node should be uncordoned when all cordons have been removed
    Given a cordoned node with multiple cordons
    When all cordons have been removed
    Then the node should be uncordoned
    And new resources deployable by the control-plane can be scheduled on the uncordoned node

  Scenario: Nexus placement on a cluster with one or more cordoned nodes
    Given one or more cordoned nodes
    When a volume is created and published
    Then the volume target node selected will not be a cordoned node
