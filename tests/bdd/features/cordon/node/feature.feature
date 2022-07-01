Feature: Cordoning

  Background:
    Given Multiple uncordoned nodes

  Scenario: Cordoning a node
    Given an uncordoned node
    When the node is cordoned
    Then new resources of any type cannot be scheduled on the cordoned node

  Scenario: Cordoning a cordoned node
    Given a cordoned node
    When the cordoned node is cordoned
    Then the cordoned node should remain cordoned
    And new resources of any type cannot be scheduled on the cordoned node

  Scenario: Cordoning a node with existing resources
    Given an uncordoned node with resources
    When the node is cordoned
    Then new resources of any type cannot be scheduled on the cordoned node
    And existing resources remain unaffected

  Scenario: Uncordoning a node
    Given a cordoned node
    When the node is uncordoned
    Then new resources of any type can be scheduled on the uncordoned node

  Scenario: Uncordoning an uncordoned node
    Given an uncordoned node
    When the uncordoned node is uncordoned
    Then the uncordoned node should remain uncordoned
    And new resources of any type can be scheduled on the uncordoned node

  Scenario: Uncordoning a node with existing resources
    Given a cordoned node with resources
    When the node is uncordoned
    Then new resources of any type can be scheduled on the uncordoned node

  Scenario: Deleting resources on a cordoned node
    Given a cordoned node with resources
    When the control plane attempts to delete a resource on a cordoned node
    Then the resource should be deleted

  Scenario: Restarting a cordoned node
    Given a cordoned node with resources
    When the cordoned node is restarted
    Then resources that existed on the cordoned node prior to the restart should be recreated on the same cordoned node