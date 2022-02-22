Feature: Managing a Volume Nexus Target

  Background:
    Given a control plane, two Mayastor instances, two pools

  Scenario: the target nexus is faulted
    Given a published self-healing volume
    When its nexus target is faulted
    And one or more of its healthy replicas are back online
    Then the nexus target shall be removed from its associated node
    And it shall be recreated

