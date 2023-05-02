Feature: Anti-Affinity for Volume Group

  Background:
    Given a deployer cluster

  Scenario: volume group target distribution
    Given two pools, one on node A and another on node B
    And one unpublished volume group volume with 2 replica
    And one volume group volume with 2 replica published on node A
    And one non volume group volume with 2 replica published on node B
    When the non published volume is published
    Then the publish should not fail
    And the target should land on node B

  Scenario: volume group volume undergoes republish
    Given two pools, one on node A and another on node B
    And two published volumes with two replicas each
    And the volume targets are on different nodes
    When one volume is republished
    Then the republish should not fail
    And both the targets should be on the same node
