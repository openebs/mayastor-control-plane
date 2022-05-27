Feature: Pool creation
  Test pool creation with different name related scenarios

  Background:
    Given a control plane and Io-Engine instances

  Scenario: creating a pool with a valid name
    When the user attempts to create a pool with a valid name "p0"
    Then the pool creation should succeed

  Scenario: creating a pool whose name is already used by an existing pool on the same node
    Given a pool named "p0"
    When the user attempts to create another pool with the same name on the same node
    Then the pool creation should fail with error "AlreadyExists"

  Scenario: creating a pool whose name is already used by an existing pool on a different node
    Given a pool named "p0"
    When the user attempts to create another pool with the same name on a different node
    Then the pool creation should fail with error "AlreadyExists"

  Scenario: recreating a pool with a different name and the same disk
    Given a pool named "p0"
    When the user attempts to recreate the pool using a different name "p1"
    Then the pool creation should fail with error "InvalidArgument"

  Scenario: recreating an unexported pool with a different name and the same disk
    Given an unexported pool named "p0"
    When the user attempts to recreate the pool using a different name "p1"
    Then the pool creation should fail with error "InvalidArgument"
