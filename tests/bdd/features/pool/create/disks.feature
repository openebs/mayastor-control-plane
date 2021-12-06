Feature: Pool creation
  Test pool creation with different disk related scenarios

  Background:
    Given a control plane and Mayastor instances

  Scenario: creating a pool with a non-existent disk
    When the user attempts to create a pool with a non-existent disk
    Then the pool creation should fail with error kind "Internal"

  Scenario: creating a pool with multiple disks
    When the user attempts to create a pool specifying multiple disks
    Then the pool creation should fail with error kind "InvalidArgument"

  Scenario: creating a pool with an AIO disk
    When the user attempts to create a pool specifying a URI representing an aio disk
    Then the pool should be created successfully

  Scenario: creating a pool with a URING disk
    When the user attempts to create a pool specifying a URI representing a uring disk
    Then the pool should be created successfully
