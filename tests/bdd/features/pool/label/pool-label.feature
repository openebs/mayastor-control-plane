Feature: Label a pool, this will be used while scheduling replica of volume considering the
        pool topology

  Background:
    Given a control plane, two Io-Engine instances, two pools

  Scenario: Label a pool
    Given an unlabeled pool
    When the user issues a label command with a label to the pool
    Then the given pool should be labeled with the given label

  Scenario: Label a pool when label key already exist and overwrite is false
    Given a labeled pool
    When the user attempts to label the same pool with the same label key with overwrite as false
    Then the pool label should fail with error "PRECONDITION_FAILED"

  Scenario: Label a pool when label key already exist and overwrite is true
    Given a labeled pool
    When the user attempts to label the same pool with the same label key and overwrite as true
    Then the given pool should be labeled with the new given label

  Scenario: Unlabel a pool
    Given a labeled pool
    When the user issues a unlabel command with a label key present as label for the pool
    Then the given pool should remove the label with the given key

  Scenario: Unlabel a pool when the label key is not present
    Given a labeled pool
    When the user issues an unlabel command with a label key that is not currently associated with the pool
    Then the unlabel operation for the pool should fail with error PRECONDITION_FAILED