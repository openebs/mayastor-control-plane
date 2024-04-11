Feature: Create Volume From Snapshot - Parameter Validation

  Background:
    Given a deployer cluster
    And a valid snapshot of a single replica volume
    And a request to create a new volume with the snapshot as its source

  Scenario: Capacity greater than the snapshot
    When the requested capacity is greater than the snapshot
    Then the request should fail with OutOfRange

  Scenario: Capacity smaller than the snapshot
    When the requested capacity is smaller than the snapshot
    Then the request should fail with OutOfRange

  # This may be allowed in the future by inflating the replicas
  Scenario: Thick provisioning
    When the request is for a thick volume
    Then the request should fail with InvalidArguments

  # This may be allowed in the future by setting the volume replica count and performing a rebuild
  Scenario: Multi-replica restore from single replica snapshot
    When the request is for a multi-replica volume
    Then the request should fail with PreconditionFailed
