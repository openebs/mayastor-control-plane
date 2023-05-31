Feature: Snapshot - CSI Controller Operations

    Background:
        Given a running CSI controller plugin

    Scenario: Create Snapshot Operation is implemented
        Given a single replica volume
        When a CreateSnapshotRequest request is sent to the CSI controller
        Then it should succeed

    Scenario: Delete Snapshot Operation is not implemented
        When a DeleteSnapshotRequest request is sent to the CSI controller
        Then the deletion should succeed

    Scenario: List Snapshot Operation is not implemented
        When a ListSnapshotRequest request is sent to the CSI controller
        Then it should fail with status NOT_IMPLEMENTED