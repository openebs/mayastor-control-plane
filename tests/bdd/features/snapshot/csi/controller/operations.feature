Feature: Snapshot - CSI Controller Operations

    Background:
        Given a running CSI controller plugin

    Scenario: Create Snapshot Operation is implemented
        Given we have a single replica volume
        When a CreateSnapshotRequest request is sent to the CSI controller
        Then the creation should succeed

    Scenario: Delete Snapshot Operation is implemented
        Given we have a single replica volume
        And a snapshot is created for that volume
        When a DeleteSnapshotRequest request is sent to the CSI controller
        Then the deletion should succeed

    Scenario: List Snapshot Operation is implemented
        Given we have a single replica volume
        And a snapshot is created for that volume
        When a ListSnapshotRequest request is sent to the CSI controller
        Then the list should succeed

    Scenario: Create Volume Operation with snapshot source
        Given we have a single replica volume
        And a snapshot is created for that volume
        When a CreateVolumeRequest request with snapshot as source is sent to the CSI controller
        Then the volume creation should succeed
