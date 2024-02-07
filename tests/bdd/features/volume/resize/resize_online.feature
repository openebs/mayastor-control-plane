Feature: Volume resize

    Background:
        Given a deployer cluster

    Scenario Outline: Expand a volume
        Given a published volume with <repl_count> replicas and all are healthy
        When we issue a volume expand request
        Then the request should succeed
        And all the replicas of volume should be resized to new capacity
        And the volume target should get resized to new capacity
        And the volume should be expanded to the new capacity
        And the new capacity should be available for the application
        Examples:
        | repl_count |
        | 1          |
        | 3          |


    Scenario: Expand a volume with an offline replica
        Given a published volume with more than one replicas
        And one of the replica is not in online state
        When we issue a volume expand request
        Then the volume expand status should be failure


    # volume expand should happen without disruption, in parallel to an ongoing
    # replica rebuild.
    Scenario: Expand a published volume with a rebuilding replica
        Given a published volume with more than one replicas
        And the volume is receiving IO
        And one of the replica is undergoing rebuild
        When we issue a volume expand request
        Then the request should succeed
        And all the replicas of volume should be resized to new capacity
        And the volume target should get resized to new capacity
        And the volume should be expanded to the new capacity
        And the new capacity should be available for the application


    Scenario: Expand a new volume created as a snapshot restore
        Given a successful snapshot is created for a published volume
        When a new volume is created with the snapshot as its source
        Then a new replica will be created for the new volume
        And the replica's capacity will be same as the snapshot
        And the new volume is published
        When we issue a volume expand request
        Then the request should succeed
        Then all the replicas of volume should be resized to new capacity
        And the volume should be expanded to the new capacity
        And IO on the new volume runs without error for the complete volume size


    Scenario: Expand a volume and take a snapshot
        Given a published volume with more than one replicas
        When we issue a volume expand request
        Then the request should succeed
        Then all the replicas of volume should be resized to new capacity
        And the volume target should get resized to new capacity
        And the volume should be expanded to the new capacity
        When we take a snapshot of expanded volume
        Then the snapshot should be successfully created
        And the new capacity should be available for the application


    Scenario: Take a snapshot and expand the volume
        Given a successful snapshot is created for a published volume
        When we issue a volume expand request
        Then the request should succeed
        Then all the replicas of volume should be resized to new capacity
        And the volume target should get resized to new capacity
        And the volume should be expanded to the new capacity
        And the new capacity should be available for the application


    # Volume shrink/downsize isn't supported by csi.
    Scenario: Shrink a volume
        Given a published volume with more than one replica and all are healthy
        When we issue a volume shrink request
        Then the volume resize status should be failure
        And the failure reason should be invalid arguments
