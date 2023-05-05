Feature: Snapshot - CSI Controller Capabilities

Background:
    Given a running CSI controller plugin

Scenario: Snapshot CreateDelete capabilities
    When a ControllerGetCapabilities request is sent to the CSI controller
    Then the response capabilities should include CREATE_DELETE_SNAPSHOT

Scenario: Snapshot List capabilities
    When a ControllerGetCapabilities request is sent to the CSI controller
    Then the response capabilities should include LIST_SNAPSHOTS