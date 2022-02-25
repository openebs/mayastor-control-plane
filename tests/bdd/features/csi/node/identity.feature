Feature: Identity gRPC API for CSI Node

Scenario: get plugin information
    Given a running CSI node plugin
    When a GetPluginInfo request is sent to CSI node
    Then CSI node should report its name and version

Scenario: get plugin capabilities
    Given a running CSI node plugin
    When a GetPluginCapabilities request is sent to CSI node
    Then CSI node should report its capabilities
