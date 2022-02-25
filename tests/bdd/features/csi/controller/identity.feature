Feature: Identity gRPC API for CSI Controller

Scenario: get plugin information
    Given a running CSI controller plugin
    When a GetPluginInfo request is sent to CSI controller
    Then CSI controller should report its name and version

Scenario: get plugin capabilities
    Given a running CSI controller plugin
    When a GetPluginCapabilities request is sent to CSI controller
    Then CSI controller should report its capabilities

Scenario: probe CSI controller when REST API endpoint is accessible
    Given a running CSI controller plugin with accessible REST API endpoint
    When a Probe request is sent to CSI controller
    Then CSI controller should report itself as being ready

Scenario: probe CSI controller when REST API endpoint is not accessible
    Given a running CSI controller plugin without REST API server running
    When a Probe request is sent to CSI controller which can not access REST API endpoint
    Then CSI controller should report itself as being not ready
