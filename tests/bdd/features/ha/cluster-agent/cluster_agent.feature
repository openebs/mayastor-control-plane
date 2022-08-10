Feature: gRPC API for cluster-agent

Background:
    Given a running cluster agent


Scenario: register node-agent
    When the RegisterNodeAgent request is sent to cluster-agent
    Then the request should be completed successfully

Scenario: register node-agent with empty nodeAgent details
    When the RegisterNodeAgent request is sent to cluster-agent with empty data
    Then the request should be failed


