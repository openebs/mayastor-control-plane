Feature: Thin Provisioning - Volume Creation
  Background:
    Given a control plane, Io-Engine instances and a pool

  # Thick volume creation.
  Scenario: Creating a thick provisioned volume
    Given a request for a tick provisioned volume
    When a volume is successfully created
    Then its replicas are reported to be thick provisioned
    And the pools usage increases by volume size

  # Thick volume space limit.
  Scenario: Creating an oversized thick provisioned volume
    Given a request for a tick provisioned volume
    When the request size exceeds available pools free space
    Then volume creation fails due to insufficient space

  # Thin volume creation.
  Scenario: Creating a thin provisioned volume
    Given a request for a thin provisioned volume
    When a volume is successfully created
    Then its replicas are reported to be thin provisioned
    And the pools usage increases by 4MiB and metadata

  # Over-committed thin volume creation.
  Scenario: Creating an over-committed thin volume
    Given a request for a thin provisioned volume
    When the request size exceeds available pools free space
    Then a volume is successfully created
    And the pools usage increases by 4MiB and metadata