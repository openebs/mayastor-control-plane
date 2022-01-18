Feature: Volume observability

  Background:
    Given an existing volume

  Scenario: requesting volume information
    When a user issues a GET request for a volume
    Then a volume object representing the volume should be returned
