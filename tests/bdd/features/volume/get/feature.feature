Feature: Issuing GET requests for volumes

  Background:
    Given five existing volumes

  Scenario: issuing a GET request without pagination
    Given a number of volumes greater than one
    When a user issues a GET request without pagination
    Then all of the volumes should be returned
    And the next token should not be returned

  Scenario: issuing a GET request with pagination variables within range
    Given a number of volumes greater than one
    When a user issues a GET request with pagination
    Then only a subset of the volumes should be returned
    And the number of returned volumes should equal the paginated request
    And the next token should be returned

  Scenario: issuing a GET request with pagination max entries larger than number of volumes
    Given a number of volumes greater than one
    When a user issues a GET request with max entries greater than the number of volumes
    Then all of the volumes should be returned
    And the next token should not be returned

  Scenario: issuing a GET request with pagination starting token set to less than the number of volumes
    Given a number of volumes greater than one
    When a user issues a GET request with the starting token set to a value less than the number of volumes
    Then the first result back should be for the given starting token offset
    And the next token should be returned

  Scenario: issuing a GET request with pagination starting token set to greater than the number of volumes
    Given a number of volumes greater than one
    When a user issues a GET request with the starting token set to a value greater than the number of volumes
    Then an empty list should be returned
    And the next token should not be returned