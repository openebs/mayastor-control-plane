Feature: Volume publishing

  Background:
    Given an existing volume

  Scenario: publish an unpublished volume
    Given an unpublished volume
    Then publishing the volume should succeed with a returned volume object containing the share URI


  Scenario: share/publish an already shared/published volume
    Given a published volume
    Then publishing the volume should return an already published error
