Feature: Volume sharing and publishing

  Background:
    Given an existing volume

  Scenario: unpublish a published volume
    Given a published volume
    Then unpublishing the volume should succeed

  Scenario: unpublish an already unpublished volume
    Given an unpublished volume
    Then unpublishing the volume should return an already unpublished error
