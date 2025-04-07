Feature: Encryption-at-Rest on Mayastor DiskPool

  Background:
    Given the product is installed and running

  Scenario Outline: Creating and Importing an encrypted DiskPool
    Given a user created Secret file containing key parameters with <cipher> and <keysize>
    When a diskpool is created with this Secret file
    And the node hosting the pool reboots
    Then the encrypted disk pool gets imported successfully eventually
    Examples:
      | cipher  | keysize |
      | AesXts  | 128     |

  Scenario Outline: Volume replica scheduling
    Given a user created Secret file containing key parameters with <cipher> and <keysize>
    When a diskpool is created with this Secret file
    Then the encrypted disk pool gets created successfully
    When a diskpool is created without encryption
    Then the non encrypted disk pool gets created successfully
    When a single replica volume is created with encryption
    Then the replica for encrypted volume should be on encrypted pool
    When a single replica volume is created without encryption
    Then the replica for non encrypted volume should be on non encrypted pool
    Examples:
      | cipher  | keysize |
      | AesXts  | 128     |
