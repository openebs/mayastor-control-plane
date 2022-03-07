Feature: CSI Node plugin parameters

  Background:
    Given a mayastor cluster

  Scenario Outline: stage volume request with a specified nvme nr io queues
    Given a csi node plugin with <io> IO queues configured
    When staging a volume
    Then the nvme device should report <total> TOTAL queues
    Examples:
      | io | total |
      | 1  |   2   |
      | 4  |   5   |
      | 20 |  21   |