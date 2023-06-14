Feature: Volume Snapshot listing

  Background:
    Given a deployer cluster

  # ONE-VOLUME - LISTING BY DIFFERENT CRITERIAS
  Scenario Outline: Volume snapshots list
    Given we have a single replica <publish_status> volume
    And we have created more than one snapshot for the volume
    Then the snapshots should be listed by <filter>
    And all the replica snapshots should be reported as online and validated
    Examples:
      | publish_status | filter         |
      | published      | by_none        |
      | published      | by_volume      |
      | published      | by_volume_snap |
      | published      | by_snap        |
      | unpublished    | by_none        |
      | unpublished    | by_volume      |
      | unpublished    | by_volume_snap |
      | unpublished    | by_snap        |

  # ONE-VOLUME - LISTING SNAPSHOTS ON A DISRUPTED NODE
  Scenario Outline: Volume snapshots list involving disrupted node
    Given we have a single replica volume
    And we have created more than one snapshot for the volume
    When we <bring_down> the node hosting the snapshot
    Then the snapshots should be listed
    And all the replica snapshots should be reported as offline and validated
    When we bring up the node hosting the snapshot
    Then all the replica snapshots should be reported as online and validated
    Examples:
      | bring_down |
      | stop       |
      | kill       |

  # TWO-VOLUME - FILTERED PAGINATED LISTING
  Scenario Outline: Paginated Volume snapshots list
    Given we have more than one single replica volumes
    And we have created more than one snapshots for each of the volumes
    Then the snapshots should be listed in a paginated manner by <filter>
    Examples:
      | filter    |
      | by_none   |
      | by_volume |

  # TWO-VOLUME - FILTERED PAGINATED LISTING
  Scenario: Paginated Volume snapshots list more than available
    Given we have more than one single replica volumes
    And we have created more than one snapshots for each of the volumes
    When we try to list max_entries more than available snapshots for a volume
    Then all of the snapshots should be listed for that volume


