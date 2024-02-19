Feature: Volume Pool Topology
  The volume topology feature is for selection of particular pools to schedule a replica on.
  All the volume topology key and values should have an exact match  or the keys must match with pool labels, although pools
  can have extra labels, for the selection of that pool. The affinity topology label implies the inclusion label 
  for pool with both key and value or just keys. The storage class has the labels as
    poolTopologyAffinity: |
     rack: 1
     zone: a
    which signifies the volume pool topology inclusion labels as {"rack": "1", "zone" : "a"}

    poolHasTopologyKey: |
      rack
      zone   
   which signifies the volume pool topology inclusion labels as {"rack": "", "zone" : ""} 

  Background:
    Given a control plane, three Io-Engine instances, nine pools


# The labels to be applied to the pools.
###############################################################################################
#   Description            ||   Pool Name   ||         Label           ||        Node        ||
#==============================================================================================
#     "pool1" has          ||   node1pool1  ||     zone-us=us-west-1   ||     io-engine-1    || 
#       the label          ||   node2pool1  ||     zone-us=us-west-1   ||     io-engine-2    ||
#   "zone-us=us-west-1"    ||   node3pool1  ||     zone-us=us-west-1   ||     io-engine-3    ||
#==============================================================================================
#     "pool2" has          ||   node1pool2  ||     zone-ap=ap-south-1  ||     io-engine-1    ||
#      the label           ||   node2pool2  ||     zone-ap=ap-south-1  ||     io-engine-2    ||
#   "zone-ap=ap-south-1"   ||   node3pool2  ||     zone-ap=ap-south-1  ||     io-engine-3    ||
#==============================================================================================
#     "pool3" has          ||   node1pool3  ||     zone-eu=eu-west-3   ||     io-engine-1    ||
#      the label           ||   node2pool3  ||     zone-eu=eu-west-3   ||     io-engine-2    ||
#   "zone-eu=eu-west-3"    ||   node3pool3  ||     zone-eu=eu-west-3   ||     io-engine-3    ||
#==============================================================================================

  Scenario Outline: Suitable pools which contain volume topology labels
    Given a request for a <replica> replica volume with poolAffinityTopologyLabel as <pool_affinity_topology_label> and pool topology inclusion as <volume_pool_topology_inclusion_label>
    When the desired number of replica of volume i.e. <replica> here; is <expression> number of the pools containing the label <volume_pool_topology_inclusion_label>
    Then the <replica> replica volume creation should <result> and <provisioned> provisioned on pools with labels <pool_label>
    Examples:
      | pool_affinity_topology_label | volume_pool_topology_inclusion_label  | replica |     expression     | result    | provisioned |     pool_label      |
      |            True              |           zone-us=us-west-1           |    1    |        <=          | succeed   |  must be    | zone-us=us-west-1   |
      |            True              |           zone-us=us-west-1           |    2    |        <=          | succeed   |  must be    | zone-us=us-west-1   |         
      |            True              |           zone-us=us-west-1           |    3    |        <=          | succeed   |  must be    | zone-us=us-west-1   |
      |            True              |           zone-us=us-west-1           |    4    |        >           |   fail    |    not      | zone-us=us-west-1   |
      |            True              |           zone-ap=ap-south-1          |    1    |        <=          | succeed   |  must be    | zone-ap=ap-south-1  |
      |            True              |           zone-ap=ap-south-1          |    2    |        <=          | succeed   |  must be    | zone-ap=ap-south-1  |
      |            True              |           zone-ap=ap-south-1          |    3    |        <=          | succeed   |  must be    | zone-ap=ap-south-1  |
      |            True              |           zone-ap=ap-south-1          |    4    |        >           |   fail    |  not        | zone-ap=ap-south-1  |
      |            True              |           zone-eu=eu-west-3           |    1    |        <=          | succeed   |  must be    | zone-eu=eu-west-3   |
      |            True              |           zone-eu=eu-west-3           |    2    |        <=          | succeed   |  must be    | zone-eu=eu-west-3   |
      |            True              |           zone-eu=eu-west-3           |    3    |        <=          | succeed   |  must be    | zone-eu=eu-west-3   |
      |            True              |           zone-eu=eu-west-3           |    4    |        >           |   fail    |    not      | zone-eu=eu-west-3   |



  Scenario Outline: Suitable pools which contain volume topology keys only
    Given a request for a <replica> replica volume with poolHasTopologyKey as <has_topology_key> and pool topology inclusion as <volume_pool_topology_inclusion_label>
    When the desired number of replica of volume i.e. <replica> here; is <expression> number of the pools containing the label <volume_pool_topology_inclusion_label>
    Then the <replica> replica volume creation should <result> and <provisioned> provisioned on pools with labels <pool_label>
    Examples:
      |       has_topology_key       | volume_pool_topology_inclusion_label   | replica |     expression     | result    | provisioned |  pool_label    |
      |            True              |           zone-us                      |    1    |        <=          | succeed   |  must be    | zone-us=us-west-1   |
      |            True              |           zone-us                      |    2    |        <=          | succeed   |  must be    | zone-us=us-west-1   |         
      |            True              |           zone-us                      |    3    |        <=          | succeed   |  must be    | zone-us=us-west-1   |
      |            True              |           zone-us                      |    4    |        >           |   fail    |    not      | zone-us=us-west-1   |
      |            True              |           zone-ap                      |    1    |        <=          | succeed   |  must be    | zone-ap=ap-south-1  |
      |            True              |           zone-ap                      |    2    |        <=          | succeed   |  must be    | zone-ap=ap-south-1  |
      |            True              |           zone-ap                      |    3    |        <=          | succeed   |  must be    | zone-ap=ap-south-1  |
      |            True              |           zone-ap                      |    4    |        >           |   fail    |  not        | zone-ap=ap-south-1  |
      |            True              |           zone-eu                      |    1    |        <=          | succeed   |  must be    | zone-eu=eu-west-3   |
      |            True              |           zone-eu                      |    2    |        <=          | succeed   |  must be    | zone-eu=eu-west-3   |
      |            True              |           zone-eu                      |    3    |        <=          | succeed   |  must be    | zone-eu=eu-west-3   |
      |            True              |           zone-eu                      |    4    |        >           |   fail    |    not      | zone-eu=eu-west-3   |
