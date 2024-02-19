Feature: Volume Node Topology
  The volume topology feature is for selection of particular nodes to schedule a replica on.
  All the volume topology key and values should have an exact match or the keys must match with node labels, although nodes
  can have extra labels, for the selection of that node. The affinity topology label implies the inclusion label 
  for node with both key and value or just keys. The storage class has the labels as
    nodeTopologyAffinity: |
     rack: 1
     zone: a
    which signifies the volume node topology inclusion labels as {"rack": "1", "zone" : "a"}

    nodeHasTopologyKey: |
      rack
      zone    
   which signifies the volume node topology inclusion labels as {"rack": "", "zone" : ""} 

  Background:
    Given a control plane, four Io-Engine instances, twelve pools


# The labels to be applied to the nodes.
################################################################################################################
#         Description                    ||     Node Name          ||    Pool Name   ||     Node Label         ||
#===============================================================================================================
#     "io-engine-1" has                  ||   node1 (io-engine-1)  ||    node1pool1  ||                       ||
#  label "zone-us=us-west-1"             ||   node1 (io-engine-1)  ||    node1pool2  ||   zone-us=us-west-1   ||
#                                        ||   node1 (io-engine-1)  ||    node1pool3  ||                       ||
#===============================================================================================================
#     "io-engine-2" has                  ||   node2 (io-engine-2)  ||    node2pool1  ||                       ||
#  label "zone-ap=ap-south-1"            ||   node2 (io-engine-2)  ||    node2pool2  ||   zone-ap=ap-south-1  ||
#                                        ||   node2 (io-engine-2)  ||    node2pool3  ||                       ||
#===============================================================================================================
#     "io-engine-3" has                  ||   node3 (io-engine-3)  ||    node3pool1  ||                       ||
#  label "zone-eu=eu-west-3 "            ||   node3 (io-engine-3)  ||    node3pool2  ||   zone-eu=eu-west-3   ||
#                                        ||   node3 (io-engine-3)  ||    node3pool3  ||                       ||
#===============================================================================================================
#     "io-engine-4" has                  ||   node4 (io-engine-4)  ||    node4pool1  ||   zone-us=us-west-1   ||
#  label "zone-us=us-west-1,             ||   node4 (io-engine-4)  ||    node4pool2  ||   zone-ap=ap-south-1  ||
# zone-ap=ap-south-1,zone-eu=eu-west-3"  ||   node4 (io-engine-4)  ||    node4pool3  ||   zone-eu=eu-west-3   ||
#===============================================================================================================


  Scenario Outline: Suitable nodes which contain volume topology labels
    Given a request for a <replica> replica volume with nodeAffinityTopologyLabel as <node_affinity_topology_label> and node topology inclusion as <volume_node_topology_inclusion_label>
    When the desired number of replica of volume i.e. <replica> here; is <expression> number of the nodes containing the label <volume_node_topology_inclusion_label>
    Then the <replica> replica volume creation should <result> and <provisioned> provisioned on pools with its corresponding node labels <node_label>
    Examples:
      | node_affinity_topology_label | volume_node_topology_inclusion_label     | replica | expression  | result    | provisioned |  node_label         |
      |            True              |           zone-us=us-west-1              |    1    |      <=     | succeed   |  must be    | zone-us=us-west-1   |
      |            True              |           zone-us=us-west-1              |    2    |      <=     | succeed   |  must be    | zone-us=us-west-1   |
      |            True              |           zone-us=us-west-1              |    3    |      >      | fail      |  not        | zone-us=us-west-1   |
      |            True              |           zone-ap=ap-south-1             |    1    |      <=     | succeed   |  must be    | zone-ap=ap-south-1  |
      |            True              |           zone-ap=ap-south-1             |    2    |      <=     | succeed   |  must be    | zone-ap=ap-south-1  |
      |            True              |           zone-ap=ap-south-1             |    3    |      >      | fail      |  not        | zone-ap=ap-south-1  |
      |            True              |           zone-eu=eu-west-3              |    1    |      <=     | succeed   |  must be    | zone-eu=eu-west-3   |
      |            True              |           zone-eu=eu-west-3              |    2    |      <=     | succeed   |  must be    | zone-eu=eu-west-3   |
      |            True              |           zone-eu=eu-west-3              |    3    |      >      | fail      |  not        | zone-eu=eu-west-3   |


  Scenario Outline: Suitable nodes which contain volume node topology keys only
    Given a request for a <replica> replica volume with nodeHasTopologyKey as <has_topology_key> and node topology inclusion as <volume_node_topology_inclusion_label>
    When the desired number of replica of volume i.e. <replica> here; is <expression> number of the nodes containing the label <volume_node_topology_inclusion_label>
    Then the <replica> replica volume creation should <result> and <provisioned> provisioned on pools with its corresponding node labels <node_label>
    Examples:
      |       has_topology_key       | volume_node_topology_inclusion_label   | replica |  expression | result    | provisioned |  node_label         |
      |            True              |           zone-us                      |    1    |     <=      | succeed   |  must be    | zone-us=us-west-1   |
      |            True              |           zone-us                      |    2    |     <=      | succeed   |  must be    | zone-us=us-west-1   |  
      |            True              |           zone-us                      |    3    |      >      |   fail    |    not      | zone-us=us-west-1   |
      |            True              |           zone-ap                      |    1    |     <=      | succeed   |  must be    | zone-ap=ap-south-1  |
      |            True              |           zone-ap                      |    2    |     <=      | succeed   |  must be    | zone-ap=ap-south-1  |
      |            True              |           zone-ap                      |    3    |      >      |   fail    |  not        | zone-ap=ap-south-1  |
      |            True              |           zone-eu                      |    1    |     <=      | succeed   |  must be    | zone-eu=eu-west-3   |
      |            True              |           zone-eu                      |    2    |     <=      | succeed   |  must be    | zone-eu=eu-west-3   |
      |            True              |           zone-eu                      |    3    |      >      |   fail    |    not      | zone-eu=eu-west-3   |
