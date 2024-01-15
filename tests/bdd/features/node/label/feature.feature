Feature: Label a node, this will be used while scheduling replica of volume considering the
        node topology

  Background:
    Given a control plane, two Io-Engine instances, two pools

  Scenario: Label a node
    Given an unlabeled node
    When the user issues a label command with a label to the node
    Then the given node should be labeled with the given label

  Scenario: Label a node when label key already exist and overwrite is false
    Given a labeled node
    When the user attempts to label the same node with the same label key with overwrite as false
    Then the node label should fail with error "AlreadyExists"

  Scenario: Label a node when label key already exist and overwrite is true
    Given a labeled node
    When the user attempts to label the same node with the same label key and overwrite as true
    Then the given node should be labeled with the new given label

  Scenario: Unlabel a node
    Given a labeled node
    When the user issues a unlabel command with a label key present as label for the node
    Then the given node should remove the label with the given key

  Scenario: Unlabel a node when the label key is not present
    Given a labeled node
    When the user issues an unlabel command with a label key that is not currently associated with the node
    Then the unlabel operation for the node should fail with error NotPresent

 # Scenario: Relabel the node with the same key and overwrite as false
   # Given an labeled node
   # When the user issues a label command with a same key to the node
   # Then the labeling of node should fail with error kind "AlreadyExists"

  #Scenario: Relabel the node with the same key and overwrite as true
   # Given an labeled node
   # When the user issues a label command with a same key and overwrite as true to the node
   # Then the given node should be labeled with the given new label
