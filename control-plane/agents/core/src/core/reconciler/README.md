# Replica HotSpare Reconciliation

Volumes can be created with a replica count greater than 1 which is intended to make them highly available. If one of
the replicas fails, then the volume remains available by making use of the remaining healthy replicas.

## Why do we need to replace faulted replicas

In such a scenario where 1 of the replicas fails, the control plane tries to add another replica to replace the faulted
one. This way, it can reestablish the previous level of redundancy and thus becoming robust to another potential fault.

**Volume created with 2 replicas**
```
         ┌────────┐
         │ Volume │
         │        │
       ┌─┴────────┴─┐
       │            │
   ┌───▼───┐    ┌───▼───┐
   │Replica│    │Replica│
   │   1   │    │   2   │
   └───────┘    └───────┘
```
If `Replica1` fails the volume remains accessible through `Replica2`. If `Replica 2` also fails, then the volume becomes
inaccessible!

**Volume created with 2 replicas**
```
         ┌────────┐
         │ Volume │
         │        │
       ┌─┴────────┴─┐
       │            │
   ┌───▼───┐    ┌───▼───┐
   │Replica│    │Replica│
   │   1   │    │   2   │
   └───────┘    └───────┘
```
If `Replica1` fails the volume remains accessible through `Replica2`.

The hotspare logic replaces `Replica1` with a brand-new replica `Replica3`. A rebuild is now running as we must rebuild
the user data from `Replica2` into `Replica3`.

Before the rebuild completes, `Replica 2` fails. The volume becomes inaccessible!

**Volume created with 2 replicas**
```
         ┌────────┐
         │ Volume │
         │        │
       ┌─┴────────┴─┐
       │            │
   ┌───▼───┐    ┌───▼───┐
   │Replica│    │Replica│
   │   1   │    │   2   │
   └───────┘    └───────┘
```
If `Replica1` fails the volume remains accessible through `Replica2`.

The hotspare logic replaces `Replica1` with a brand-new replica `Replica3`. A rebuild is now running as we must rebuild
the user data from `Replica2` into `Replica3`. The rebuild completes successfully.
`Replica 2` fails.

The volume is still accessible through `Replica 3`.
#
It's clear now, that we need to replace faulted replicas with new ones, and we need to rebuild them before we're able
to sustain further failures.
#


## Scenarios

### Scenario One

```gherkin
Scenario: Replacing faulty replicas
  Given a degraded volume
  When a nexus state has faulty children
  Then they should eventually be removed from the state and spec
  And the replicas should eventually be destroyed
```

#### The reconciliation loop example:

#
1. finds a degraded volume
2. finds the nexus with a faulty child
3. removes the faulty child
4. disowns the replica from the volume
5. deletes the replica (if accessible, otherwise it'll be garbage collected)
#

### Scenario Two

```gherkin
Scenario: Replacing unknown replicas
  Given a volume
  When a nexus state has children that are not present in the spec
  Then the children should eventually be removed from the state
  And the uri's should not be destroyed
```

#### The reconciliation loop example:

#
1. finds a volume
2. finds the nexus with an unknown child
3. removes the unknown child
#

### Scenario Three

```gherkin
Scenario: Replacing missing replicas
  Given a degraded volume
  When a nexus spec has children that are not present in the state
  Then the children should eventually be removed from the spec
  And the replicas should eventually be destroyed
```

#### The reconciliation loop example:

#
1. finds a degraded volume
2. finds the nexus with a missing replica
3. forgets about the missing replica (might have been removed for a specific reason?)
4. disowns the missing replica
5. deletes the replica (if accessible, otherwise it'll be garbage collected)
#

### Scenario Four

```gherkin
Scenario: Nexus is out of sync with its volume
  Given a degraded volume
  When the nexus spec has a different number of children to the number of volume replicas
  Then the nexus spec should eventually have as many children as the number of volume replicas
```

#### The reconciliation loop examples:

#
1. finds a degraded volume
2. finds the nexus out of sync, with more replicas than required
3. removes excess replicas from the nexus
#
1. finds a degraded volume
2. finds a 1 replica nexus for a 2 replica volume
3. finds an unused volume replica
4. adds the unused replica to the nexus
#

### Scenario Five

```gherkin
Scenario: Number of volume replicas out of sync with replica requirements
  Given a degraded volume
  When the number of created volume replicas is different to the required number of replicas
  Then the number of created volume replicas should eventually match the required number of replicas
```

#### The reconciliation loop examples:

#
1. finds a degraded volume
2. finds a degraded volume missing 1 replica
3. creates a new replica
#
1. finds a degraded volume
2. finds a degraded volume with 1 extra replica
3. finds an unused volume replica and deletes it
#
1. finds a degraded volume
2. finds a degraded volume with 1 extra replica
3. tries to find an unused volume replica (can't find it)
4. finds a nexus with 1 more replica than required
5. removes the replica from its nexus
6. finds a degraded volume with 1 extra replica
7. finds an unused volume replica and deletes it
#