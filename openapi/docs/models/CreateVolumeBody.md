# CreateVolumeBody

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**policy** | [**crate::models::VolumeHealPolicy**](VolumeHealPolicy.md) | Volume Healing policy used to determine if and how to replace a replica | 
**replicas** | **i32** | number of storage replicas | 
**size** | **i64** | size of the volume in bytes | 
**topology** | [**crate::models::Topology**](Topology.md) | Volume topology used to determine how to place/distribute the data.  Should either be labelled or explicit, not both.  If neither is used then the control plane will select from all available resources. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


