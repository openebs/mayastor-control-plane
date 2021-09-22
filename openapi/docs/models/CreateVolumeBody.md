# CreateVolumeBody

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**policy** | [**crate::models::VolumePolicy**](VolumePolicy.md) |  | 
**replicas** | **u8** | number of storage replicas | 
**size** | **u64** | size of the volume in bytes | 
**topology** | Option<[**crate::models::Topology**](Topology.md)> |  | [optional]
**labels** | Option<**::std::collections::HashMap<String, String>**> | Optionally used to store custom volume information | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


