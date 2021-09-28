# VolumeSpec

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**labels** | Option<**::std::collections::HashMap<String, String>**> | Optionally used to store custom volume information | [optional]
**num_replicas** | **u8** | Number of children the volume should have. | 
**operation** | Option<[**crate::models::VolumeSpecOperation**](VolumeSpec_operation.md)> |  | [optional]
**size** | **u64** | Size that the volume should be. | 
**status** | [**crate::models::SpecStatus**](SpecStatus.md) |  | 
**target** | Option<[**crate::models::VolumeTarget**](VolumeTarget.md)> |  | [optional]
**uuid** | [**uuid::Uuid**](uuid::Uuid.md) | Volume Id | 
**topology** | Option<[**crate::models::Topology**](Topology.md)> |  | [optional]
**policy** | [**crate::models::VolumePolicy**](VolumePolicy.md) |  | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


