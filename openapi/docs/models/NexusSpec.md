# NexusSpec

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**children** | **Vec<String>** | List of children. | 
**managed** | **bool** | Managed by our control plane | 
**node** | **String** | Node where the nexus should live. | 
**operation** | Option<[**crate::models::NexusSpecOperation**](NexusSpec_operation.md)> |  | [optional]
**owner** | Option<[**uuid::Uuid**](uuid::Uuid.md)> | Volume which owns this nexus, if any | [optional]
**share** | [**crate::models::Protocol**](Protocol.md) |  | 
**size** | **i64** | Size of the nexus. | 
**state** | [**crate::models::SpecState**](SpecState.md) |  | 
**uuid** | [**uuid::Uuid**](uuid::Uuid.md) | Nexus Id | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


