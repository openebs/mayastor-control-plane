# ReplicaSpec

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**managed** | **bool** | Managed by our control plane | 
**operation** | Option<[**crate::models::ReplicaSpecOperation**](ReplicaSpec_operation.md)> |  | [optional]
**owners** | [**crate::models::ReplicaSpecOwners**](ReplicaSpec_owners.md) |  | 
**pool** | **String** | The pool that the replica should live on. | 
**share** | [**crate::models::Protocol**](Protocol.md) |  | 
**size** | **i64** | The size that the replica should be. | 
**state** | [**crate::models::SpecState**](SpecState.md) |  | 
**thin** | **bool** | Thin provisioning. | 
**uuid** | [**uuid::Uuid**](uuid::Uuid.md) | uuid of the replica | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


