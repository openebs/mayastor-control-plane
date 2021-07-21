# Replica

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**node** | **String** | id of the mayastor instance | 
**pool** | **String** | id of the pool | 
**share** | [**crate::models::Protocol**](Protocol.md) |  | 
**size** | **u64** | size of the replica in bytes | 
**state** | [**crate::models::ReplicaState**](ReplicaState.md) |  | 
**thin** | **bool** | thin provisioning | 
**uri** | **String** | uri usable by nexus to access it | 
**uuid** | [**uuid::Uuid**](uuid::Uuid.md) | uuid of the replica | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


