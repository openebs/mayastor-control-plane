# Nexus

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**children** | [**Vec<crate::models::Child>**](Child.md) | Array of Nexus Children | 
**device_uri** | **String** | URI of the device for the volume (missing if not published).  Missing property and empty string are treated the same. | 
**node** | **String** | id of the mayastor instance | 
**rebuilds** | **i32** | total number of rebuild tasks | 
**share** | [**crate::models::Protocol**](Protocol.md) |  | 
**size** | **i64** | size of the volume in bytes | 
**state** | [**crate::models::NexusState**](NexusState.md) |  | 
**uuid** | [**uuid::Uuid**](uuid::Uuid.md) | uuid of the nexus | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


