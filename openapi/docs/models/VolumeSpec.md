# VolumeSpec

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**labels** | **Vec<String>** | Volume labels. | 
**num_paths** | **i32** | Number of front-end paths. | 
**num_replicas** | **i32** | Number of children the volume should have. | 
**operation** | Option<[**crate::models::VolumeSpecOperation**](VolumeSpec_operation.md)> |  | [optional]
**protocol** | [**crate::models::Protocol**](Protocol.md) |  | 
**size** | **i64** | Size that the volume should be. | 
**state** | [**crate::models::SpecState**](SpecState.md) |  | 
**target_node** | Option<**String**> | The node where front-end IO will be sent to | [optional]
**uuid** | [**uuid::Uuid**](uuid::Uuid.md) | Volume Id | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


