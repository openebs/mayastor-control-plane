# VolumeHealPolicy

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**self_heal** | **bool** | the server will attempt to heal the volume by itself  the client should not attempt to do the same if this is enabled | 
**topology** | Option<[**crate::models::Topology**](Topology.md)> | topology to choose a replacement replica for self healing  (overrides the initial creation topology) | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


