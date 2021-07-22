# Volumes

All URIs are relative to *http://localhost/v0*

Method | HTTP request | Description
------------- | ------------- | -------------
[**del_share**](Volumes.md#del_share) | **Delete** /volumes{volume_id}/share | 
[**del_volume**](Volumes.md#del_volume) | **Delete** /volumes/{volume_id} | 
[**del_volume_target**](Volumes.md#del_volume_target) | **Delete** /volumes/{volume_id}/target | 
[**get_node_volume**](Volumes.md#get_node_volume) | **Get** /nodes/{node_id}/volumes/{volume_id} | 
[**get_node_volumes**](Volumes.md#get_node_volumes) | **Get** /nodes/{node_id}/volumes | 
[**get_volume**](Volumes.md#get_volume) | **Get** /volumes/{volume_id} | 
[**get_volumes**](Volumes.md#get_volumes) | **Get** /volumes | 
[**put_volume**](Volumes.md#put_volume) | **Put** /volumes/{volume_id} | 
[**put_volume_replica_count**](Volumes.md#put_volume_replica_count) | **Put** /volumes/{volume_id}/replica_count/{replica_count} | 
[**put_volume_share**](Volumes.md#put_volume_share) | **Put** /volumes/{volume_id}/share/{protocol} | 
[**put_volume_target**](Volumes.md#put_volume_target) | **Put** /volumes/{volume_id}/target | 



## del_share

> del_share(volume_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**volume_id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## del_volume

> del_volume(volume_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**volume_id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## del_volume_target

> crate::models::Volume del_volume_target(volume_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**volume_id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

[**crate::models::Volume**](Volume.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_node_volume

> crate::models::Volume get_node_volume(node_id, volume_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node_id** | **String** |  | [required] |
**volume_id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

[**crate::models::Volume**](Volume.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_node_volumes

> Vec<crate::models::Volume> get_node_volumes(node_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node_id** | **String** |  | [required] |

### Return type

[**Vec<crate::models::Volume>**](Volume.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_volume

> crate::models::Volume get_volume(volume_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**volume_id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

[**crate::models::Volume**](Volume.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_volumes

> Vec<crate::models::Volume> get_volumes()


### Parameters

This endpoint does not need any parameter.

### Return type

[**Vec<crate::models::Volume>**](Volume.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## put_volume

> crate::models::Volume put_volume(volume_id, create_volume_body)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**volume_id** | [**uuid::Uuid**](.md) |  | [required] |
**create_volume_body** | [**CreateVolumeBody**](CreateVolumeBody.md) |  | [required] |

### Return type

[**crate::models::Volume**](Volume.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## put_volume_replica_count

> crate::models::Volume put_volume_replica_count(volume_id, replica_count)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**volume_id** | [**uuid::Uuid**](.md) |  | [required] |
**replica_count** | **u8** |  | [required] |

### Return type

[**crate::models::Volume**](Volume.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## put_volume_share

> String put_volume_share(volume_id, protocol)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**volume_id** | [**uuid::Uuid**](.md) |  | [required] |
**protocol** | [**crate::models::VolumeShareProtocol**](.md) |  | [required] |

### Return type

**String**

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## put_volume_target

> crate::models::Volume put_volume_target(volume_id, node, protocol)


Create a volume target connectable for front-end IO from the specified node. Due to a limitation, this must currently be a mayastor storage node.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**volume_id** | [**uuid::Uuid**](.md) |  | [required] |
**node** | **String** | The node where the front-end workload resides. If the workload moves then the volume must be republished. | [required] |
**protocol** | [**crate::models::VolumeShareProtocol**](.md) | The protocol used to connect to the front-end node. | [required] |

### Return type

[**crate::models::Volume**](Volume.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

