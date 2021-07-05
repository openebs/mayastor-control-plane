# \VolumesApi

All URIs are relative to *http://localhost/v0*

Method | HTTP request | Description
------------- | ------------- | -------------
[**del_share**](VolumesApi.md#del_share) | **Delete** /volumes{volume_id}/share | 
[**del_volume**](VolumesApi.md#del_volume) | **Delete** /volumes/{volume_id} | 
[**get_node_volume**](VolumesApi.md#get_node_volume) | **Get** /nodes/{node_id}/volumes/{volume_id} | 
[**get_node_volumes**](VolumesApi.md#get_node_volumes) | **Get** /nodes/{node_id}/volumes | 
[**get_volume**](VolumesApi.md#get_volume) | **Get** /volumes/{volume_id} | 
[**get_volumes**](VolumesApi.md#get_volumes) | **Get** /volumes | 
[**put_volume**](VolumesApi.md#put_volume) | **Put** /volumes/{volume_id} | 
[**put_volume_share**](VolumesApi.md#put_volume_share) | **Put** /volumes/{volume_id}/share/{protocol} | 



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

