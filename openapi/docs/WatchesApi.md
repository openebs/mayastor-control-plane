# \WatchesApi

All URIs are relative to *http://localhost/v0*

Method | HTTP request | Description
------------- | ------------- | -------------
[**del_watch_volume**](WatchesApi.md#del_watch_volume) | **Delete** /watches/volumes/{volume_id} | 
[**get_watch_volume**](WatchesApi.md#get_watch_volume) | **Get** /watches/volumes/{volume_id} | 
[**put_watch_volume**](WatchesApi.md#put_watch_volume) | **Put** /watches/volumes/{volume_id} | 



## del_watch_volume

> del_watch_volume(volume_id, callback)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**volume_id** | [**uuid::Uuid**](.md) |  | [required] |
**callback** | **url::Url** | URL callback | [required] |

### Return type

 (empty response body)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_watch_volume

> Vec<crate::models::RestWatch> get_watch_volume(volume_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**volume_id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

[**Vec<crate::models::RestWatch>**](RestWatch.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## put_watch_volume

> put_watch_volume(volume_id, callback)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**volume_id** | [**uuid::Uuid**](.md) |  | [required] |
**callback** | **url::Url** | URL callback | [required] |

### Return type

 (empty response body)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

