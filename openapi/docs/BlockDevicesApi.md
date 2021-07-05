# \BlockDevicesApi

All URIs are relative to *http://localhost/v0*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_node_block_devices**](BlockDevicesApi.md#get_node_block_devices) | **Get** /nodes/{node}/block_devices | 



## get_node_block_devices

> Vec<crate::models::BlockDevice> get_node_block_devices(node, all)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node** | **String** |  | [required] |
**all** | Option<**bool**> | specifies whether to list all devices or only usable ones |  |

### Return type

[**Vec<crate::models::BlockDevice>**](BlockDevice.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

