# \JsonGrpcApi

All URIs are relative to *http://localhost/v0*

Method | HTTP request | Description
------------- | ------------- | -------------
[**put_node_jsongrpc**](JsonGrpcApi.md#put_node_jsongrpc) | **Put** /nodes/{node}/jsongrpc/{method} | 



## put_node_jsongrpc

> serde_json::Value put_node_jsongrpc(node, method, body)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node** | **String** |  | [required] |
**method** | **String** |  | [required] |
**body** | **serde_json::Value** |  | [required] |

### Return type

[**serde_json::Value**](serde_json::Value.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

