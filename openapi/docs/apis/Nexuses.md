# Nexuses

All URIs are relative to *http://localhost/v0*

Method | HTTP request | Description
------------- | ------------- | -------------
[**del_nexus**](Nexuses.md#del_nexus) | **Delete** /nexuses/{nexus_id} | 
[**del_node_nexus**](Nexuses.md#del_node_nexus) | **Delete** /nodes/{node_id}/nexuses/{nexus_id} | 
[**del_node_nexus_share**](Nexuses.md#del_node_nexus_share) | **Delete** /nodes/{node_id}/nexuses/{nexus_id}/share | 
[**get_nexus**](Nexuses.md#get_nexus) | **Get** /nexuses/{nexus_id} | 
[**get_nexuses**](Nexuses.md#get_nexuses) | **Get** /nexuses | 
[**get_node_nexus**](Nexuses.md#get_node_nexus) | **Get** /nodes/{node_id}/nexuses/{nexus_id} | 
[**get_node_nexuses**](Nexuses.md#get_node_nexuses) | **Get** /nodes/{id}/nexuses | 
[**put_node_nexus**](Nexuses.md#put_node_nexus) | **Put** /nodes/{node_id}/nexuses/{nexus_id} | 
[**put_node_nexus_share**](Nexuses.md#put_node_nexus_share) | **Put** /nodes/{node_id}/nexuses/{nexus_id}/share/{protocol} | 



## del_nexus

> del_nexus(nexus_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**nexus_id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## del_node_nexus

> del_node_nexus(node_id, nexus_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node_id** | **String** |  | [required] |
**nexus_id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## del_node_nexus_share

> del_node_nexus_share(node_id, nexus_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node_id** | **String** |  | [required] |
**nexus_id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_nexus

> crate::models::Nexus get_nexus(nexus_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**nexus_id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

[**crate::models::Nexus**](Nexus.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_nexuses

> Vec<crate::models::Nexus> get_nexuses()


### Parameters

This endpoint does not need any parameter.

### Return type

[**Vec<crate::models::Nexus>**](Nexus.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_node_nexus

> crate::models::Nexus get_node_nexus(node_id, nexus_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node_id** | **String** |  | [required] |
**nexus_id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

[**crate::models::Nexus**](Nexus.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_node_nexuses

> Vec<crate::models::Nexus> get_node_nexuses(id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** |  | [required] |

### Return type

[**Vec<crate::models::Nexus>**](Nexus.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## put_node_nexus

> crate::models::Nexus put_node_nexus(node_id, nexus_id, create_nexus_body)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node_id** | **String** |  | [required] |
**nexus_id** | [**uuid::Uuid**](.md) |  | [required] |
**create_nexus_body** | [**CreateNexusBody**](CreateNexusBody.md) |  | [required] |

### Return type

[**crate::models::Nexus**](Nexus.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## put_node_nexus_share

> String put_node_nexus_share(node_id, nexus_id, protocol)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node_id** | **String** |  | [required] |
**nexus_id** | [**uuid::Uuid**](.md) |  | [required] |
**protocol** | [**crate::models::NexusShareProtocol**](.md) |  | [required] |

### Return type

**String**

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

