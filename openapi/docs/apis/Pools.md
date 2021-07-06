# Pools

All URIs are relative to *http://localhost/v0*

Method | HTTP request | Description
------------- | ------------- | -------------
[**del_node_pool**](Pools.md#del_node_pool) | **Delete** /nodes/{node_id}/pools/{pool_id} | 
[**del_pool**](Pools.md#del_pool) | **Delete** /pools/{pool_id} | 
[**get_node_pool**](Pools.md#get_node_pool) | **Get** /nodes/{node_id}/pools/{pool_id} | 
[**get_node_pools**](Pools.md#get_node_pools) | **Get** /nodes/{id}/pools | 
[**get_pool**](Pools.md#get_pool) | **Get** /pools/{pool_id} | 
[**get_pools**](Pools.md#get_pools) | **Get** /pools | 
[**put_node_pool**](Pools.md#put_node_pool) | **Put** /nodes/{node_id}/pools/{pool_id} | 



## del_node_pool

> del_node_pool(node_id, pool_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node_id** | **String** |  | [required] |
**pool_id** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## del_pool

> del_pool(pool_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**pool_id** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_node_pool

> crate::models::Pool get_node_pool(node_id, pool_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node_id** | **String** |  | [required] |
**pool_id** | **String** |  | [required] |

### Return type

[**crate::models::Pool**](Pool.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_node_pools

> Vec<crate::models::Pool> get_node_pools(id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** |  | [required] |

### Return type

[**Vec<crate::models::Pool>**](Pool.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_pool

> crate::models::Pool get_pool(pool_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**pool_id** | **String** |  | [required] |

### Return type

[**crate::models::Pool**](Pool.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_pools

> Vec<crate::models::Pool> get_pools()


### Parameters

This endpoint does not need any parameter.

### Return type

[**Vec<crate::models::Pool>**](Pool.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## put_node_pool

> crate::models::Pool put_node_pool(node_id, pool_id, create_pool_body)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node_id** | **String** |  | [required] |
**pool_id** | **String** |  | [required] |
**create_pool_body** | [**CreatePoolBody**](CreatePoolBody.md) |  | [required] |

### Return type

[**crate::models::Pool**](Pool.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

