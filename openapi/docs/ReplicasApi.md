# \ReplicasApi

All URIs are relative to *http://localhost/v0*

Method | HTTP request | Description
------------- | ------------- | -------------
[**del_node_pool_replica**](ReplicasApi.md#del_node_pool_replica) | **Delete** /nodes/{node_id}/pools/{pool_id}/replicas/{replica_id} | 
[**del_node_pool_replica_share**](ReplicasApi.md#del_node_pool_replica_share) | **Delete** /nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}/share | 
[**del_pool_replica**](ReplicasApi.md#del_pool_replica) | **Delete** /pools/{pool_id}/replicas/{replica_id} | 
[**del_pool_replica_share**](ReplicasApi.md#del_pool_replica_share) | **Delete** /pools/{pool_id}/replicas/{replica_id}/share | 
[**get_node_pool_replica**](ReplicasApi.md#get_node_pool_replica) | **Get** /nodes/{node_id}/pools/{pool_id}/replicas/{replica_id} | 
[**get_node_pool_replicas**](ReplicasApi.md#get_node_pool_replicas) | **Get** /nodes/{node_id}/pools/{pool_id}/replicas | 
[**get_node_replicas**](ReplicasApi.md#get_node_replicas) | **Get** /nodes/{id}/replicas | 
[**get_replica**](ReplicasApi.md#get_replica) | **Get** /replicas/{id} | 
[**get_replicas**](ReplicasApi.md#get_replicas) | **Get** /replicas | 
[**put_node_pool_replica**](ReplicasApi.md#put_node_pool_replica) | **Put** /nodes/{node_id}/pools/{pool_id}/replicas/{replica_id} | 
[**put_node_pool_replica_share**](ReplicasApi.md#put_node_pool_replica_share) | **Put** /nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}/share/{protocol} | 
[**put_pool_replica**](ReplicasApi.md#put_pool_replica) | **Put** /pools/{pool_id}/replicas/{replica_id} | 
[**put_pool_replica_share**](ReplicasApi.md#put_pool_replica_share) | **Put** /pools/{pool_id}/replicas/{replica_id}/share/{protocol} | 



## del_node_pool_replica

> del_node_pool_replica(node_id, pool_id, replica_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node_id** | **String** |  | [required] |
**pool_id** | **String** |  | [required] |
**replica_id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## del_node_pool_replica_share

> del_node_pool_replica_share(node_id, pool_id, replica_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node_id** | **String** |  | [required] |
**pool_id** | **String** |  | [required] |
**replica_id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## del_pool_replica

> del_pool_replica(pool_id, replica_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**pool_id** | **String** |  | [required] |
**replica_id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## del_pool_replica_share

> del_pool_replica_share(pool_id, replica_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**pool_id** | **String** |  | [required] |
**replica_id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_node_pool_replica

> crate::models::Replica get_node_pool_replica(node_id, pool_id, replica_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node_id** | **String** |  | [required] |
**pool_id** | **String** |  | [required] |
**replica_id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

[**crate::models::Replica**](Replica.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_node_pool_replicas

> Vec<crate::models::Replica> get_node_pool_replicas(node_id, pool_id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node_id** | **String** |  | [required] |
**pool_id** | **String** |  | [required] |

### Return type

[**Vec<crate::models::Replica>**](Replica.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_node_replicas

> Vec<crate::models::Replica> get_node_replicas(id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** |  | [required] |

### Return type

[**Vec<crate::models::Replica>**](Replica.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_replica

> crate::models::Replica get_replica(id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | [**uuid::Uuid**](.md) |  | [required] |

### Return type

[**crate::models::Replica**](Replica.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_replicas

> Vec<crate::models::Replica> get_replicas()


### Parameters

This endpoint does not need any parameter.

### Return type

[**Vec<crate::models::Replica>**](Replica.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## put_node_pool_replica

> crate::models::Replica put_node_pool_replica(node_id, pool_id, replica_id, create_replica_body)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node_id** | **String** |  | [required] |
**pool_id** | **String** |  | [required] |
**replica_id** | [**uuid::Uuid**](.md) |  | [required] |
**create_replica_body** | [**CreateReplicaBody**](CreateReplicaBody.md) |  | [required] |

### Return type

[**crate::models::Replica**](Replica.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## put_node_pool_replica_share

> String put_node_pool_replica_share(node_id, pool_id, replica_id, protocol)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**node_id** | **String** |  | [required] |
**pool_id** | **String** |  | [required] |
**replica_id** | [**uuid::Uuid**](.md) |  | [required] |
**protocol** | [**crate::models::ReplicaShareProtocol**](.md) |  | [required] |

### Return type

**String**

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## put_pool_replica

> crate::models::Replica put_pool_replica(pool_id, replica_id, create_replica_body)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**pool_id** | **String** |  | [required] |
**replica_id** | [**uuid::Uuid**](.md) |  | [required] |
**create_replica_body** | [**CreateReplicaBody**](CreateReplicaBody.md) |  | [required] |

### Return type

[**crate::models::Replica**](Replica.md)

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## put_pool_replica_share

> String put_pool_replica_share(pool_id, replica_id, protocol)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**pool_id** | **String** |  | [required] |
**replica_id** | [**uuid::Uuid**](.md) |  | [required] |
**protocol** | [**crate::models::ReplicaShareProtocol**](.md) |  | [required] |

### Return type

**String**

### Authorization

[JWT](../README.md#JWT)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

