# BlockDevice

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**available** | **bool** | identifies if device is available for use (ie. is not \"currently\" in  use) | 
**devlinks** | **Vec<String>** | list of udev generated symlinks by which device may be identified | 
**devmajor** | **i32** | major device number | 
**devminor** | **i32** | minor device number | 
**devname** | **String** | entry in /dev associated with device | 
**devpath** | **String** | official device path | 
**devtype** | **String** | currently \"disk\" or \"partition\" | 
**filesystem** | [**crate::models::BlockDeviceFilesystem**](BlockDevice_filesystem.md) |  | 
**model** | **String** | device model - useful for identifying mayastor devices | 
**partition** | [**crate::models::BlockDevicePartition**](BlockDevice_partition.md) |  | 
**size** | **i64** | size of device in (512 byte) blocks | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


