use rpc::csi::{
    plugin_capability::{
        service::Type as PluginServiceType,
        volume_expansion::Type as PluginVolExpansionType,
        Service,
        Type::{Service as ServiceCapability, VolumeExpansion as VolExpansionCapability},
        VolumeExpansion,
    },
    PluginCapability,
};

/// This returns the exhaustive set of capabilities for this CSI driver.
pub fn plugin_capabilities() -> Vec<PluginCapability> {
    let service_capabilities = &[
        PluginServiceType::ControllerService,
        PluginServiceType::VolumeAccessibilityConstraints,
    ];

    let expansion_capabilities = &[
        PluginVolExpansionType::Offline,
        PluginVolExpansionType::Online,
    ];

    let mut capabilities: Vec<PluginCapability> = Vec::new();
    capabilities.extend(service_capabilities.iter().map(|c| PluginCapability {
        r#type: Some(ServiceCapability(Service { r#type: *c as i32 })),
    }));
    capabilities.extend(expansion_capabilities.iter().map(|c| PluginCapability {
        r#type: Some(VolExpansionCapability(VolumeExpansion {
            r#type: *c as i32,
        })),
    }));

    capabilities
}
