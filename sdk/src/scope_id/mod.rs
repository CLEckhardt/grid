
struct FullyQualifiedServiceId {}

pub trait ScopeId {}

pub struct GlobalScopeId {}
impl ScopeId for GlobalScopeId {}

pub struct ServiceScopeId {
    service_id: FullyQualifiedServiceId,
}
impl ScopeId for ServiceScopeId {}

