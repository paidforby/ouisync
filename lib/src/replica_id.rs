/// Size of replica ID in bytes.
pub const REPLICA_ID_SIZE: usize = 16;

define_random_id! {
    /// Unique id of a replica.
    pub struct ReplicaId([u8; REPLICA_ID_SIZE]);
}

derive_sqlx_traits_for_u8_array_wrapper!(ReplicaId);

impl ReplicaId {
    pub fn starts_with(&self, needle: &[u8]) -> bool {
        self.0.starts_with(needle)
    }
}