use super::{peer_addr::PeerAddr, peer_source::PeerSource, peer_state::PeerState};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};

/// Information about a peer.
#[derive(Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct PeerInfo {
    #[serde(with = "as_str")]
    pub addr: PeerAddr,
    pub source: PeerSource,
    pub state: PeerState,
}

impl PeerInfo {
    pub(super) fn new(addr: PeerAddr, source: PeerSource, state: PeerState) -> Self {
        Self {
            addr,
            source,
            state,
        }
    }
}

mod as_str {
    use super::*;

    pub fn serialize<S>(value: &PeerAddr, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.to_string().serialize(s)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<PeerAddr, D::Error>
    where
        D: Deserializer<'de>,
    {
        <&str>::deserialize(d)?.parse().map_err(D::Error::custom)
    }
}
