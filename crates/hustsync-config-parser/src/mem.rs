//! Memory sizes such as `256M` or `1.5GiB`; every unit is a power of 1024, as Docker reads `-m`.

use std::str::FromStr;

use parse_size::Config;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(try_from = "String")]
pub struct MemBytes(pub u64);

impl FromStr for MemBytes {
    type Err = parse_size::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Config::new().with_binary().parse_size(s).map(Self)
    }
}

impl TryFrom<String> for MemBytes {
    type Error = parse_size::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

#[cfg(test)]
mod tests {
    use super::MemBytes;

    #[test]
    fn units_are_binary_as_docker_reads_them() {
        for (s, n) in [
            ("32", 32),
            ("32 B", 32),
            ("32k", 32 << 10),
            ("256M", 256 << 20),
            ("256mb", 256 << 20),
            ("128MiB", 128 << 20),
            ("1.5g", 3 << 29),
        ] {
            assert_eq!(s.parse::<MemBytes>().ok(), Some(MemBytes(n)), "{s}");
        }
        assert!("1x".parse::<MemBytes>().is_err());
    }
}
