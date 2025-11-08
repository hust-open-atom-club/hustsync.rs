use regex::Regex;
use reqwest::Certificate;
use reqwest::blocking::{Client, Response};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;
use std::process::ExitStatus;
use std::time::Duration;

fn rsync_exit_values_map() -> HashMap<i32, &'static str> {
    let mut m = HashMap::new();
    m.insert(0, "Success");
    m.insert(1, "Syntax or usage error");
    m.insert(2, "Protocol incompatibility");
    m.insert(3, "Errors selecting input/output files, dirs");
    m.insert(
        4,
        "Requested action not supported: an attempt was made to manipulate 64-bit files on a platform that cannot support them; or an option was specified that is supported by the client and not by the server.",
    );
    m.insert(5, "Error starting client-server protocol");
    m.insert(6, "Daemon unable to append to log-file");
    m.insert(10, "Error in socket I/O");
    m.insert(11, "Error in file I/O");
    m.insert(12, "Error in rsync protocol data stream");
    m.insert(13, "Errors with program diagnostics");
    m.insert(14, "Error in IPC code");
    m.insert(20, "Received SIGUSR1 or SIGINT");
    m.insert(21, "Some error returned by waitpid()");
    m.insert(22, "Error allocating core memory buffers");
    m.insert(23, "Partial transfer due to error");
    m.insert(24, "Partial transfer due to vanished source files");
    m.insert(25, "The --max-delete limit stopped deletions");
    m.insert(30, "Timeout in data send/receive");
    m.insert(35, "Timeout waiting for daemon connection");
    m
}

pub fn get_tls_certificate<P: AsRef<Path>>(ca_file: P) -> io::Result<Certificate> {
    let pem = fs::read(ca_file)?;
    Certificate::from_pem(&pem).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

pub fn create_http_client(ca_file: Option<&str>) -> Result<Client, Box<dyn std::error::Error>> {
    let mut builder = Client::builder()
        .timeout(Duration::from_secs(5))
        .pool_idle_timeout(Duration::from_secs(20));

    if let Some(path) = ca_file {
        let cert = get_tls_certificate(path)?;
        builder = builder.add_root_certificate(cert);
    }

    Ok(builder.build()?)
}

pub fn post_json<T: Serialize>(
    url: &str,
    obj: &T,
    client: Option<&Client>,
) -> Result<Response, Box<dyn std::error::Error>> {
    let c = match client {
        Some(c) => c,
        None => &create_http_client(None)?,
    };

    let resp = c
        .post(url)
        .header("Content-Type", "application/json; charset=utf-8")
        .json(obj)
        .send()?;
    Ok(resp)
}

pub fn get_json<T: DeserializeOwned>(
    url: &str,
    client: Option<&Client>,
) -> Result<T, Box<dyn std::error::Error>> {
    let c = match client {
        Some(c) => c,
        None => &create_http_client(None)?,
    };

    let resp = c.get(url).send()?;
    if !resp.status().is_success() {
        return Err(format!("HTTP status code is not 200: {}", resp.status()).into());
    }
    let parsed = resp.json::<T>()?;
    Ok(parsed)
}

pub fn find_all_submatch_in_file(
    file_name: &str,
    re: &Regex,
) -> Result<Vec<Vec<Vec<u8>>>, Box<dyn std::error::Error>> {
    if file_name == "/dev/null" {
        return Err("invalid log file".into());
    }
    let content = fs::read(file_name)?;
    let content_str = String::from_utf8_lossy(&content);
    let mut matches: Vec<Vec<Vec<u8>>> = Vec::new();

    for caps in re.captures_iter(&content_str) {
        let mut groups: Vec<Vec<u8>> = Vec::new();
        for i in 1..caps.len() {
            if let Some(m) = caps.get(i) {
                groups.push(m.as_str().as_bytes().to_vec());
            } else {
                groups.push(Vec::new());
            }
        }
        matches.push(groups);
    }
    Ok(matches)
}

pub fn extract_size_from_log(log_file: &str, re: &Regex) -> String {
    let matches = match find_all_submatch_in_file(log_file, re) {
        Ok(m) => m,
        Err(_) => return String::new(),
    };

    if matches.is_empty() {
        return String::new();
    }

    if let Some(last) = matches.last()
        && !last.is_empty()
        && let Ok(s) = String::from_utf8(last[0].clone())
    {
        return s;
    }
    
    String::new()
}

pub fn extract_size_from_rsync_log(log_file: &str) -> Result<String, Box<dyn std::error::Error>> {
    let re = Regex::new(r"(?m)^Total file size: ([0-9\.]+[KMGTP]?) bytes")?;
    Ok(extract_size_from_log(log_file, &re))
}

pub fn translate_rsync_exit_status(status: &ExitStatus) -> (Option<i32>, Option<String>) {
    if let Some(code) = status.code() {
        let map = rsync_exit_values_map();
        if let Some(&msg) = map.get(&code) {
            return (Some(code), Some(format!("rsync error: {}", msg)));
        }
        return (Some(code), None);
    }
    (None, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::path::PathBuf;
    use std::process;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn write_temp_file(content: &str) -> PathBuf {
        let fname = format!(
            "hustsync_test_{}_{}.log",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let path = env::temp_dir().join(fname);
        fs::write(&path, content.as_bytes()).unwrap();
        path
    }

    #[test]
    fn test_extract_size_from_rsync_log_basic() {
        let real_log = r#"
Number of files: 998,470 (reg: 925,484, dir: 58,892, link: 14,094)
Number of created files: 1,049 (reg: 1,049)
Number of deleted files: 1,277 (reg: 1,277)
Number of regular files transferred: 5,694
Total file size: 1.33T bytes
Total transferred file size: 2.86G bytes
Literal data: 780.62M bytes
Matched data: 2.08G bytes
File list size: 37.55M
File list generation time: 7.845 seconds
File list transfer time: 0.000 seconds
Total bytes sent: 7.55M
Total bytes received: 823.25M

sent 7.55M bytes  received 823.25M bytes  5.11M bytes/sec
total size is 1.33T  speedup is 1,604.11
"#;
        let path = write_temp_file(real_log);
        let res = extract_size_from_rsync_log(path.to_str().unwrap()).unwrap();
        let _ = fs::remove_file(&path);
        assert_eq!(res, "1.33T");
    }

    #[test]
    fn test_extract_size_from_rsync_log_multiple_matches_uses_last() {
        let log = r#"
Total file size: 123M bytes
some other lines
Total file size: 2.5G bytes
"#;
        let path = write_temp_file(log);
        let res = extract_size_from_rsync_log(path.to_str().unwrap()).unwrap();
        let _ = fs::remove_file(&path);
        assert_eq!(res, "2.5G");
    }

    #[test]
    fn test_extract_size_from_rsync_log_no_match_returns_empty() {
        let log = r#"
This log does not contain the expected line.
Total transferred file size: 99M bytes
"#;
        let path = write_temp_file(log);
        let res = extract_size_from_rsync_log(path.to_str().unwrap()).unwrap();
        let _ = fs::remove_file(&path);
        assert_eq!(res, "");
    }

    #[test]
    fn test_extract_size_from_rsync_log_dev_null_returns_empty() {
        let res = extract_size_from_rsync_log("/dev/null").unwrap();
        assert_eq!(res, "");
    }
}
