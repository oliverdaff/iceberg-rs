use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    static ref S3A: Regex = Regex::new("s3a://\\w*/").unwrap();
    static ref S3: Regex = Regex::new("s3://\\w*/").unwrap();
    static ref GS: Regex = Regex::new("gs://\\w*/").unwrap();
}

pub fn strip_prefix(path: &str) -> String {
    if path.starts_with("s3a://") {
        S3A.replace(path, "").to_string()
    } else if path.starts_with("s3://") {
        S3.replace(path, "").to_string()
    } else if path.starts_with("gs://") {
        GS.replace(path, "").to_string()
    } else {
        path.to_owned()
    }
}
