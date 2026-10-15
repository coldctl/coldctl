//! Schema-v2 immutable pages with explicit commit; no partial report becomes visible.
use coldctl_core::paths::StatePaths;
use serde_json::{Value, json};
use std::time::Duration;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
pub fn collect(paths: &StatePaths, version: &str) -> Result<Vec<Value>> {
    let mut report = serde_json::to_value(crate::client::collect_config(paths, version, false)?)?;
    report["schemaVersion"] = json!(2);
    report["jobs"] = json!(coldctl_core::cloud_lifecycle::jobs(paths)?);
    report["lifecycle"] = json!(coldctl_core::cloud_lifecycle::collect(paths)?);
    split(report)
}
pub fn split(mut report: Value) -> Result<Vec<Value>> {
    let kinds = ["sources", "destinations", "policies", "jobs", "lifecycle"];
    let mut records = Vec::new();
    for kind in kinds {
        let values = report[kind]
            .as_array_mut()
            .ok_or("Missing metadata collection")?;
        records.extend(values.drain(..).map(|v| (kind, v)));
    }
    if records.len() > 102400 {
        return Err("Cloud report exceeds 102,400 metadata records".into());
    }
    let blank = report.clone();
    let mut pages = Vec::new();
    let mut count = 0;
    for (kind, item) in records {
        report[kind].as_array_mut().unwrap().push(item.clone());
        let text = serde_json::to_string(&report)?;
        if count >= 100 || text.len() > 40 * 1024 || text.encode_utf16().count() > 24000 {
            report[kind].as_array_mut().unwrap().pop();
            if count == 0 {
                return Err("Single metadata record exceeds page limit".into());
            }
            pages.push(report);
            report = blank.clone();
            count = 0;
            report[kind].as_array_mut().unwrap().push(item);
            let text = serde_json::to_string(&report)?;
            if text.len() > 40 * 1024 || text.encode_utf16().count() > 24000 {
                return Err("Single metadata record exceeds page limit".into());
            }
        }
        count += 1;
    }
    pages.push(report);
    if pages.len() > 1024 {
        return Err("Cloud report exceeds 1,024 metadata pages".into());
    }
    Ok(pages)
}
async fn post(
    client: &reqwest::Client,
    url: reqwest::Url,
    token: &str,
    body: &Value,
) -> Result<()> {
    let response = client
        .post(url)
        .bearer_auth(token)
        .json(body)
        .send()
        .await
        .map_err(|_| "Cloud connection failed; retry with a fresh report")?;
    match response.status().as_u16() {
        200 => Ok(()),
        401 | 403 => Err("Cloud rejected lifecycle credentials".into()),
        404 => Err("Cloud does not support sync v2; upgrade Cloud before this agent".into()),
        409 => {
            Err("Cloud report conflicted, expired or was superseded; retry a fresh report".into())
        }
        400 | 413 => Err("Cloud rejected metadata contract or size".into()),
        _ => Err("Cloud unavailable; retry with a fresh report".into()),
    }
}
pub async fn sync(base: &str, token: &str, local: bool, pages: Vec<Value>) -> Result<()> {
    let mut url = crate::client::endpoint(base, local)?;
    let first = pages.first().ok_or("No report pages")?;
    let report_id = uuid::Uuid::new_v4().to_string();
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| "Cannot initialize Cloud HTTP client")?;
    url.set_path("/api/agents/sync/v2/cleanup");
    let _ = post(
        &client,
        url.clone(),
        token,
        &json!({"agentId":first["agentId"]}),
    )
    .await;
    url.set_path("/api/agents/sync/v2/pages");
    for (index, page) in pages.iter().enumerate() {
        post(
            &client,
            url.clone(),
            token,
            &json!({"reportId":report_id,"pageIndex":index,"pageCount":pages.len(),"report":page}),
        )
        .await?;
    }
    url.set_path("/api/agents/sync/v2/commit");
    post(&client,url,token,&json!({"agentId":first["agentId"],"reportId":report_id,"pageCount":pages.len(),"capturedAtUnixMs":first["capturedAtUnixMs"]})).await
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_pages_preserve_more_than_two_hundred_records() {
        let records: Vec<_> = (0..1001)
            .map(|i| json!({"id":i,"name":"metadata"}))
            .collect();
        let pages=split(json!({"schemaVersion":2,"sources":records,"destinations":[],"policies":[],"jobs":[],"lifecycle":[]})).unwrap();
        assert_eq!(pages.len(), 11);
        assert_eq!(
            pages
                .iter()
                .map(|p| p["sources"].as_array().unwrap().len())
                .sum::<usize>(),
            1001
        );
        for p in pages {
            assert!(p["sources"].as_array().unwrap().len() <= 100);
        }
    }
    #[test]
    fn oversized_metadata_fails_without_truncating() {
        assert!(split(json!({"sources":[{"name":"x".repeat(50000)}],"destinations":[],"policies":[],"jobs":[],"lifecycle":[]})).is_err());
    }
}
