//! Pushes metric groups to each `[[metrics.push]]` target on its own interval.

use std::time::{Duration, SystemTime};

use reqwest::{
    Client,
    header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue},
};
use tokio::{
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};

use super::{Families, Group, PushConfig, Res, groups};

/// A checked `[[metrics.push]]` entry.
#[derive(Debug, Clone)]
pub struct Target<G> {
    url: String,
    every: Duration,
    groups: Vec<G>,
    headers: HeaderMap,
}

impl<G: Group> Target<G> {
    pub fn new(cfg: &PushConfig) -> Result<Self, String> {
        if cfg.interval == 0 {
            return Err(format!(
                "metrics push to {}: interval must be positive",
                cfg.url
            ));
        }
        let headers = cfg
            .headers
            .iter()
            .map(|(k, v)| {
                Ok((
                    HeaderName::try_from(k.as_str()).map_err(|e| format!("header `{k}`: {e}"))?,
                    HeaderValue::try_from(v.as_str()).map_err(|e| format!("header `{k}`: {e}"))?,
                ))
            })
            .collect::<Result<_, String>>()?;
        Ok(Self {
            url: cfg.url.clone(),
            every: Duration::from_secs(cfg.interval),
            groups: cfg
                .groups
                .as_deref()
                .map_or_else(|| Ok(G::ALL.to_vec()), groups)?,
            headers,
        })
    }

    /// Checks every entry, so a bad one stops the service at start-up.
    pub fn all(cfgs: &[PushConfig]) -> Result<Vec<Self>, String> {
        cfgs.iter().map(Self::new).collect()
    }

    async fn run<F, Fut>(self, client: Client, gather: F)
    where
        F: Fn(G) -> Fut + Sync,
        Fut: Future<Output = Res<Families>> + Send,
    {
        let mut tick = interval(self.every);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut ok = true;
        loop {
            tick.tick().await;
            match self.once(&client, &gather).await {
                Ok(()) if !ok => {
                    ok = true;
                    tracing::info!("metrics push to {} works again", self.url);
                }
                Err(e) if ok => {
                    ok = false;
                    tracing::warn!("metrics push to {} failed: {e}", self.url);
                }
                _ => {}
            }
        }
    }

    async fn once<F, Fut>(&self, client: &Client, gather: &F) -> Res<()>
    where
        F: Fn(G) -> Fut + Sync,
        Fut: Future<Output = Res<Families>> + Send,
    {
        let mut fams = Vec::with_capacity(self.groups.len());
        for &g in &self.groups {
            fams.push(gather(g).await?);
        }
        client
            .post(&self.url)
            .headers(self.headers.clone())
            .header(CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(super::influx::encode(&fams, SystemTime::now()))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

/// Starts one task per target; each gathers its groups through `gather` and posts them.
pub fn spawn<G, F, Fut>(targets: Vec<Target<G>>, client: &Client, gather: F) -> Vec<JoinHandle<()>>
where
    G: Group,
    F: Fn(G) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Res<Families>> + Send + 'static,
{
    targets
        .into_iter()
        .map(|t| tokio::spawn(t.run(client.clone(), gather.clone())))
        .collect()
}
