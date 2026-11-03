use crate::model::*;
use crate::storage::ZSet;
use feldera_rest_api::types::*;
use feldera_rest_api::{Client, ClientInfo, RetryPolicy};
use feldera_types::config::*;
use feldera_types::transport::http::Chunk;
use futures_util::stream::LocalBoxStream;
use futures_util::StreamExt;
use std::fs;
use tokio::runtime::Runtime;
use tokio::time;

const PIPELINE_NAME: &str = "pipeline01";

pub struct FelderaEngine {
    rt: Runtime,
    client: Client,
    body: LocalBoxStream<'static, Result<Vec<u8>, Error>>,
    buf: Vec<u8>,
    view: ZSet<JoinRow>,
    processed_steps: i64,
}

impl FelderaEngine {
    pub fn new(sql_path: &str) -> Result<Self, Error> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("shouldn't have been set before");

        let client = Client::new("http://127.0.0.1:8080", RetryPolicy::none());

        let found = rt.block_on(async {
            match client
                .get_pipeline()
                .pipeline_name(PIPELINE_NAME)
                .selector(PipelineFieldSelector::Status)
                .send()
                .await
            {
                Ok(_response) => Ok(true),
                Err(err) => match err.status() {
                    Some(status) => Ok(status != 404),
                    None => Err("Unexpected error"),
                },
            }
        })?;

        if found {
            rt.block_on(async {
                println!("Trying to stop the pipeline");
                match client
                    .post_pipeline_stop()
                    .pipeline_name(PIPELINE_NAME)
                    .force(true)
                    .send()
                    .await
                {
                    Ok(_response) => {
                        let mut interval = time::interval(time::Duration::from_secs(1));
                        loop {
                            interval.tick().await;
                            match client
                                .get_pipeline()
                                .pipeline_name(PIPELINE_NAME)
                                .selector(PipelineFieldSelector::Status)
                                .send()
                                .await
                            {
                                Ok(response) => {
                                    if response.deployment_status == CombinedStatus::Stopped {
                                        break;
                                    }
                                }
                                Err(err) => {
                                    println!("{}", err)
                                }
                            }
                        }
                    }
                    Err(err) => {
                        println!("{}", err)
                    }
                }
            });

            rt.block_on(async {
                println!("Trying to clear the pipeline storage");
                match client
                    .post_pipeline_clear()
                    .pipeline_name(PIPELINE_NAME)
                    .send()
                    .await
                {
                    Ok(_response) => {
                        let mut interval = time::interval(time::Duration::from_secs(1));
                        loop {
                            interval.tick().await;
                            match client
                                .get_pipeline()
                                .pipeline_name(PIPELINE_NAME)
                                .selector(PipelineFieldSelector::Status)
                                .send()
                                .await
                            {
                                Ok(response) => {
                                    if response.storage_status == StorageStatus::Cleared {
                                        break;
                                    }
                                }
                                Err(err) => {
                                    println!("{}", err)
                                }
                            }
                        }
                    }
                    Err(err) => {
                        println!("{}", err)
                    }
                }
            });

            println!("Trying to delete the pipeline");
            rt.block_on(async {
                client
                    .delete_pipeline()
                    .pipeline_name(PIPELINE_NAME)
                    .send()
                    .await
            })?;
        }

        let program_code: String = fs::read_to_string(sql_path)?;

        let create_pipeline = rt.block_on(async {
            client
                .post_pipeline()
                .body(
                    PostPutPipeline::builder()
                        .name(PIPELINE_NAME)
                        .program_code(program_code)
                        .runtime_config(RuntimeConfig {
                            storage: None,
                            ..Default::default()
                        }),
                )
                .send()
                .await
        })?;
        println!("Pipeline created with status {}", create_pipeline.status());

        println!("Trying to start the pipeline");
        rt.block_on(async {
            match client
                .post_pipeline_start()
                .pipeline_name(PIPELINE_NAME)
                .send()
                .await
            {
                Ok(_response) => {
                    let mut interval = time::interval(time::Duration::from_secs(1));
                    loop {
                        interval.tick().await;
                        match client
                            .get_pipeline()
                            .pipeline_name(PIPELINE_NAME)
                            .selector(PipelineFieldSelector::Status)
                            .send()
                            .await
                        {
                            Ok(response) => {
                                println!("Current pipeline status: {}", response.deployment_status);
                                if response.deployment_status == CombinedStatus::Running {
                                    break;
                                }
                            }
                            Err(err) => {
                                println!("{}", err)
                            }
                        }
                    }
                    Ok(())
                }
                Err(err) => Err(err),
            }
        })?;

        let egress_url = format!(
            "{}/v0/pipelines/{}/egress/v1?format=json&backpressure=true",
            client.baseurl(),
            PIPELINE_NAME
        );
        let body = rt
            .block_on(async { client.client().post(egress_url).send().await })?
            .bytes_stream()
            .map(|bytes| Ok(bytes?.to_vec()))
            .boxed_local();

        Ok(Self {
            rt,
            client,
            body,
            buf: Vec::new(),
            view: ZSet::default(),
            processed_steps: -1,
        })
    }

    fn ingest(&self, table_name: &str, body: String) -> Result<i64, Error> {
        self.rt.block_on(async {
            let token = self
                .client
                .http_input()
                .pipeline_name(PIPELINE_NAME)
                .table_name(table_name)
                .force(false)
                .format("json")
                .body(body)
                .send()
                .await?
                .token
                .clone();
            loop {
                let status = self
                    .client
                    .completion_status()
                    .pipeline_name(PIPELINE_NAME)
                    .token(&token)
                    .send()
                    .await?;
                if let Some(step) = status.step {
                    return Ok(step);
                }
                time::sleep(time::Duration::from_millis(10)).await;
            }
        })
    }

    fn read_until(&mut self, step: i64) -> Result<(), Error> {
        let Self { rt, body, buf, view, processed_steps, .. } = self;
        while *processed_steps < step {
            buf.extend_from_slice(&rt.block_on(body.next()).ok_or("egress closed")??);
            let consumed = {
                let mut chunks = serde_json::Deserializer::from_slice(buf).into_iter::<Chunk>();
                loop {
                    match chunks.next() {
                        Some(Ok(chunk)) => {
                            if let Some(json_data) = chunk.json_data {
                                for event in serde_json::from_value::<Vec<DataChangeEvent<JoinRow>>>(json_data)? {
                                    match event {
                                        DataChangeEvent::Insert(row) => view.insert(row),
                                        DataChangeEvent::Delete(row) => view.delete(row),
                                    }
                                }
                            }
                            if let Some(steps) = chunk.processed_steps {
                                *processed_steps = steps as i64;
                            }
                        }
                        Some(Err(err)) if err.is_eof() => break,
                        Some(Err(err)) => return Err(err.into()),
                        None => break,
                    }
                }
                chunks.byte_offset()
            };
            buf.drain(..consumed);
        }
        Ok(())
    }
}

impl Engine for FelderaEngine {
    fn update(&mut self, batch: &[Change]) -> Result<(), Error> {
        let mut step = -1;
        for change in batch {
            step = step.max(match change {
                Change::T1(event) => self.ingest("t1", serde_json::to_string(event)?)?,
                Change::T2(event) => self.ingest("t2", serde_json::to_string(event)?)?,
            });
        }
        self.read_until(step)
    }

    fn view(&self) -> ZSet<JoinRow> {
        self.view.clone()
    }
}
