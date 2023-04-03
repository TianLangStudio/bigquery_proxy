use actix_web::{web, App, Error, HttpResponse, HttpServer};
use futures::future::FutureExt;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::Client;
use std::sync::Arc;

use actix_web::web::resource;
use serde_json::{self, json, Value};

use log::{debug, info};
pub const JSON_KEY: &str = "key";
pub const JSON_KEY_DATA: &str = "data";
pub const JSON_KEY_ERR: &str = "err";

async fn bigquery_query(
    project_id: web::Path<String>,
    client: web::Data<Arc<Client>>,
    body: web::Bytes,
) -> Result<HttpResponse, Error> {
    // body is loaded, now we can deserialize json-rust
    info!("project_id:{}", project_id);
    let query_json: Value = serde_json::from_str(std::str::from_utf8(&body)?)
        .map_err(|e| actix_web::error::ErrorPreconditionFailed(e.to_string()))?; // return Result
    let query_json_map: &serde_json::Map<String, Value> =
        query_json
            .as_object()
            .ok_or(actix_web::error::ErrorPreconditionFailed(format!(
                "not a valid json object:{}",
                query_json
            )))?;
    let mut query_jobs = vec![];

    let query_json_iter: serde_json::map::Iter = query_json_map.iter();

    for (ek, ev) in query_json_iter {
        info!("query key: {}, sql: {}", ek, ev);
        let sql = ev
            .as_str()
            .ok_or(actix_web::error::ErrorPreconditionFailed(format!(
                "query {} must be a valid sql",
                ek
            )))?;
        let query_job = client
            .job()
            .query(&project_id, QueryRequest::new(sql))
            .map(|r| match r {
                Ok(mut result_set) => {
                    let mut data_vec = vec![];
                    let col_names = result_set.column_names();
                    while result_set.next_row() {
                        let mut json_object = serde_json::Map::new();
                        debug!("result_set:{:?}", result_set);
                        for col_name in &col_names {
                            let col_value: Option<Value> =
                                result_set.get_json_value_by_name(&col_name).unwrap();
                            debug!("{} -> {:?}", col_name, col_value);
                            let col_value: Value = col_value.unwrap_or(Value::Null);
                            debug!("{} -> {}", col_name, col_value);
                            json_object.insert(col_name.to_string(), col_value);
                        }
                        debug!("json_object:{:?}", json_object);
                        data_vec.push(json_object);
                    }
                    (ek.to_string(), Some(data_vec), None)
                }
                Err(e) => (ek.to_string(), None, Some(e.to_string())),
            });
        query_jobs.push(query_job);
    }
    let job_total = query_jobs.len();
    info!("query job total:{}", job_total);
    let mut query_result_map = serde_json::Map::new();
    if job_total > 0 {
        use futures::future::join_all;
        let query_results = join_all(query_jobs).await;
        for query_result in query_results {
            //query_result_json.push(query_result).unwrap();

            let key = &query_result.0;
            let query_data = &query_result.1;
            let query_err = &query_result.2;
            let mut resp = serde_json::Map::new();
            if query_err.is_some() {
                resp.insert(JSON_KEY_ERR.to_string(), json!(query_err));
            } else {
                resp.insert(JSON_KEY_DATA.to_string(), json!(query_data));
            };
            query_result_map.insert(key.to_string(), json!(resp));
        }
    }
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(json!(query_result_map).to_string()))
}
async fn create_bq_client() -> Arc<Client> {
    let gcp_sa_key = "D:\\work\\googlecloud\\secret-primacy-382307-8c8032cdb8cc.json";
    info!("creating client ...");
    let client = Client::from_service_account_key_file(gcp_sa_key)
        .await
        .expect("create bigquery client error");
    Arc::new(client)
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    info!("begin init log");
    log4rs::init_file("conf/log4rs.yml", Default::default()).unwrap();
    info!("initiated log");
    info!("begin create bigquery client");
    let client = create_bq_client().await;
    info!("created bigquery client client");

    HttpServer::new(move || {
        let cors = actix_cors::Cors::default()
            .allow_any_origin() //Watch out: allow any origin is not security maybe.
            .allow_any_method();

        // .allow_private_network_access();
        App::new()
            .wrap(cors)
            .wrap(actix_web::middleware::Logger::default())
            .app_data(web::Data::new(client.clone()))
            .service(resource("/bigquery/query/{project_id}").route(web::post().to(bigquery_query)))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use actix_web::{body::to_bytes, dev::Service, test, web, App};

    use super::*;

    #[actix_web::test]
    async fn test_bigquery_query() {
        let client = create_bq_client().await;
        log4rs::init_file("conf/log4rs.yml", Default::default()).unwrap();
        let app = test::init_service(App::new().app_data(web::Data::new(client)).service(
            web::resource("/bigquery/query/{project_id}").route(web::post().to(bigquery_query)),
        ))
        .await;
        let project_id = "secret-primacy-382307";
        let json_query = json!({
            "query1": r#"SELECT count(1) as c FROM `bigquery-public-data.bls.cpi_u` "#,
            "query1": r#"SELECT 999 as n1, 9999.9 as n2, 'str' as s1, 'tianlangstudio@aliyun.com' as s2, 'FusionZhu' as s3 "#,
            "query2": r#"SELECT count(1) as c FROM `bigquery-public-data.bls.not_exists_table` "#
        });
        let req = test::TestRequest::post()
            .uri(&format!("/bigquery/query/{}", project_id))
            .set_payload(json_query.to_string())
            .to_request();

        let resp = app.call(req).await.unwrap();
        let body_bytes = to_bytes(resp.into_body()).await.unwrap();
        let json_result: Value = serde_json::from_slice(&body_bytes).unwrap();
        println!(
            "json_query:{}",
            serde_json::to_string_pretty(&json_query).unwrap()
        );
        println!(
            "json_result:{}",
            serde_json::to_string_pretty(&json_result).unwrap()
        );
        //output example:
        /*
        json_query:{
              "query1": "SELECT 999 as n1, 9999.9 as n2, 'str' as s1, 'tianlangstudio@aliyun.com' as s2, 'FusionZhu' as s3 ",
              "query2": "SELECT count(1) as c FROM `bigquery-public-data.bls.not_exists_table` "
            }
            json_result:{
              "query1": {
                "data": [
                  {
                    "n1": "999",
                    "n2": "9999.9",
                    "s1": "str",
                    "s2": "tianlangstudio@aliyun.com",
                    "s3": "FusionZhu"
                  }
                ]
              },
              "query2": {
                "err": "Authentication error (error: connection error: connection reset)"
              }
            }
         */
    }
}
