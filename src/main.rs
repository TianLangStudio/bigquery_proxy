
use std::sync::Arc;
use futures::{future::FutureExt};
use actix_web::{web, App, Error, HttpResponse, HttpServer};
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::Client;

use actix_web::web::resource;
use json::JsonValue;
use log::info;
pub const JSON_KEY:&str = "key";
pub const JSON_KEY_DATA:&str = "data";
pub const JSON_KEY_ERR: &str = "err";

async fn bigquery_query(project_id: web::Path<String>, client:web::Data<Arc<Client>>, body: web::Bytes) -> Result<HttpResponse, Error> {
    // body is loaded, now we can deserialize json-rust
    info!("project_id:{}", project_id);
    let query_json = json::parse(std::str::from_utf8(&body)?).map_err(|e|{
        actix_web::error::ErrorPreconditionFailed(e.to_string())
    })?; // return Result

    let mut query_jobs = vec![];
    let mut query_result_json = json::object! {};

    for (ek,ev) in query_json.entries() {
        info!("query key: {}, sql: {}", ek, ev);
        let sql = ev.as_str().ok_or(actix_web::error::ErrorPreconditionFailed(
            format!("query {} must be a valid sql", ek)
        ))?;
        let query_job = client
            .job()
            .query(&project_id, QueryRequest::new(sql))
            .map(|r|{
                let mut json = JsonValue::new_object();
                json[JSON_KEY] = ek.into();
                match r {
                    Ok(mut result_set) => {
                        let mut json_array = json::JsonValue::new_array();
                        let col_names = result_set.column_names();
                        while result_set.next_row() {
                            let mut json_object = JsonValue::new_object();
                            for col_name in &col_names {
                                let col_value = result_set.get_string_by_name(&col_name).unwrap().unwrap_or("".to_string());
                                json_object[col_name] = col_value.into();
                            }
                            json_array.push(json_object).unwrap();
                        }
                        json[JSON_KEY_DATA] = json_array;
                    },
                    Err(e) => {
                        json[JSON_KEY_ERR] = e.to_string().into();
                    }
                }
                json
            });
        query_jobs.push(query_job);
    }
    let job_total = query_jobs.len();
    info!("query job total:{}", job_total);
    if job_total > 0 {
        use futures::future::join_all;
        let query_results = join_all(query_jobs).await;
        for query_result in query_results  {
            //query_result_json.push(query_result).unwrap();

            let key = &query_result[JSON_KEY];
            let mut resp = JsonValue::new_object();
            if query_result.has_key(JSON_KEY_ERR) {
                resp[JSON_KEY_ERR] = query_result[JSON_KEY_ERR].clone();
            } else {
                resp[JSON_KEY_DATA] = query_result[JSON_KEY_DATA].clone()
            };
            query_result_json.insert(key.as_str().unwrap(), resp).unwrap();
        }
    }
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(query_result_json.dump()))
}
async fn create_bq_client() -> Arc<Client> {
    let gcp_sa_key = "D:\\work\\googlecloud\\secret-primacy-382307-8c8032cdb8cc.json";
    info!("creating client ...");
    let client = Client::from_service_account_key_file(gcp_sa_key).await.expect("create bigquery client error");
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
            App::new()
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
        let app =
            test::init_service(App::new()
                .app_data(web::Data::new(client))
                .service(web::resource("/bigquery/query/{project_id}")
                    .route(web::post().to(bigquery_query))))
                .await;
        let project_id = "secret-primacy-382307";

        let req = test::TestRequest::post()
            .uri(&format!("/bigquery/query/{}", project_id))
            .set_payload(json::object! {
                "query1": r#"SELECT count(1) as c FROM `bigquery-public-data.bls.cpi_u` "#,
                "query2": r#"SELECT count(1) as c FROM `bigquery-public-data.bls.not_exists_table` "#
            }.to_string())
            .to_request();

        let resp = app.call(req).await.unwrap();
        let body_bytes = to_bytes(resp.into_body()).await.unwrap();
        println!("body_bytes:{:?}", body_bytes);
        //output example:
        //body_bytes:b"{\"query1\":{\"data\":[{\"c\":\"939014\"}]},\"query2\":{\"err\":\"Response error (error: ResponseError { error: NestedResponseError { code: 404, errors: [{\\\"reason\\\": \\\"notFound\\\", \\\"message\\\": \\\"Not found: Table bigquery-public-data:bls.not_exists_table was not found in location US\\\", \\\"domain\\\": \\\"global\\\"}], message: \\\"Not found: Table bigquery-public-data:bls.not_exists_table was not found in location US\\\", status: \\\"NOT_FOUND\\\" } })\"}}"
    }
}