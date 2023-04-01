use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::Client;

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let gcp_sa_key = "D:\\work\\googlecloud\\secret-primacy-382307-8c8032cdb8cc.json";
    println!("creating client ...");
    let client = Client::from_service_account_key_file(gcp_sa_key).await?;
    println!("created client");
    let mut rs = client
        .job()
        .query(
            "secret-primacy-382307",
            QueryRequest::new(r#"SELECT count(1) as c FROM `bigquery-public-data.bls.cpi_u` "#),
        )
        .await?;
    while rs.next_row() {
        println!(
            "Number of rows inserted: {}",
            rs.get_i64_by_name("c")?.unwrap()
        );
    }

    Ok(())
}
