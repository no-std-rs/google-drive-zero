//! Host adapter example. The library itself remains no_std + alloc.
use google_drive_zero::{host::UreqTransport, Client, ListOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("DRIVE_ACCESS_TOKEN")
        .map_err(|_| "set DRIVE_ACCESS_TOKEN to a supplied OAuth access token")?;
    let mut drive = Client::new(UreqTransport::default(), token)?;
    futures_executor::block_on(async {
        let mut page_token = None;
        loop {
            let page = drive
                .list(&ListOptions {
                    page_token: page_token.as_deref(),
                    ..Default::default()
                })
                .await?;
            if page.incomplete_search {
                return Err("Drive search is incomplete".into());
            }
            for file in page.files {
                println!("{}\t{}\t{:?}", file.id, file.name, file.size);
            }
            page_token = page.next_page_token;
            if page_token.is_none() {
                break;
            }
        }
        Ok(())
    })
}
