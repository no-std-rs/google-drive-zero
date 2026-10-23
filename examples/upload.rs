//! Creates a new Drive file from a local file. Never buffers the full payload.
use google_drive_zero::{
    host::{ReadBody, UreqTransport},
    Client, NewFile, UploadOutcome,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("DRIVE_ACCESS_TOKEN")
        .map_err(|_| "set DRIVE_ACCESS_TOKEN to a supplied OAuth access token")?;
    let path = std::env::args_os()
        .nth(1)
        .ok_or("usage: upload LOCAL_FILE")?;
    let path = std::path::Path::new(&path);
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or("invalid file name")?;
    let file = std::fs::File::open(path)?;
    let size = file.metadata()?.len();
    let mut drive = Client::new(UreqTransport::default(), token)?;
    futures_executor::block_on(async {
        let mut session = drive
            .create_upload(
                NewFile {
                    name,
                    mime_type: "application/octet-stream",
                    parent: None,
                },
                Some(size),
            )
            .await?;
        // To survive a process restart, securely persist session.session_url()
        // and total_size(). On failure: resume, query_upload, seek the source to
        // session.offset(), and stream the remaining bytes. Never log the URL.
        match drive
            .write_stream(&mut session, &mut ReadBody(file), size, true)
            .await?
        {
            UploadOutcome::Complete(file) => println!("{}\t{}", file.id, file.name),
            UploadOutcome::Incomplete { confirmed } => {
                return Err(format!(
                    "Drive acknowledged {confirmed}/{size} bytes; resume from the confirmed offset"
                )
                .into())
            }
        }
        Ok(())
    })
}
