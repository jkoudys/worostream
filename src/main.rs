use bytes::Bytes;
use sqlx::sqlite::SqlitePool;
use sqlx::Error;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use tokio::sync::Mutex;

type DbPool = Arc<Mutex<SqlitePool>>;

/**
 * TODOS:
 * There's a lot of i64 everywhere because that's how sqlx::Sqlite likes it, but we should keep
 * indexes at u64 as they're never negative.
 * Sqlite shoud be abstracted as one _possible_ storage option. One the supplies encryption,
 * compression, etc. But if this is created and stays very small, a plain old filesystem dd works.
 * https://crates.io/crates/stream-download has some nice existing ways to implement these traits.
 * This could be modified so it never returns pure binary data, rather a true stream. Then we
 * apply traits to the chunks returned by sqlite so they are read in-place with no copy.
 * There are some easy low-hanging fruit functions not in the original spec:
 *  - flush the stream
 *  - flush all chunks (and readmarks) from before a certain point (eg everything before last read)
 */

#[tokio::main]
async fn main() {
    // TODO: for a real implementation, use the on-disk table. This is for testing.
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    let db_pool = Arc::new(Mutex::new(pool));

    // Create tables
    // TODO: separate the table init into its own space so you only set one up if there's no cache.
    create_tables(db_pool.clone(), "schema.sql").await.unwrap();

    // Example usage
    CreateStream(db_pool.clone(), "stream1".to_string())
        .await
        .unwrap();
    let _index = AppendDataToStream(
        db_pool.clone(),
        "stream1".to_string(),
        Bytes::from("new binary data"),
    )
    .await
    .unwrap();
    let data = ReadDataFromStream(db_pool.clone(), "stream1".to_string(), 0, 14)
        .await
        .unwrap();
    println!("{:?}", data);
}

/// Reads the SQL schema from a file and executes it to create the necessary tables
///
/// # Arguments
/// * `db_pool` - Database connection pool
/// * `file_path` - Path to the SQL file containing the schema
///
/// # Errors
/// Returns an error if the file cannot be read or the SQL execution fails
async fn create_tables(db_pool: DbPool, file_path: &str) -> Result<(), Error> {
    let mut conn = db_pool.lock().await.acquire().await?;

    // Read the SQL file
    let mut file = File::open(file_path).map_err(Error::Io)?;
    let mut sql = String::new();
    file.read_to_string(&mut sql).map_err(Error::Io)?;

    // Execute the SQL
    sqlx::query(&sql).execute(&mut *conn).await?;

    Ok(())
}

/// Creates a new stream with the given ID
///
/// # Arguments
/// * `db_pool` - Database connection pool
/// * `id` - The unique string identifier of the Stream
///
/// # Errors
/// Returns an error if a stream with the same id already exists
async fn CreateStream(db_pool: DbPool, id: String) -> Result<(), Error> {
    let mut conn = db_pool.lock().await.acquire().await?;
    sqlx::query("INSERT INTO Streams (stream_id, total_length) VALUES (?, 0)")
        .bind(&id)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Appends binary data to the specified stream
///
/// # Arguments
/// * `db_pool` - Database connection pool
/// * `stream_id` - The unique string identifier of the Stream
/// * `binary_data` - The binary data to append
///
/// # Returns
/// The index assigned to the first byte of the added binary data
///
/// # Errors
/// Returns an error if the stream does not exist
async fn AppendDataToStream(
    db_pool: DbPool,
    stream_id: String,
    binary_data: Bytes,
) -> Result<i64, Error> {
    // TODO: keep active connections in a live LRU so we don't waste time building them.
    let mut conn = db_pool.lock().await.acquire().await?;

    // Retrieve the current total length of the stream
    // TODO: keep all prepared statements cached on the pool so nothing gets re-parsed. Might
    // not be necessary if sqlx is smart enough to do that on its own.
    let result: (i64,) = sqlx::query_as("SELECT total_length FROM Streams WHERE stream_id = ?")
        .bind(&stream_id)
        .fetch_one(&mut *conn)
        .await?;

    let total_length = result.0;
    let new_length = total_length + binary_data.len() as i64;

    // Insert the new chunk of data
    sqlx::query("INSERT INTO Chunks (stream_id, start_offset, data) VALUES (?, ?, ?)")
        .bind(&stream_id)
        .bind(total_length)
        .bind(binary_data.to_vec())
        .execute(&mut *conn)
        .await?;

    // Update the total length of the stream
    sqlx::query("UPDATE Streams SET total_length = ? WHERE stream_id = ?")
        .bind(new_length)
        .bind(&stream_id)
        .execute(&mut *conn)
        .await?;

    Ok(total_length)
}

/// Reads a range of bytes from the specified stream
///
/// # Arguments
/// * `db_pool` - Database connection pool
/// * `stream_id` - The unique string identifier of the Stream
/// * `start_idx` - The index of the first byte to be read
/// * `length` - The number of continuous bytes to be read
///
/// # Returns
/// The continuous byte array requested
///
/// # Errors
/// Returns an error if the requested stream does not exist, the requested bytes do not exist, or the requested bytes were already read before
async fn ReadDataFromStream(
    db_pool: DbPool,
    stream_id: String,
    start_idx: i64,
    length: i64,
) -> Result<Bytes, Error> {
    let mut conn = db_pool.lock().await.acquire().await?;

    // Check if the requested byte range has already been read
    let count: (i64,) = sqlx::query_as("SELECT COUNT(1) FROM ReadMarks WHERE stream_id = ? AND start_offset <= ? AND end_offset > ?")
        .bind(&stream_id)
        .bind(start_idx)
        .bind(start_idx)
        .fetch_one(&mut *conn)
        .await?;

    if count.0 > 0 {
        return Err(Error::RowNotFound);
    }

    // TODO: change to u64 as the start can't be negative indexed
    // Pull chunk as byte data
    let chunks: Vec<(i64, Vec<u8>)> = sqlx::query_as("SELECT start_offset, data FROM Chunks WHERE stream_id = ? AND start_offset < ? + ? ORDER BY start_offset")
        .bind(&stream_id)
        .bind(start_idx)
        .bind(length)
        .fetch_all(&mut *conn)
        .await?;

    // We merge the chunks together into the requested range from the stream
    // TODO: this could be done 0-copy with a new struct that stores only the vectors returned from
    // sqlx, then defines Serialize on them so they can be read out into a return array over the
    // wire.
    let mut data = Vec::new();
    for (chunk_start, chunk_data) in chunks {
        // TODO: u64 indexes
        let chunk_end = chunk_start + chunk_data.len() as i64;
        if chunk_start < start_idx && chunk_end > start_idx {
            data.extend_from_slice(
                &chunk_data[(start_idx - chunk_start) as usize
                    ..(start_idx + length - chunk_start) as usize],
            );
        } else if start_idx <= chunk_start && start_idx + length > chunk_start {
            data.extend_from_slice(&chunk_data[..(start_idx + length - chunk_start) as usize]);
        }
    }

    // Mark the range as read
    sqlx::query("INSERT INTO ReadMarks (stream_id, start_offset, end_offset) VALUES (?, ?, ?)")
        .bind(&stream_id)
        .bind(start_idx)
        .bind(start_idx + length)
        .execute(&mut *conn)
        .await?;

    Ok(Bytes::from(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::SqlitePool;

    async fn setup_db() -> DbPool {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let db_pool = Arc::new(Mutex::new(pool));
        create_tables(db_pool.clone(), "schema.sql").await.unwrap();
        db_pool
    }

    #[tokio::test]
    async fn test_create_stream() {
        let db_pool = setup_db().await;
        assert!(CreateStream(db_pool.clone(), "test_stream".to_string())
            .await
            .is_ok());
        assert!(CreateStream(db_pool.clone(), "test_stream".to_string())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_append_data_to_stream() {
        let db_pool = setup_db().await;
        CreateStream(db_pool.clone(), "test_stream".to_string())
            .await
            .unwrap();
        let index = AppendDataToStream(
            db_pool.clone(),
            "test_stream".to_string(),
            Bytes::from("binary data"),
        )
        .await
        .unwrap();
        assert_eq!(index, 0);
    }

    #[tokio::test]
    async fn test_read_data_from_stream() {
        let db_pool = setup_db().await;
        CreateStream(db_pool.clone(), "test_stream".to_string())
            .await
            .unwrap();
        AppendDataToStream(
            db_pool.clone(),
            "test_stream".to_string(),
            Bytes::from("binary data"),
        )
        .await
        .unwrap();
        let data = ReadDataFromStream(db_pool.clone(), "test_stream".to_string(), 0, 11)
            .await
            .unwrap();
        assert_eq!(data, Bytes::from("binary data"));

        // Attempt to read the same range again should fail
        assert!(
            ReadDataFromStream(db_pool.clone(), "test_stream".to_string(), 0, 11)
                .await
                .is_err()
        );
    }
}
