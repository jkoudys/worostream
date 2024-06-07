use bytes::Bytes;
use sqlx::sqlite::SqlitePool;
use sqlx::Error as SqlxError;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

type DbPool = Arc<Mutex<SqlitePool>>;

/**
 * TODOS:
 * There's a lot of i64 everywhere because that's how sqlx::Sqlite likes it, but we should keep
 * indexes at u64 as they're never negative. At least newtype it for sanity.
 * Sqlite shoud be abstracted as one _possible_ storage option. One the supplies encryption,
 * compression, etc. But if this is created and stays very small, a plain old filesystem dd works.
 * https://crates.io/crates/stream-download has some nice existing ways to implement these traits.
 * This could be modified so it never returns pure binary data, rather a true stream. Then we
 * apply traits to the chunks returned by sqlite so they are read in-place with no copy.
 */

#[derive(Error, Debug)]
pub enum WoroError {
    #[error("A stream with the same id already exists")]
    StreamAlreadyExists,

    #[error("The stream does not exist")]
    StreamNotFound,

    #[error("The requested bytes do not exist in the storage")]
    BytesNotFound,

    #[error("The requested bytes were already read before")]
    BytesAlreadyRead,

    #[error("SQL error: {0}")]
    SqlxError(#[from] SqlxError),
}

#[tokio::main]
async fn main() {
    // TODO: for a real implementation, use the on-disk table. This is for testing.
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    let db_pool = Arc::new(Mutex::new(pool));

    // Create tables
    // TODO: separate the table init into its own space so you only set one up if there's no cache.
    create_tables(db_pool.clone(), "schema.sql").await.unwrap();

    let test_stream = "stream1";

    // Example usage
    create_stream(db_pool.clone(), test_stream.to_string())
        .await
        .unwrap();

    // Start our stream
    let _index = append_data_to_stream(
        db_pool.clone(),
        test_stream.to_string(),
        Bytes::from("abcde"),
    )
    .await
    .unwrap();

    let data = read_data_from_stream(db_pool.clone(), test_stream.to_string(), 0, 3)
        .await;
    println!("{:?}", data);

    // Add a little more data
    let _index = append_data_to_stream(
        db_pool.clone(),
        test_stream.to_string(),
        Bytes::from("fghijk"),
    )
    .await
    .unwrap();

    // Try reading some more bytes off the same string, from both the first and last append
    let data = read_data_from_stream(db_pool.clone(), test_stream.to_string(), 3, 2)
        .await;
    println!("{:?}", data);

    // Read some data that includes a byte we've already read.
    // Should fail.
    let data = read_data_from_stream(db_pool.clone(), test_stream.to_string(), 4, 2)
        .await;
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
async fn create_tables(db_pool: DbPool, file_path: &str) -> Result<(), WoroError> {
    let mut conn = db_pool.lock().await.acquire().await?;

    // Read the SQL file
    let mut file = File::open(file_path).map_err(SqlxError::Io)?;
    let mut sql = String::new();
    file.read_to_string(&mut sql).map_err(SqlxError::Io)?;

    // Execute the SQL
    // This is only setup code for testing purposes, so we're not too clever about error handling.
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
async fn create_stream(db_pool: DbPool, id: String) -> Result<(), WoroError> {
    let mut conn = db_pool.lock().await.acquire().await?;
    match sqlx::query("INSERT INTO Streams (stream_id, total_length) VALUES (?, 0)")
        .bind(&id)
        .execute(&mut *conn)
        .await
    {
        Ok(_) => Ok(()),
        // if stream PK collides, it must be for this error.
        // TODO: is there a better way than just looking at the error message?
        Err(SqlxError::Database(db_err))
            if db_err.message().contains("UNIQUE constraint failed") =>
        {
            Err(WoroError::StreamAlreadyExists)
        }
        Err(e) => Err(WoroError::SqlxError(e)),
    }
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
async fn append_data_to_stream(
    db_pool: DbPool,
    stream_id: String,
    binary_data: Bytes,
) -> Result<i64, WoroError> {
    // TODO: keep active connections in a live LRU so we don't waste time building them.
    let mut conn = db_pool.lock().await.acquire().await?;

    // Retrieve the current total length of the stream
    // TODO: keep all prepared statements cached on the pool so nothing gets re-parsed. Might
    // not be necessary if sqlx is smart enough to do that on its own.
    let result: (i64,) = sqlx::query_as("SELECT total_length FROM Streams WHERE stream_id = ?")
        .bind(&stream_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| {
            if let SqlxError::RowNotFound = e {
                WoroError::StreamNotFound
            } else {
                WoroError::SqlxError(e)
            }
        })?;

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
async fn read_data_from_stream(
    db_pool: DbPool,
    stream_id: String,
    start_idx: i64,
    length: i64,
) -> Result<Bytes, WoroError> {
    let mut conn = db_pool.lock().await.acquire().await?;

    // Check if the requested byte range has already been read
    // Use fact that ranges [a1:a2] and [b1:b2] overlap if a1 <= b2 and b1 <= a2
    // ranges collide to check quickly in sql. Hopefully it picks up a sorted index, too.
    let count: (i64,) = sqlx::query_as(
        r#"
SELECT COUNT(1)
FROM ReadMarks
WHERE
    stream_id = ?
    AND
    start_offset <= ? AND ? < end_offset
LIMIT 1
"#,
    )
    .bind(&stream_id)
    .bind(start_idx + length)
    .bind(start_idx)
    .fetch_one(&mut *conn)
    .await
    // TODO: not sure that we CAN get a rownotfound on here, but may as well double-check
    .map_err(|e| {
        if let SqlxError::RowNotFound = e {
            WoroError::BytesAlreadyRead
        } else {
            WoroError::SqlxError(e)
        }
    })?;

    if count.0 > 0 {
        return Err(WoroError::BytesAlreadyRead);
    }

    // Pull chunk as byte data
    let chunks: Vec<(i64, Vec<u8>)> = sqlx::query_as(
        r#"
SELECT
    start_offset,
    data
FROM Chunks
WHERE stream_id = ?
    AND start_offset < ?
ORDER BY start_offset
"#,
    )
    .bind(&stream_id)
    .bind(start_idx + length)
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| {
        if let SqlxError::RowNotFound = e {
            WoroError::BytesNotFound
        } else {
            WoroError::SqlxError(e)
        }
    })?;

    if chunks.is_empty() {
        return Err(WoroError::BytesNotFound);
    }

    // We merge the chunks together into the requested range from the stream
    // TODO: this could be done 0-copy with a new struct that stores only the vectors returned from
    // sqlx, then defines Serialize on them so they can be read out into a return array over the
    // wire.
    // We know the resulting stream size ahead of time, so prealloc for efficiency.
    let mut data = Vec::with_capacity(length as usize);

    let mut read_bytes = 0;

    for (chunk_start, chunk_data) in chunks {
        let chunk_end = chunk_start + chunk_data.len() as i64;

        if chunk_start <= start_idx && chunk_end > start_idx {
            let offset = (start_idx - chunk_start) as usize;
            let end_offset = (start_idx + length - chunk_start).min(chunk_data.len() as i64) as usize;
            data.extend_from_slice(&chunk_data[offset..end_offset]);
            read_bytes += end_offset - offset;
        } else if chunk_start > start_idx && chunk_start < start_idx + length {
            let end_offset = (start_idx + length - chunk_start).min(chunk_data.len() as i64) as usize;
            data.extend_from_slice(&chunk_data[..end_offset]);
            read_bytes += end_offset;
        }

        if read_bytes >= length as usize {
            break;
        }
    }

    if data.len() < length as usize {
        return Err(WoroError::BytesNotFound);
    }

    // Mark the range as read
    sqlx::query("INSERT INTO ReadMarks (stream_id, start_offset, end_offset) VALUES (?, ?, ?)")
        .bind(&stream_id)
        .bind(start_idx)
        .bind(start_idx + length)
        .execute(&mut *conn)
        .await?;

    // TODO: we COULD delete the data right away once it's been read here. But it may make more
    // sense to batch all the readmarks together and delete from the table on a background thread.
    // This gets the results to the caller back ASAP, and could reduce the number of deletes if
    // you have many contiguous ReadMarks on the same stream that could be DELETEd in one call.

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
        assert!(create_stream(db_pool.clone(), "test_stream".to_string())
            .await
            .is_ok());
        assert!(matches!(
            create_stream(db_pool.clone(), "test_stream".to_string()).await,
            Err(WoroError::StreamAlreadyExists)
        ));
    }

    #[tokio::test]
    async fn test_append_data_to_stream() {
        let db_pool = setup_db().await;
        create_stream(db_pool.clone(), "test_stream".to_string())
            .await
            .unwrap();
        let index = append_data_to_stream(
            db_pool.clone(),
            "test_stream".to_string(),
            Bytes::from("abcdefghijkl"),
        )
        .await
        .unwrap();
        assert_eq!(index, 0);
        // Finish our alphabet
        let index = append_data_to_stream(
            db_pool.clone(),
            "test_stream".to_string(),
            Bytes::from("mnopqrstuvwxyz"),
        )
        .await
        .unwrap();
        assert_eq!(index, 12);
    }

    #[tokio::test]
    async fn test_read_data_from_stream() {
        let db_pool = setup_db().await;
        create_stream(db_pool.clone(), "test_stream".to_string())
            .await
            .unwrap();
        append_data_to_stream(
            db_pool.clone(),
            "test_stream".to_string(),
            Bytes::from("bin"),
        )
        .await
        .unwrap();

        append_data_to_stream(
            db_pool.clone(),
            "test_stream".to_string(),
            Bytes::from("ary data"),
        )
        .await
        .unwrap();

        let data = read_data_from_stream(db_pool.clone(), "test_stream".to_string(), 0, 11)
            .await
            .unwrap();
        assert_eq!(data, Bytes::from("binary data"));

        // Attempt to read the same range again should fail
        assert!(matches!(
            read_data_from_stream(db_pool.clone(), "test_stream".to_string(), 0, 11).await,
            Err(WoroError::BytesAlreadyRead)
        ));

        // Attempt to read a byte inside the range should fail too.
        assert!(matches!(
            read_data_from_stream(db_pool.clone(), "test_stream".to_string(), 9, 2).await,
            Err(WoroError::BytesAlreadyRead)
        ));
    }
}
