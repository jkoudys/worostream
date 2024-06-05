-- schema.sql

CREATE TABLE IF NOT EXISTS Streams (
    stream_id TEXT PRIMARY KEY,
    total_length INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS Chunks (
    chunk_id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id TEXT NOT NULL,
    start_offset INTEGER NOT NULL,
    data BLOB NOT NULL,
    FOREIGN KEY (stream_id) REFERENCES Streams(stream_id)
);

CREATE TABLE IF NOT EXISTS ReadMarks (
    mark_id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id TEXT NOT NULL,
    start_offset INTEGER NOT NULL,
    end_offset INTEGER NOT NULL,
    FOREIGN KEY (stream_id) REFERENCES Streams(stream_id)
);
