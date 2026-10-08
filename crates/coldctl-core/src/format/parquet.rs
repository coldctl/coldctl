use crate::{
    archive::batch::{ArchiveColumn, DataBatch},
    error::Error,
};
use arrow_array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};
use std::{collections::HashMap, sync::Arc};

fn encoding_error<T>(_: T) -> Error {
    Error::Archive(
        "Parquet encoding or verification failed; row values are omitted from diagnostics",
    )
}
fn parse<T: std::str::FromStr>(values: &[Option<String>]) -> Result<Vec<Option<T>>, Error> {
    values
        .iter()
        .map(|v| {
            v.as_deref()
                .map(|s| s.parse::<T>().map_err(encoding_error))
                .transpose()
        })
        .collect()
}

pub fn encode(
    columns: &[ArchiveColumn],
    batch: &DataBatch,
) -> Result<tempfile::NamedTempFile, Error> {
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (position, column) in columns.iter().enumerate() {
        let values: Vec<Option<String>> = batch
            .rows
            .iter()
            .map(|r| {
                r.get(position)
                    .cloned()
                    .ok_or(Error::Archive("invalid batch shape"))
            })
            .collect::<Result<_, _>>()?;
        if !column.nullable && values.iter().any(Option::is_none) {
            return Err(Error::Archive("unexpected NULL in non-null archive column"));
        }
        let (kind, array): (DataType, ArrayRef) = match column.postgres_type.as_str() {
            "int2" => (
                DataType::Int16,
                Arc::new(Int16Array::from(parse::<i16>(&values)?)),
            ),
            "int4" => (
                DataType::Int32,
                Arc::new(Int32Array::from(parse::<i32>(&values)?)),
            ),
            "int8" => (
                DataType::Int64,
                Arc::new(Int64Array::from(parse::<i64>(&values)?)),
            ),
            "float4" => (
                DataType::Float32,
                Arc::new(Float32Array::from(parse::<f32>(&values)?)),
            ),
            "float8" => (
                DataType::Float64,
                Arc::new(Float64Array::from(parse::<f64>(&values)?)),
            ),
            "bool" => {
                let bools: Vec<Option<bool>> = values
                    .iter()
                    .map(|v| match v.as_deref() {
                        None => Ok(None),
                        Some("t" | "true") => Ok(Some(true)),
                        Some("f" | "false") => Ok(Some(false)),
                        _ => Err(Error::Archive("invalid boolean in archive batch")),
                    })
                    .collect::<Result<_, _>>()?;
                (DataType::Boolean, Arc::new(BooleanArray::from(bools)))
            }
            "text" | "varchar" | "bpchar" | "numeric" | "date" | "timestamp" | "timestamptz"
            | "uuid" | "json" | "jsonb" => (DataType::Utf8, Arc::new(StringArray::from(values))),
            crate::format::bson::ID_TYPE | crate::format::bson::DOCUMENT_TYPE => {
                (DataType::Utf8, Arc::new(StringArray::from(values)))
            }
            kind if crate::source::mysql_types::parts(kind).is_ok() => {
                (DataType::Utf8, Arc::new(StringArray::from(values)))
            }
            _ => return Err(Error::Archive("unsupported Parquet column type")),
        };
        fields.push(
            Field::new(&column.name, kind, column.nullable).with_metadata(HashMap::from([(
                if column.postgres_type.starts_with("mysql:")
                    || column.postgres_type.starts_with("mongodb:")
                {
                    "coldctl.native_type".into()
                } else {
                    "coldctl.postgres_type".into()
                },
                column.postgres_type.clone(),
            )])),
        );
        arrays.push(array);
    }
    let record =
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(encoding_error)?;
    let temp = tempfile::NamedTempFile::new().map_err(encoding_error)?;
    let properties = WriterProperties::builder()
        .set_key_value_metadata(if columns == crate::format::bson::columns() {
            Some(vec![parquet::format::KeyValue::new(
                "coldctl.document_encoding".into(),
                Some("bson-hex-v1".into()),
            )])
        } else {
            None
        })
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(batch.rows.len().max(1))
        .build();
    let mut writer = ArrowWriter::try_new(
        temp.reopen().map_err(encoding_error)?,
        record.schema(),
        Some(properties),
    )
    .map_err(encoding_error)?;
    writer.write(&record).map_err(encoding_error)?;
    writer.close().map_err(encoding_error)?;
    temp.as_file().sync_all().map_err(encoding_error)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(temp.reopen().map_err(encoding_error)?)
        .map_err(encoding_error)?
        .build()
        .map_err(encoding_error)?;
    let mut rows = 0;
    for batch in reader {
        rows += batch.map_err(encoding_error)?.num_rows();
    }
    if rows != record.num_rows() {
        return Err(Error::Archive("Parquet row count verification failed"));
    }
    Ok(temp)
}
