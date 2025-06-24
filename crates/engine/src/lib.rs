//! Engine crate
//!
//! Implements the core query engine for Igloo.
//!
//! # Example
//! ```rust
//! // Example usage will go here once implemented
//! ```
//!
//! # TODO
//! Implement query engine logic

// std
use std::sync::Arc;

// datafusion -> arrow
use datafusion::arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;

// datafusion -> core
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility};

pub mod join_engine;
pub mod parser;

pub use join_engine::{JoinEngine, JoinConfig, JoinType};

#[derive(Clone)]
pub struct QueryEngine {
    ctx: SessionContext,
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryEngine {
    pub fn new() -> Self {
        let ctx = SessionContext::new();
        let capitalize_udf = make_capitalize_udf();
        ctx.register_udf(capitalize_udf);
        QueryEngine { ctx }
    }

    pub fn register_table(
        &self,
        name: &str,
        table: Arc<dyn datafusion::datasource::TableProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn datafusion::datasource::TableProvider>>> {
        self.ctx.register_table(name, table)
    }

    pub async fn execute(&self, sql: &str) -> Vec<RecordBatch> {
        let df = self.ctx.sql(sql).await.expect("SQL execution failed");
        df.collect().await.expect("Failed to collect results")
    }

    /// Get the underlying SessionContext for advanced operations
    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }
}

/// Capitalizes the first string array in the input.
///
/// # Errors
///
/// Returns a `DataFusionError` if:
/// - The input slice is empty.
/// - The first element in the slice cannot be cast to `StringArray`.
pub fn capitalize_internal(args: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    if args.is_empty() {
        return Err(DataFusionError::Execution(
            "capitalize_internal expects at least one argument".to_string(),
        ));
    }

    let string_array = args[0].as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        DataFusionError::Execution("Failed to downcast to StringArray".to_string())
    })?;

    let mut builder = StringBuilder::new();

    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            builder.append_null();
        } else {
            let value = string_array.value(i);
            builder.append_value(value.to_uppercase());
        }
    }

    let result_array = builder.finish();
    Ok(Arc::new(result_array) as ArrayRef)
}

/// Wraps `capitalize_internal` to match the signature expected by `create_udf`.
fn capitalize_columnar_wrapper(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    // Extract ArrayRefs from ColumnarValues.
    // This basic wrapper assumes args are ColumnarValue::Array.
    // More robust handling might check ColumnarValue variants.
    let arrays: Vec<ArrayRef> = args
        .iter()
        .map(|cv| {
            Ok(match cv {
                ColumnarValue::Array(array) => array.clone(),
                ColumnarValue::Scalar(_scalar) => {
                    // This is a simplification. In a real scenario, you might want to
                    // create a full array from the scalar or handle it differently.
                    // For capitalize, we expect a StringArray.
                    // DataFusion would typically handle scalar expansion to array before UDF call for many cases.
                    // If a scalar string is passed, capitalize_internal would need to handle it or expect an array.
                    // For now, let's assume DataFusion handles scalar arguments appropriately by broadcasting
                    // them into arrays before they reach this point, or that this UDF is always called with array args.
                    // If not, this part would need more sophisticated logic to create an array from the scalar.
                    // A direct conversion from a generic ScalarValue to a specific ArrayRef (like StringArray)
                    // is not straightforward without knowing the DataType.
                    // Let's return an error for now if we encounter a scalar directly in the wrapper,
                    // as capitalize_internal expects an array.
                    return Err(DataFusionError::Execution(
                        "capitalize UDF expects array arguments, received scalar".to_string(),
                    ));
                }
            })
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;

    // Call the original function
    let result_array_ref = capitalize_internal(&arrays)?;

    // Wrap the result back into a ColumnarValue
    Ok(ColumnarValue::Array(result_array_ref))
}

/// Creates a ScalarUDF for the capitalize function.
pub fn make_capitalize_udf() -> ScalarUDF {
    create_udf(
        "capitalize",
        vec![DataType::Utf8],
        DataType::Utf8, // Corrected: directly use DataType
        Volatility::Immutable,
        Arc::new(capitalize_columnar_wrapper), // Use the wrapper
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::MemTable; // Corrected path
                                       // DataFusionResult is brought in by super::*
    use std::sync::Arc;

    #[tokio::test]
    async fn can_execute_simple_query() {
        // Arrange
        let engine = QueryEngine::new();
        let sql = "SELECT 42 as answer;";

        // Act
        let results = engine.execute(sql).await;

        // Assert
        assert_eq!(results.len(), 1, "Expected one RecordBatch");

        let batch = &results[0];

        // Check schema
        let expected_schema = Schema::new(vec![Field::new("answer", DataType::Int64, false)]);
        assert_eq!(batch.schema(), Arc::new(expected_schema), "Schema mismatch");

        // Check data
        assert_eq!(batch.num_rows(), 1, "Expected one row");
        let answer_column = batch
            .column_by_name("answer")
            .expect("answer column not found")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Failed to downcast to Int64Array");

        assert_eq!(answer_column.value(0), 42, "Incorrect value in answer column");
    }

    #[tokio::test]
    async fn test_capitalize_udf() -> DataFusionResult<()> {
        // 1. Set up QueryEngine (UDF is registered in new())
        let engine = QueryEngine::new();

        // 2. Create Test Data and Register Table
        let schema = Arc::new(Schema::new(vec![Field::new("text_col", DataType::Utf8, true)]));
        let data_array =
            StringArray::from(vec![Some("hello"), Some("WoRlD"), None, Some("rust"), Some("")]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(data_array)])?;

        // Wrap RecordBatch in a MemTable
        let table_provider = MemTable::try_new(schema, vec![vec![batch.clone()]])?; // MemTable expects Vec<Vec<RecordBatch>> for partitions

        engine.register_table("test_strings", Arc::new(table_provider))?;

        // 3. Execute SQL Query
        // Order by the result to ensure consistent order for assertion.
        // Explicitly state NULLS FIRST, as default behavior might vary or change.
        let sql = "SELECT capitalize(text_col) AS capitalized_text FROM test_strings ORDER BY capitalized_text ASC NULLS FIRST";
        let results = engine.execute(sql).await;

        // 4. Assert Results
        assert_eq!(results.len(), 1, "Expected one RecordBatch");
        let result_batch = &results[0];

        let capitalized_column = result_batch
            .column_by_name("capitalized_text")
            .expect("capitalized_text column not found")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast to StringArray");

        // Expected order: NULL, "", "HELLO", "RUST", "WORLD"
        let expected_data = StringArray::from(vec![
            None, // NULL sorts first
            Some(""),
            Some("HELLO"),
            Some("RUST"),
            Some("WORLD"),
        ]);

        assert_eq!(capitalized_column, &expected_data, "Capitalized data mismatch");

        Ok(())
    }
}