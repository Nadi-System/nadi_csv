use nadi_core::nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod csv {
    use crate::utils::*;
    use nadi_core::attrs::IntOrStr;
    use nadi_core::prelude::*;
    use polars::error::ErrString;
    use polars::prelude::*;
    use std::collections::HashMap;
    use std::path::PathBuf;

    /// The macros imported from nadi_plugin read the rust function you
    /// write and use that as a base to write more core internally that
    /// will be compiled into the shared libraries. This means it'll
    /// automatically get the argument types, documentation, mutability,
    /// etc. For more details on what they can do, refer to nadi book.
    use nadi_core::nadi_plugin::{env_func, network_func, node_func};

    /// List the columns in a CSV file
    #[env_func(header = true, dates = true)]
    fn schema(
        /// Path to the CSV file
        path: PathBuf,
        /// CSV file has header row
        header: bool,
        /// Parse Dates in the file
        dates: bool,
    ) -> Result<HashMap<String, String>, String> {
        let schema = LazyCsvReader::new(path)
            .with_has_header(header)
            .with_try_parse_dates(dates)
            .with_infer_schema_length(Some(10))
            .with_ignore_errors(true)
            .finish()
            .map_err(|e| e.to_string())?
            .collect_schema()
            .map_err(|e| e.to_string())?;
        Ok(schema
            .iter()
            .map(|(c, v)| (c.to_string(), v.to_string()))
            .collect())
    }

    /// read a single column from a CSV file
    #[node_func(dates = true)]
    fn column(
        node: &mut NodeInner,
        /// Path to the CSV file
        path: PathBuf,
        /// Name or Index of the column to load
        column: IntOrStr,
        /// Parse Dates in the file
        dates: bool,
        /// Name of the series defaults to column name
        name: Option<String>,
    ) -> Result<(), PolarsError> {
        let df = LazyCsvReader::new(path)
            .with_has_header(true)
            .with_try_parse_dates(dates)
            .with_infer_schema_length(Some(10))
            .with_ignore_errors(true)
            .finish()?;
        let sel = match column {
            IntOrStr::Integer(i) => nth(i),
            IntOrStr::String(ref s) => col(s.as_str()),
        };
        let df: DataFrame = df.select([sel]).collect()?;
        let col = df
            .select_at_idx(0)
            .expect("Should have at least one column");
        let ser = polars_col_to_series(col)?;
        let name = name.unwrap_or_else(|| match column {
            IntOrStr::Integer(_) => col.name().to_string(),
            IntOrStr::String(s) => s.to_string(),
        });
        node.set_series(&name, ser);
        Ok(())
    }

    /// Count the number of data in a column from a CSV file
    ///
    /// Returns the number of valid data and the number of total rows
    #[env_func]
    fn count_data(
        /// Path to the CSV file
        path: PathBuf,
        /// Name or Index of the column to load
        column: IntOrStr,
    ) -> Result<(usize, usize), PolarsError> {
        let df = LazyCsvReader::new(path)
            .with_has_header(true)
            .with_infer_schema_length(Some(10))
            .with_ignore_errors(true)
            .finish()?;
        let sel = match column {
            IntOrStr::Integer(i) => nth(i),
            IntOrStr::String(ref s) => col(s.as_str()),
        };
        let df: DataFrame = df.select([sel]).collect()?;
        let col = df
            .select_at_idx(0)
            .expect("Should have at least one column");
        let len = col.len();
        let nulls = col.null_count();
        Ok((len - nulls, len))
    }

    /// Count the number of data in a column from a CSV file
    ///
    /// Returns the number of total rows and the number of valid data
    #[env_func]
    fn count_usgs_years(
        /// Path to the CSV file
        path: PathBuf,
        /// Output CSV file
        outfile: PathBuf,
        /// year dam affected
        year: Option<i64>,
    ) -> Result<(), PolarsError> {
        let df = LazyCsvReader::new(path)
            .with_has_header(false)
            .with_try_parse_dates(true)
            .with_infer_schema_length(Some(10))
            .with_ignore_errors(true)
            .finish()?;
        let df = df
            .select([nth(0), nth(1).is_not_null().alias("data")])
            .group_by([nth(0).dt().year().alias("year")])
            .agg([sum("data")])
            .sort(["year"], Default::default());
        let mut df: DataFrame = if let Some(yr) = year {
            df.with_column(
                ternary_expr(col("year").lt(yr), lit("pre"), lit("post")).alias("category"),
            )
        } else {
            df.with_column(lit("unknown").alias("category"))
        }
        .collect()?;
        let mut file = std::fs::File::create(outfile).unwrap();
        CsvWriter::new(&mut file).finish(&mut df)?;
        Ok(())
    }

    /// Example Node function for the plugin
    #[node_func]
    fn node_name(node: &NodeInner) -> String {
        node.name().to_string()
    }

    /// Load series values to nodes
    ///
    /// ```task,ignore
    /// network csv.load_series("/home/gaurav/work/nadi-project/codes/nadi_csv/test.csv", "test")
    ///
    #[network_func(dates = true)]
    fn load_series(
        net: &Network,
        /// Path to the CSV file
        path: PathBuf,
        /// Name of the Series
        name: String,
        /// HashMap of column name to Node name
        nodemap: Option<HashMap<String, String>>,
        /// Parse Dates in the file
        dates: bool,
    ) -> Result<(), PolarsError> {
        let df = LazyCsvReader::new(path)
            .with_has_header(true)
            .with_try_parse_dates(dates)
            .with_infer_schema_length(Some(10))
            .with_ignore_errors(true)
            .finish()?;
        let df: DataFrame = df.collect()?;
        for node in net.nodes() {
            let mut node = node.lock();
            let node_name = node.name();
            let col = match nodemap {
                // TODO handle this error
                Some(ref map) => match map.get(node_name) {
                    Some(n) => df.column(&map[n])?,
                    None => {
                        return Err(PolarsError::ColumnNotFound(ErrString::new_static(
                            "Node Map does not have the node",
                        )))
                    }
                },
                None => df.column(node_name)?,
            };
            let series = polars_col_to_series(col)?;
            node.set_series(&name, series);
        }
        Ok(())
    }
}

mod utils {
    use abi_stable::std_types::{RNone, ROption, RString};
    use nadi_core::attrs::Date;
    use nadi_core::timeseries::{CompleteSeries, MaskedSeries, Series};
    use polars::error::ErrString;
    use polars::prelude::*;

    pub fn polars_col_to_series(col: &Column) -> Result<Series, PolarsError> {
        Ok(match col.dtype() {
            DataType::Boolean => {
                if col.has_nulls() {
                    let vals: Vec<ROption<bool>> =
                        col.bool()?.into_iter().map(|op| op.into()).collect();
                    Series::Masked(MaskedSeries::booleans(vals), RNone)
                } else {
                    let vals: Vec<bool> = col.bool()?.into_no_null_iter().collect();
                    Series::Complete(CompleteSeries::booleans(vals))
                }
            }
            DataType::UInt8
            | DataType::UInt16
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64 => {
                let col = col.cast(&DataType::Int64)?;
                if col.has_nulls() {
                    let vals: Vec<ROption<i64>> =
                        col.i64()?.into_iter().map(|op| op.into()).collect();
                    Series::Masked(MaskedSeries::integers(vals), RNone)
                } else {
                    let vals: Vec<i64> = col.i64()?.into_no_null_iter().collect();
                    Series::Complete(CompleteSeries::integers(vals))
                }
            }
            DataType::Float32 | DataType::Float64 => {
                let col = col.cast(&DataType::Float64)?;
                if col.has_nulls() {
                    let vals: Vec<ROption<f64>> =
                        col.f64()?.into_iter().map(|op| op.into()).collect();
                    Series::Masked(MaskedSeries::floats(vals), RNone)
                } else {
                    let vals: Vec<f64> = col.f64()?.into_no_null_iter().collect();
                    Series::Complete(CompleteSeries::floats(vals))
                }
            }
            DataType::String => {
                if col.has_nulls() {
                    let vals: Vec<ROption<RString>> = col
                        .str()?
                        .into_iter()
                        .map(|op| op.map(RString::from).into())
                        .collect();
                    Series::Masked(MaskedSeries::strings(vals), RNone)
                } else {
                    let vals: Vec<RString> =
                        col.str()?.into_no_null_iter().map(RString::from).collect();
                    Series::Complete(CompleteSeries::strings(vals))
                }
            }
            DataType::Date => {
                if col.has_nulls() {
                    let dt = col.date()?;
                    let vals: Vec<ROption<Date>> = dt
                        .year()
                        .into_iter()
                        .zip(dt.month().into_iter())
                        .zip(dt.day().into_iter())
                        .map(|((y, m), d)| {
                            y.zip(m)
                                .zip(d)
                                .map(|((y, m), d)| Date::new(y as u16, m as u8, d as u8))
                                .into()
                        })
                        .collect();
                    Series::Masked(MaskedSeries::dates(vals), RNone)
                } else {
                    let dt = col.date()?;
                    let vals: Vec<Date> = dt
                        .year()
                        .into_no_null_iter()
                        .zip(dt.month().into_no_null_iter())
                        .zip(dt.day().into_no_null_iter())
                        .map(|((y, m), d)| Date::new(y as u16, m as u8, d as u8))
                        .collect();
                    Series::Complete(CompleteSeries::dates(vals))
                }
            }
            // Datetime(_, _) => ,
            // Time => ,
            _ => {
                return Err(PolarsError::SchemaMismatch(ErrString::new_static(
                    "Data Type cannot be converted to Series",
                )))
            }
        })
    }
}
