#[cfg(test)]
mod tests {
    use crate::api::{QueryResultData, Row};
    use crate::core::common::types::Value;

    #[test]
    fn test_rows_iter_yields_all_rows() {
        // Construct QueryResultData with 3 rows and explicit columns
        let row1 = Row::new(vec![Value::Integer(1), Value::Integer(2)]);
        let row2 = Row::new(vec![Value::Integer(3), Value::Integer(4)]);
        let row3 = Row::new(vec![Value::Integer(5), Value::Integer(6)]);
        let data = QueryResultData::new(
            vec!["c1".to_string(), "c2".to_string()],
            vec![row1.clone(), row2.clone(), row3.clone()],
        );

        let iter_count = data.rows_iter().count();
        let into_iter_count = (&data).into_iter().count();

        assert_eq!(iter_count, 3);
        assert_eq!(into_iter_count, 3);
    }

    #[test]
    fn test_row_into_iter_values() {
        let vals = vec!["a".to_string().into(), "b".to_string().into(), "c".to_string().into()];
        let row = Row::new(vals.clone());

        let iter_vals = row.values_iter().cloned().collect::<Vec<_>>();
        assert_eq!(iter_vals, vals);

        let into_iter_vals = (&row).into_iter().cloned().collect::<Vec<_>>();
        assert_eq!(into_iter_vals, vals);
    }
}