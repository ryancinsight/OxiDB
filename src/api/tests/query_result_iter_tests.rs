#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{QueryResultData, Row};

    #[test]
    fn test_rows_iter_yields_all_rows() {
        // Construct QueryResultData with 3 rows
        let row1 = Row::new(vec![1.into(), 2.into()]);
        let row2 = Row::new(vec![3.into(), 4.into()]);
        let row3 = Row::new(vec![5.into(), 6.into()]);
        let data = QueryResultData::new(vec![row1, row2, row3]);

        let iter_count = data.rows_iter().collect::<Vec<_>>().len();
        let into_iter_count = data.clone().into_iter().collect::<Vec<_>>().len();

        assert_eq!(iter_count, 3);
        assert_eq!(into_iter_count, 3);
    }

    #[test]
    fn test_row_into_iter_values() {
        let vals = vec!["a".to_string().into(), "b".to_string().into(), "c".to_string().into()];
        let row = Row::new(vals.clone());

        let iter_vals = row.values_iter().cloned().collect::<Vec<_>>();
        assert_eq!(iter_vals, vals);

        let into_iter_vals = Row::new(vals.clone()).into_iter().collect::<Vec<_>>();
        assert_eq!(into_iter_vals, vals);
    }
}