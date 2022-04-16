/*!
Definition of [Sort orders](https://iceberg.apache.org/spec/#sorting) for a Table.

A [SortOrder] is composed of a list of [SortField] where each field has a [Transform],
[SortDirection] and [NullOrder].

*/
use crate::model::partition::Transform;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Defines the sort order for a field.
pub enum SortDirection {
    /// Sort the field ascending.
    #[serde(rename = "asc")]
    Ascending,
    /// Sort the field descending.
    #[serde(rename = "desc")]
    Descending,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Defines the sort order for nulls in a field.
pub enum NullOrder {
    #[serde(rename = "nulls-first")]
    /// Place the nulls first in the search.
    First,
    #[serde(rename = "nulls-last")]
    /// Place the nulls last in the search.
    Last,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Definition of a how a field should be used within a sort.
pub struct SortField {
    /// A source column id from the table’s schema
    pub source_id: i32,
    /// A transform that is used to produce values to be sorted on from the source column.
    pub transform: Transform,
    /// A sort direction, that can only be either asc or desc
    pub direction: SortDirection,
    /// A null order that describes the order of null values when sorted.
    pub null_order: NullOrder,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// A sort order is defined by an sort order id and a list of sort fields.
/// The order of the sort fields within the list defines the order in
/// which the sort is applied to the data.
pub struct SortOrder {
    /// Identifier for SortOrder, order_id `0` is no sort order.
    pub order_id: i32,
    /// Details of the sort
    pub fields: Vec<SortField>,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_sort_field() {
        let data = r#"
            {
               "transform": "bucket[4]",   
               "source-id": 3,   
               "direction": "desc",   
               "null-order": "nulls-last"
            } 
        "#;

        let field: SortField = serde_json::from_str(&data).unwrap();
        assert_eq!(3, field.source_id);
        assert_eq!(Transform::Bucket(4), field.transform);
        assert_eq!(SortDirection::Descending, field.direction);
        assert_eq!(NullOrder::Last, field.null_order);
    }

    #[test]
    fn test_sort_order() {
        let data = r#"
            {
                "order-id" : 1,
                "fields": [
                    {
                        "transform": "bucket[4]",   
                        "source-id": 3,   
                        "direction": "desc",   
                        "null-order": "nulls-last"
                    }]
            } 
        "#;

        let field: SortOrder = serde_json::from_str(&data).unwrap();
        assert_eq!(1, field.order_id);
        assert_eq!(1, field.fields.len());
    }
}
