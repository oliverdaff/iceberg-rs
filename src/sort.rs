use serde::{Deserialize, Serialize};
use crate::partition::Transform;


#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
enum  SortDirecion {
    #[serde(rename="asc")]
    Ascending,
    #[serde(rename="desc")]
    Descending
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
enum  NullOrder {
    #[serde(rename="nulls-first")]
    First,
    #[serde(rename="nulls-last")]
    Last,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all="kebab-case")]
struct SortField {
    /// A source column id from the table’s schema
    source_id: i32,
    /// A transform that is used to produce values to be sorted on from the source column. 
    transform: Transform,
    /// A sort direction, that can only be either asc or desc
    direction: SortDirecion,
    /// A null order that describes the order of null values when sorted.
    null_order: NullOrder
}

#[cfg(test)]
mod tests {
    
}