use super::data_type::DataType;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub is_nullable: bool,
}

impl ColumnDef {
    #[must_use]
    pub const fn new(name: String, data_type: DataType, is_nullable: bool) -> Self {
        Self { name, data_type, is_nullable }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
}

impl Schema {
    #[must_use]
    pub const fn new(columns: Vec<ColumnDef>) -> Self {
        Self { columns }
    }

    #[must_use]
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|col| col.name == name)
    }
}
