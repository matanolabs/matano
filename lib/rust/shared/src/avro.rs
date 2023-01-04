use apache_avro::types::Value;

pub trait AvroValueExt<'a> {
    fn as_str(&self) -> Option<&str>;
    fn get(&'a self, field: &'a str) -> Option<&'a Value>;
    fn get_nested(&'a self, path: &'a str) -> Option<&'a Value>;
}

impl<'a> AvroValueExt<'a> for Value {
    fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            Value::Union(pos, v) => {
                if *pos == 0 {
                    None
                } else {
                    v.as_str()
                }
            }
            _ => None,
        }
    }

    fn get(&'a self, field: &'a str) -> Option<&'a Value> {
        match self {
            Value::Record(ref vals) => vals.iter().find(|(k, _)| k == &field).map(|(_, v)| v),
            Value::Union(pos, v) => {
                if *pos == 0 {
                    None
                } else {
                    v.get(field)
                }
            }
            _ => None,
        }
    }

    fn get_nested(&'a self, path: &'a str) -> Option<&'a Value> {
        let parts = path.split(".");
        let mut curval = self;
        for part in parts {
            if let Some(newval) = curval.get(part) {
                curval = newval;
            } else {
                return None;
            }
        }
        Some(curval)
    }
}
