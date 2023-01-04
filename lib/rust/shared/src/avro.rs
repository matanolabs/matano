use anyhow::{anyhow, Context, Error, Ok, Result};
use apache_avro::types::Value;

pub trait AvroValueExt<'a> {
    fn as_str(&self) -> Option<&str>;
    fn as_int(&self) -> Option<i32>;
    fn as_bool(&self) -> Option<bool>;
    fn as_ts(&self) -> Option<i64>;

    fn as_mut(&'a self) -> &'a mut Self;

    fn get(&'a self, field: &'a str) -> Option<&'a Value>;
    fn get_nested(&'a self, path: &'a str) -> Option<&'a Value>;

    fn insert_record(&mut self, insert_field_name: &str, insert_value: Value) -> Result<()>;
    fn insert_record_nested(&mut self, path: &str, insert_value: Value) -> Result<()>;

    fn into_union(self) -> Value;
}

impl<'a> AvroValueExt<'a> for Value {
    // TODO: remove this, get avro change merged.
    fn as_mut(&'a self) -> &'a mut Self {
        unsafe { make_mut(self) }
    }

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

    fn as_int(&self) -> Option<i32> {
        match self {
            Value::Int(i) => Some(*i),
            Value::Union(pos, v) => {
                if *pos == 0 {
                    None
                } else {
                    v.as_int()
                }
            }
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            Value::Union(pos, v) => {
                if *pos == 0 {
                    None
                } else {
                    v.as_bool()
                }
            }
            _ => None,
        }
    }

    fn as_ts(&self) -> Option<i64> {
        match self {
            Value::TimestampMicros(b) => Some(*b),
            Value::Union(pos, v) => {
                if *pos == 0 {
                    None
                } else {
                    v.as_ts()
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

    fn into_union(self) -> Value {
        match self {
            Value::Union(_, _) => self,
            Value::Null => Value::Union(0, Box::new(self)),
            _ => Value::Union(1, Box::new(self)),
        }
    }

    fn insert_record(&mut self, insert_field_name: &str, insert_value: Value) -> Result<()> {
        match self {
            apache_avro::types::Value::Record(ref mut vals) => {
                let index = &vals.iter().position(|(k, _)| k == insert_field_name);
                if let Some(idx) = index {
                    let val_ref = vals.get_mut(*idx).unwrap();
                    *val_ref = (insert_field_name.to_string(), insert_value);
                    Ok(())
                } else {
                    Err(anyhow!("Field not found!"))
                }
            }
            apache_avro::types::Value::Union(pos, v) => {
                if *pos == 1 {
                    v.insert_record(insert_field_name, insert_value)
                } else {
                    Err(anyhow!("Null value!"))
                }
            }
            _ => Err(anyhow!("Not a record!")),
        }
    }

    fn insert_record_nested(&mut self, path: &str, insert_value: Value) -> Result<()> {
        let parts = path.split(".").collect::<Vec<_>>();
        let (tail, parts) = parts.split_last().context("Invalid path")?;

        let record_nested_path = parts.join(".");
        let record = self.get_nested(&record_nested_path).unwrap();
        let record_mut = unsafe { make_mut(record) };

        record_mut.insert_record(tail, insert_value)
    }
}

unsafe fn make_mut<T>(reference: &T) -> &mut T {
    &mut *((reference as *const T) as *mut T)
}
