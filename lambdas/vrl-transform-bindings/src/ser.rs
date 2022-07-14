//!
//! Serialize a Rust data structure into a `JsValue`
//!

use crate::error::{Error, Result as LibResult};
use neon::{prelude::*, types::buffer::TypedArray};
use serde::ser::{self, Serialize};
use std::marker::PhantomData;

pub fn to_value<'js, C, V>(cx: &mut C, value: &V) -> LibResult<Handle<'js, JsValue>>
where
    C: Context<'js>,
    V: Serialize + ?Sized,
{
    let serializer = Serializer {
        cx,
        ph: PhantomData,
    };
    let serialized_value = value.serialize(serializer)?;
    Ok(serialized_value)
}

pub struct Serializer<'a, 'j, C: 'a>
where
    C: Context<'j>,
{
    cx: &'a mut C,
    ph: PhantomData<&'j ()>,
}

pub struct ArraySerializer<'a, 'j, C: 'a>
where
    C: Context<'j>,
{
    cx: &'a mut C,
    array: Handle<'j, JsArray>,
}

pub struct TupleVariantSerializer<'a, 'j, C: 'a>
where
    C: Context<'j>,
{
    outter_object: Handle<'j, JsObject>,
    inner: ArraySerializer<'a, 'j, C>,
}

pub struct MapSerializer<'a, 'j, C: 'a>
where
    C: Context<'j>,
{
    cx: &'a mut C,
    object: Handle<'j, JsObject>,
    key_holder: Handle<'j, JsObject>,
}

pub struct StructSerializer<'a, 'j, C: 'a>
where
    C: Context<'j>,
{
    cx: &'a mut C,
    object: Handle<'j, JsObject>,
}

pub struct StructVariantSerializer<'a, 'j, C: 'a>
where
    C: Context<'j>,
{
    outer_object: Handle<'j, JsObject>,
    inner: StructSerializer<'a, 'j, C>,
}

impl<'a, 'j, C> ser::Serializer for Serializer<'a, 'j, C>
where
    C: Context<'j>,
{
    type Ok = Handle<'j, JsValue>;
    type Error = Error;

    type SerializeSeq = ArraySerializer<'a, 'j, C>;
    type SerializeTuple = ArraySerializer<'a, 'j, C>;
    type SerializeTupleStruct = ArraySerializer<'a, 'j, C>;
    type SerializeTupleVariant = TupleVariantSerializer<'a, 'j, C>;
    type SerializeMap = MapSerializer<'a, 'j, C>;
    type SerializeStruct = StructSerializer<'a, 'j, C>;
    type SerializeStructVariant = StructVariantSerializer<'a, 'j, C>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(JsBoolean::new(self.cx, v).upcast())
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(v as f64)
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(v as f64)
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(v as f64)
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(v as f64)
    }

    fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(v as f64)
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(v as f64)
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(v as f64)
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(v as f64)
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(v as f64)
    }

    fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(v as f64)
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(v as f64)
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(JsNumber::new(self.cx, v).upcast())
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        let mut b = [0; 4];
        let result = v.encode_utf8(&mut b);
        let js_str =
            JsString::try_new(self.cx, result).map_err(|_| Error::StringTooLongForChar(4))?;
        Ok(js_str.upcast())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        let len = v.len();
        let js_str = JsString::try_new(self.cx, v).map_err(|_| Error::StringTooLong(len))?;
        Ok(js_str.upcast())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        let mut buff = JsBuffer::new(self.cx, v.len())?;
        buff.as_mut_slice(self.cx).clone_from_slice(v);
        Ok(buff.upcast())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.cx.null().upcast())
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.cx.null().upcast())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(self.cx.null().upcast())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        let obj = JsObject::new(&mut *self.cx);
        let value_js = to_value(self.cx, value)?;
        obj.set(self.cx, variant, value_js)?;

        Ok(obj.upcast())
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(ArraySerializer::new(self.cx))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(ArraySerializer::new(self.cx))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(ArraySerializer::new(self.cx))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        TupleVariantSerializer::new(self.cx, variant)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(MapSerializer::new(self.cx))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(StructSerializer::new(self.cx))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        StructVariantSerializer::new(self.cx, variant)
    }
}

impl<'a, 'j, C> ArraySerializer<'a, 'j, C>
where
    C: Context<'j>,
{
    fn new(cx: &'a mut C) -> Self {
        let array = JsArray::new(cx, 0);
        ArraySerializer { cx, array }
    }
}

impl<'a, 'j, C> ser::SerializeSeq for ArraySerializer<'a, 'j, C>
where
    C: Context<'j>,
{
    type Ok = Handle<'j, JsValue>;
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let value = to_value(self.cx, value)?;

        let arr: Handle<'j, JsArray> = self.array;
        let len = arr.len(self.cx);
        arr.set(self.cx, len, value)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.array.upcast())
    }
}

impl<'a, 'j, C> ser::SerializeTuple for ArraySerializer<'a, 'j, C>
where
    C: Context<'j>,
{
    type Ok = Handle<'j, JsValue>;
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeSeq::end(self)
    }
}

impl<'a, 'j, C> ser::SerializeTupleStruct for ArraySerializer<'a, 'j, C>
where
    C: Context<'j>,
{
    type Ok = Handle<'j, JsValue>;
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        ser::SerializeSeq::end(self)
    }
}

impl<'a, 'j, C> TupleVariantSerializer<'a, 'j, C>
where
    C: Context<'j>,
{
    fn new(cx: &'a mut C, key: &'static str) -> LibResult<Self> {
        let inner_array = JsArray::new(cx, 0);
        let outter_object = JsObject::new(cx);
        outter_object.set(cx, key, inner_array)?;
        Ok(TupleVariantSerializer {
            outter_object,
            inner: ArraySerializer {
                cx,
                array: inner_array,
            },
        })
    }
}

impl<'a, 'j, C> ser::SerializeTupleVariant for TupleVariantSerializer<'a, 'j, C>
where
    C: Context<'j>,
{
    type Ok = Handle<'j, JsValue>;
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        use serde::ser::SerializeSeq;
        self.inner.serialize_element(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.outter_object.upcast())
    }
}

impl<'a, 'j, C> MapSerializer<'a, 'j, C>
where
    C: Context<'j>,
{
    fn new(cx: &'a mut C) -> Self {
        let object = JsObject::new(cx);
        let key_holder = JsObject::new(cx);
        MapSerializer {
            cx,
            object,
            key_holder,
        }
    }
}

impl<'a, 'j, C> ser::SerializeMap for MapSerializer<'a, 'j, C>
where
    C: Context<'j>,
{
    type Ok = Handle<'j, JsValue>;
    type Error = Error;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let key = to_value(self.cx, key)?;
        self.key_holder.set(self.cx, "key", key)?;
        Ok(())
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let key: Handle<'j, JsValue> = self.key_holder.get(&mut *self.cx, "key")?;
        let value_obj = to_value(self.cx, value)?;
        self.object.set(self.cx, key, value_obj)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.object.upcast())
    }
}

impl<'a, 'j, C> StructSerializer<'a, 'j, C>
where
    C: Context<'j>,
{
    fn new(cx: &'a mut C) -> Self {
        let object = JsObject::new(cx);
        StructSerializer { cx, object }
    }
}

impl<'a, 'j, C> ser::SerializeStruct for StructSerializer<'a, 'j, C>
where
    C: Context<'j>,
{
    type Ok = Handle<'j, JsValue>;
    type Error = Error;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let value = to_value(self.cx, value)?;
        self.object.set(self.cx, key, value)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.object.upcast())
    }
}

impl<'a, 'j, C> StructVariantSerializer<'a, 'j, C>
where
    C: Context<'j>,
{
    fn new(cx: &'a mut C, key: &'static str) -> LibResult<Self> {
        let inner_object = JsObject::new(cx);
        let outter_object = JsObject::new(cx);
        outter_object.set(cx, key, inner_object)?;
        Ok(StructVariantSerializer {
            outer_object: outter_object,
            inner: StructSerializer {
                cx,
                object: inner_object,
            },
        })
    }
}

impl<'a, 'j, C> ser::SerializeStructVariant for StructVariantSerializer<'a, 'j, C>
where
    C: Context<'j>,
{
    type Ok = Handle<'j, JsValue>;
    type Error = Error;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        use serde::ser::SerializeStruct;
        self.inner.serialize_field(key, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.outer_object.upcast())
    }
}