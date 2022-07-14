use neon::{prelude::*, result::Throw};
use serde::Serialize;
use std::error::Error;

#[macro_export]
macro_rules! throws {
    ($cx:ident, $f:expr) => {
        match $f {
            Ok(v) => Ok(v),
            Err(e) => {
                let err: Error = e.into();
                $cx.throw_error(err.0)
            }
        }
    };
}

#[macro_export]
macro_rules! arg {
    ($cx: ident, $ty: path, $i: expr) => {{
        let arg = $cx.argument::<JsValue>($i)?;
        crate::de::from_value::<_, $ty>(&mut $cx, arg).expect("failed to parse argument")
    }};
}

#[macro_export]
macro_rules! args {
    ($cx: ident, $id0: ident : $ty0: path) => {
        let $id0: $ty0 = arg!($cx, $ty0, 0);
    };
    ($cx: ident, $id0: ident : $ty0: path, $id1: ident : $ty1: path) => {
        let ($id0, $id1) = (arg!($cx, $ty0, 0), arg!($cx, $ty1, 1));
    };
    ($cx: ident, $id0: ident : $ty0: path, $id1: ident : $ty1: path, $id2: ident : $ty2: path) => {
        let ($id0, $id1, $id2) = (
            arg!($cx, $ty0, 0),
            arg!($cx, $ty1, 1),
            arg!($cx, $ty2, 2),
        );
    };
    ($cx: ident, $id0: ident : $ty0: path, $id1: ident : $ty1: path, $id2: ident : $ty2: path, $id3: ident : $ty3: path) => {
        let ($id0, $id1, $id2, $id3) = (
            arg!($cx, $ty0, 0),
            arg!($cx, $ty1, 1),
            arg!($cx, $ty2, 2),
            arg!($cx, $ty3, 3),
        );
    };
    ($cx: ident, $id0: ident : $ty0: path, $id1: ident : $ty1: path, $id2: ident : $ty2: path, $id3: ident : $ty3: path, $id4: ident : $ty4: path) => {
        let ($id0, $id1, $id2, $id3, $id4) = (
            arg!($cx, $ty0, 0),
            arg!($cx, $ty1, 1),
            arg!($cx, $ty2, 2),
            arg!($cx, $ty3, 3),
            arg!($cx, $ty4, 4),
        );
    };
    ($cx: ident, $id0: ident : $ty0: path, $id1: ident : $ty1: path, $id2: ident : $ty2: path, $id3: ident : $ty3: path, $id4: ident : $ty4: path, $id5: ident : $ty5: path) => {
        let ($id0, $id1, $id2, $id3, $id4, $id5) = (
            arg!($cx, $ty0, 0),
            arg!($cx, $ty1, 1),
            arg!($cx, $ty2, 2),
            arg!($cx, $ty3, 3),
            arg!($cx, $ty4, 4),
            arg!($cx, $ty5, 5),
        );
    };
    ($cx: ident, $id0: ident : $ty0: path, $id1: ident : $ty1: path, $id2: ident : $ty2: path, $id3: ident : $ty3: path, $id4: ident : $ty4: path, $id5: ident : $ty5: path, $id6: ident : $ty6: path) => {
        let ($id0, $id1, $id2, $id3, $id4, $id5, $id6) = (
            arg!($cx, $ty0, 0),
            arg!($cx, $ty1, 1),
            arg!($cx, $ty2, 2),
            arg!($cx, $ty3, 3),
            arg!($cx, $ty4, 4),
            arg!($cx, $ty5, 5),
            arg!($cx, $ty6, 6),
        );
    };
    ($cx: ident, $id0: ident : $ty0: path, $id1: ident : $ty1: path, $id2: ident : $ty2: path, $id3: ident : $ty3: path, $id4: ident : $ty4: path, $id5: ident : $ty5: path, $id6: ident : $ty6: path, $id7: ident : $ty7: path) => {
        let ($id0, $id1, $id2, $id3, $id4, $id5, $id6, $id7) = (
            arg!($cx, $ty0, 0),
            arg!($cx, $ty1, 1),
            arg!($cx, $ty2, 2),
            arg!($cx, $ty3, 3),
            arg!($cx, $ty4, 4),
            arg!($cx, $ty5, 5),
            arg!($cx, $ty6, 6),
            arg!($cx, $ty7, 7),
        );
    };
}

pub fn convert_err<E: Error>(cx: &mut FunctionContext, e: E) -> Throw {
    let mut trace = vec![e.to_string()];
    let mut source = e.source();
    while let Some(s) = source {
        trace.push(s.to_string());
        source = s.source();
    }
    let err = crate::ser::to_value(cx, &trace).expect("failed to convert error");
    cx.throw::<_, ()>(err).expect_err("should never happen")
}

// pub fn convert_res<'a, T: Serialize, E: Error>(
//     cx: &mut FunctionContext<'a>,
//     res: Result<T, E>,
// ) -> Result<Handle<'a, JsValue>, Throw> {
//     res.map_err(|e| convert_err(cx, e))
//         .and_then(|t| convert(cx, &t))
// }

pub fn convert<'a, T: Serialize>(
    cx: &mut FunctionContext<'a>,
    t: &T,
) -> Result<Handle<'a, JsValue>, Throw> {
    crate::ser::to_value(cx, &t).map_err(|e| convert_err(cx, e))
}