use dyn_clone::DynClone;
use std::{any::Any, fmt::Debug};

#[typetag::serde]
pub trait SqlType: DynClone + Debug {
    fn name(self) -> String;
    fn value(self: &mut Self) -> Box<dyn Any>;
}
dyn_clone::clone_trait_object!(SqlType);

#[typetag::serde]
impl SqlType for String {
    fn name(self) -> String { "STRING".into() }
    fn value(self: &mut Self) -> Box<dyn Any> { Box::new(self.clone()) }
}

#[typetag::serde]
impl SqlType for i64 {
    fn name(self) -> String { "I64".into() }
    fn value(self: &mut Self) -> Box<dyn Any> { Box::new(self.clone()) }
}

#[typetag::serde]
impl SqlType for f64 {
    fn name(self) -> String { "F64".into() }
    fn value(self: &mut Self) -> Box<dyn Any> { Box::new(self.clone()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_rt::test]
    async fn can_downcast_single_type() {
        let row: Vec<String> = ["a", "b", "c"].iter().map(|s| String::from(*s)).collect();
        let any_row: Vec<Box<dyn Any>> = row.iter().map(|s| {
            let b: Box<dyn Any> = Box::new(s.clone());
            b
        }).collect();

        row.iter().zip(any_row.iter()).for_each(|(a, b)| {
            assert!(b.is::<String>());
            let b = b.downcast_ref::<String>().unwrap();
            assert_eq!(a, b);
        });
    }

    #[actix_rt::test]
    async fn can_downcast_nonnull_type() {
        // .iter().map(|s| String::from(*s)).collect();
        let mut any_row: Vec<Box<dyn SqlType>> = (0..20).map(|i| {
            let b: Box<dyn SqlType> = match i % 3 {
                0 => Box::new(i.to_string()),
                1 => Box::new(i as i64),
                2 => Box::new(i as f64),
                _ => panic!(),
            };
            b
        }).collect();

        for col in any_row.iter_mut().enumerate() {
            let v = col.1.value();
            match col.0 % 3 {
                0 => {
                    assert!(v.is::<String>());
                    v.downcast_ref::<String>().unwrap();
                },
                1 => {
                    assert!(v.is::<i64>());
                    v.downcast_ref::<i64>().unwrap();
                },
                2 => {
                    assert!(v.is::<f64>());
                    v.downcast_ref::<f64>().unwrap();
                },
                _ => panic!(),
            };
        }
    }
}