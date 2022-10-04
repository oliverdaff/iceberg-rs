/*!
 * Decimal type
 */
use anyhow::{ensure, Result};

/// The decimal type
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Decimal {
    value: num_bigint::BigInt,
    /// The number of digits in the number. Must be 38 or less
    /// This must be calculated
    precision: u8,
    /// The number of digits to the right of the decimal point.
    /// This is part of the big_num
    scale: u32,
}

impl Decimal {
    /// Create a new Decimal object.
    pub fn new(value: num_bigint::BigInt, precision: u8, scale: u32) -> Result<Self> {
        //check that the provided value has the correct precision.
        ensure!(precision <= 38, "Precision {precision} must be 38 or less");
        ensure!(
            scale <= precision as u32,
            "Scale {scale} is greater the Precision {precision}"
        );
        let bytes: usize = f64::ceil(value.bits() as f64 / 8_f64) as usize;
        ensure!(
            max_prec_for_len(bytes)? >= precision as usize,
            "The number of bytes {bytes} can not hold this precision {precision}"
        );
        //check that the provided value has the correct scale.
        Ok(Decimal {
            value,
            precision,
            scale,
        })
    }
}

fn max_prec_for_len(len: usize) -> Result<usize> {
    let len = i32::try_from(len)?;
    Ok((2.0_f64.powi(8 * len - 1) - 1.0).log10().floor() as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_decimal() {
        let dec = Decimal {
            value: num_bigint::BigInt::new(num_bigint::Sign::Plus, vec![123]),
            precision: 1,
            scale: 2,
        };

        assert_eq!(
            dec.value.to_u32_digits(),
            (num_bigint::Sign::Plus, vec![123])
        )
    }

    #[test]
    fn test_scale_gt_precision() {
        let value = num_bigint::BigInt::new(num_bigint::Sign::Plus, vec![123]);
        let dec = Decimal::new(value, 2, 3);
        assert!(dec.is_err());
    }

    #[test]
    fn test_scale_error_precision_gt_38() {
        let value = num_bigint::BigInt::new(num_bigint::Sign::Plus, vec![123]);
        let dec = Decimal::new(value, 39, 3);
        assert!(dec.is_err());
    }

    #[test]
    fn test_max_prec() {
        //16 bytes is required, this is
        assert_eq!(38, max_prec_for_len(16).unwrap());
    }

    #[test]
    fn test_precision_less_than_bytes() {
        let value = num_bigint::BigInt::new(num_bigint::Sign::Plus, vec![123]);
        let dec = Decimal::new(value, 1, 3);
        assert!(dec.is_err());
    }

    //#[test]
    //fn test_decimal_value() {
    //    let bi = num_bigint::BigInt::new(num_bigint::Sign::Plus, vec![12]);
    //    let value = Value::Decimal(AvDecimal::from(bi.to_signed_bytes_be()));
    //    let x = value
    //        .clone()
    //        .resolve(&Schema::Decimal {
    //            precision: 3,
    //            scale: 3,
    //            inner: Box::new(Schema::Bytes),
    //        })
    //        .unwrap();
    //    if let Value::Decimal(dec) = x {
    //        use std::convert::TryInto;
    //        let bytes : Vec<u8>= (&dec).try_into().unwrap();
    //        let num = num_bigint::BigInt::from_signed_bytes_be(&bytes);
    //        println!("Output num: {}", num);
    //    } else {
    //        println!("Not a decimal");
    //    }
    //}
}
