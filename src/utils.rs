use libc::{c_char, size_t, c_int};

extern "C" {
    pub fn gethostname(name: *mut c_char, size: size_t) -> c_int;
}

pub fn rust_gethostname() -> Result<String, ()> {
    let len = 255;
    let mut buf = Vec::<u8>::with_capacity(len);

    let ptr = buf.as_mut_slice().as_mut_ptr();

    let err = unsafe { gethostname(ptr as *mut c_char, len as size_t) } as isize;

    match err {
        0 => {
            let mut real_len = len;
            let mut i = 0;
            loop {
                let byte = unsafe { *(((ptr as u64) + (i as u64)) as *const u8) };
                if byte == 0 {
                    real_len = i;
                    break;
                }

                i += 1;
            }
            unsafe { buf.set_len(real_len) }
            Ok(String::from_utf8_lossy(buf.as_slice()).into_owned())
        }
        _ => Err(()),
    }
}