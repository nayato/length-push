use tokio_io::codec::{Decoder, Encoder};
use bytes::{BytesMut, BufMut, BigEndian, ByteOrder};
use super::error::*;

pub struct LengthCodec {
    state: DecodeState,
}

#[derive(Debug, Clone, Copy)]
enum DecodeState {
    Head,
    Data(usize),
}

impl LengthCodec {
    pub fn new() -> LengthCodec {
        LengthCodec { state: DecodeState::Head }
    }
}

const HEAD_LEN: usize = 4; 
impl Decoder for LengthCodec {
    type Item = BytesMut;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let n = match self.state {
            DecodeState::Head => {
                if src.len() < HEAD_LEN {
                    return Ok(None);
                }

                let n = BigEndian::read_u32(src.as_ref());

                // if n > self.builder.max_frame_len as u64 { // todo: max length check
                //     return Err(ErrorKind::FrameTooBig(src.len()).into());
                // }

                let n = n as usize; // The check above ensures there is no overflow
                let _ = src.split_to(HEAD_LEN);
                src.reserve(n); // Ensure that the buffer has enough space to read the incoming payload
                self.state = DecodeState::Data(n);
                n
            }
            DecodeState::Data(n) => n,
        };

        if src.len() < n {
            return Ok(None);
        }

        self.state = DecodeState::Head;
        src.reserve(HEAD_LEN);
        Ok(Some(src.split_to(n)))
    }
}

impl Encoder for LengthCodec {
    type Item = BytesMut;
    type Error = Error;

    fn encode(&mut self, msg: Self::Item, dst: &mut BytesMut) -> Result<()> {
        let len = msg.len();
        dst.reserve(4 + len);
        dst.put_u32::<BigEndian>(len as u32);
        if len > 0 {
            dst.put_slice(msg.as_ref());
        }
        Ok(())
    }
}